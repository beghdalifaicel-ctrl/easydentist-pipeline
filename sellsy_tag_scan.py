"""
sellsy_tag_scan.py — Scan ciblé des prospects Sellsy taggés pour identifier
les "fauteuils vides" (>= seuil de créneaux Doctolib sur N jours).

Workflow :
  1. Auth Sellsy v2 (OAuth client_credentials)
  2. Pull les companies portant n'importe lequel des tags fournis (pagination)
  3. Pour chaque company avec un Site Internet Doctolib, hit /availabilities.json
     via proxy Bright Data datacenter pour compter les créneaux sur N jours
  4. Filtre : ne garder que celles avec count >= min_slots
  5. Écrit un onglet GSheet `Fauteuils_Vides_<YYYY-MM-DD>` avec le résultat trié
  6. Retourne un summary dict (à sérialiser pour /status)

Usage Python (CLI) :
    python sellsy_tag_scan.py \
        --tags "new cab avril 26,new centre avril 26" \
        --min-slots 5 --days 7

Usage via webhook :
    POST /trigger/sellsy-tag-scan?tags=new+cab+avril+26,new+centre+avril+26&min_slots=5&days=7
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
import argparse
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────
SELLSY_API_URL = os.getenv("SELLSY_API_URL", "https://api.sellsy.com/v2")
SELLSY_CLIENT_ID = os.getenv("SELLSY_CLIENT_ID", "")
SELLSY_CLIENT_SECRET = os.getenv("SELLSY_CLIENT_SECRET", "")

BRIGHT_DATA_HOST = os.getenv("BRIGHT_DATA_HOST", "brd.superproxy.io")
BRIGHT_DATA_PORT = os.getenv("BRIGHT_DATA_PORT", "33335")
BRIGHT_DATA_USER = os.getenv("BRIGHT_DATA_USER", "")
BRIGHT_DATA_PASS = os.getenv("BRIGHT_DATA_PASS", "")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

DELAY_MIN = float(os.getenv("DELAY_MIN", "0.5"))
DELAY_MAX = float(os.getenv("DELAY_MAX", "1.5"))


class SellsySync:
    """Mini client Sellsy sync — assez pour lister companies + smart-tags."""

    def __init__(self):
        self.token = None
        self.token_expires = 0
        self.session = requests.Session()

    def _ensure_token(self):
        if self.token and time.time() < self.token_expires - 60:
            return
        resp = self.session.post(
            "https://login.sellsy.com/oauth2/access-tokens",
            json={
                "grant_type": "client_credentials",
                "client_id": SELLSY_CLIENT_ID,
                "client_secret": SELLSY_CLIENT_SECRET,
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        self.token_expires = time.time() + data.get("expires_in", 3600)
        log.info("Sellsy token obtenu")

    @property
    def headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def list_smart_tags(self):
        self._ensure_token()
        out = []
        offset = 0
        while True:
            resp = self.session.get(
                f"{SELLSY_API_URL}/smart-tags",
                params={"limit": 100, "offset": offset},
                headers=self.headers,
                timeout=30,
            )
            if resp.status_code != 200:
                log.warning(f"list_smart_tags {resp.status_code}: {resp.text[:200]}")
                break
            data = resp.json().get("data", [])
            if not data:
                break
            out.extend(data)
            if len(data) < 100:
                break
            offset += 100
        return out

    def resolve_tag_ids(self, tag_names):
        all_tags = self.list_smart_tags()
        wanted = {n.strip().lower() for n in tag_names if n and n.strip()}
        ids = []
        seen = []
        for t in all_tags:
            name = (t.get("value") or t.get("name") or "").strip().lower()
            if name in wanted:
                ids.append(int(t["id"]))
                seen.append(name)
        missing = wanted - set(seen)
        if missing:
            log.warning(f"Tags introuvables : {missing}")
        return ids

    def search_companies_by_smart_tag_ids(self, tag_ids):
        if not tag_ids:
            return []
        self._ensure_token()
        out = []
        offset = 0
        page_size = 100
        while True:
            resp = self.session.post(
                f"{SELLSY_API_URL}/companies/search",
                json={
                    "filters": {"smart_tag_ids": tag_ids},
                    "limit": page_size,
                    "offset": offset,
                },
                headers=self.headers,
                timeout=60,
            )
            if resp.status_code != 200:
                log.error(f"search_companies smart_tag_ids {resp.status_code}: {resp.text[:300]}")
                break
            data = resp.json().get("data", [])
            if not data:
                break
            out.extend(data)
            log.info(f"  Sellsy page offset={offset} -> +{len(data)} companies (cumul {len(out)})")
            if len(data) < page_size:
                break
            offset += page_size
        return out

    def get_company_full(self, company_id):
        self._ensure_token()
        resp = self.session.get(
            f"{SELLSY_API_URL}/companies/{company_id}",
            headers=self.headers,
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        return resp.json().get("data") or resp.json()


def _doctolib_session():
    s = requests.Session()
    proxy = f"http://{BRIGHT_DATA_USER}:{BRIGHT_DATA_PASS}@{BRIGHT_DATA_HOST}:{BRIGHT_DATA_PORT}"
    s.proxies = {"http": proxy, "https": proxy}
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,text/html",
        "Accept-Language": "fr-FR,fr;q=0.9",
    })
    return s


def count_doctolib_slots(session, doctolib_url, days=7):
    if not doctolib_url or "doctolib" not in (doctolib_url or "").lower():
        return 0, "no_url"
    try:
        resp = session.get(doctolib_url, timeout=30)
        if resp.status_code == 404:
            return 0, "not_on_doctolib"
        resp.raise_for_status()
        html = resp.text

        m_practice = re.search(r'"practice_ids":\[(\d+)', html)
        m_motive = re.search(r'"visit_motive_ids":\[([^\]]+)\]', html)
        if not m_practice or not m_motive:
            has_text = re.search(r"(Prochain rendez-vous|Prochaine disponibilit)", html, re.I)
            return (1 if has_text else 0), "no_motive"

        practice_id = m_practice.group(1)
        first_motive = m_motive.group(1).split(",")[0].strip()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        url_avail = (
            f"https://www.doctolib.fr/availabilities.json"
            f"?start_date={today}"
            f"&visit_motive_ids={first_motive}"
            f"&practice_ids={practice_id}"
            f"&limit={days}"
        )
        r2 = session.get(url_avail, timeout=30)
        if r2.status_code != 200:
            return 0, f"http_error:{r2.status_code}"
        data = r2.json()
        availabilities = data.get("availabilities", []) or []
        total = sum(len(d.get("slots", []) or []) for d in availabilities)
        return total, "ok"
    except requests.RequestException:
        return 0, "exception"
    except Exception:
        return 0, "exception"


def write_results_to_gsheet(rows, tag_names):
    if not GOOGLE_SHEET_ID or not GOOGLE_REFRESH_TOKEN:
        log.warning("GSheet non configuré, skip write")
        return None
    try:
        import gspread
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request as GRequest
    except ImportError:
        log.warning("gspread non installé, skip write")
        return None

    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    creds.refresh(GRequest())
    client = gspread.authorize(creds)
    sheet = client.open_by_key(GOOGLE_SHEET_ID)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ws_name = f"Fauteuils_Vides_{today}"
    try:
        existing = sheet.worksheet(ws_name)
        sheet.del_worksheet(existing)
    except Exception:
        pass
    ws = sheet.add_worksheet(title=ws_name, rows=max(len(rows) + 5, 100), cols=12)

    headers = [
        "sellsy_id", "nom", "tag", "ville", "code_postal",
        "telephone", "doctolib_url", "nb_creneaux_7j", "scrape_status",
        "scrape_date", "tags_query", "min_slots_threshold",
    ]
    ws.update("A1", [headers])

    if rows:
        values = [
            [
                r.get("sellsy_id"),
                r.get("nom") or "",
                r.get("tag") or "",
                r.get("ville") or "",
                r.get("code_postal") or "",
                r.get("telephone") or "",
                r.get("doctolib_url") or "",
                r.get("nb_creneaux", 0),
                r.get("scrape_status") or "",
                r.get("scrape_date") or "",
                ",".join(tag_names),
                r.get("min_slots_threshold", 0),
            ]
            for r in rows
        ]
        ws.update("A2", values, value_input_option="USER_ENTERED")

    log.info(f"GSheet : onglet `{ws_name}` ecrit ({len(rows)} lignes)")
    return ws_name


def _company_doctolib_url(company):
    for k in ("website", "web", "site_web", "url"):
        v = (company or {}).get(k)
        if v and "doctolib" in str(v).lower():
            return str(v)
    for nested_key in ("contact_information", "informations"):
        nest = (company or {}).get(nested_key) or {}
        for k in ("website", "web"):
            v = nest.get(k)
            if v and "doctolib" in str(v).lower():
                return str(v)
    return ""


def _company_phone(company):
    for k in ("phone_number", "phone", "tel", "mobile"):
        v = (company or {}).get(k)
        if v:
            return str(v)
    return ""


def _company_city(company):
    for nested in ("address", "billing_address", "primary_address"):
        a = (company or {}).get(nested) or {}
        v = a.get("city") or a.get("ville")
        if v:
            return str(v)
    return ""


def _company_postal(company):
    for nested in ("address", "billing_address", "primary_address"):
        a = (company or {}).get(nested) or {}
        v = a.get("postal_code") or a.get("zipcode") or a.get("zip_code")
        if v:
            return str(v)
    return ""


def run(tag_names, min_slots=5, days=7):
    started_at = datetime.now(timezone.utc).isoformat()
    log.info(f"=== sellsy_tag_scan : tags={tag_names} min_slots={min_slots} days={days} ===")

    sellsy = SellsySync()
    tag_ids = sellsy.resolve_tag_ids(tag_names)
    if not tag_ids:
        return {"error": "no_tag_ids", "tag_names": tag_names}
    log.info(f"  Tag IDs resolus : {tag_ids}")

    companies = sellsy.search_companies_by_smart_tag_ids(tag_ids)
    log.info(f"  -> {len(companies)} companies recuperees via tags")
    if not companies:
        return {"total_companies": 0, "tag_ids": tag_ids, "started_at": started_at}

    sess = _doctolib_session()
    results = []
    stats = {"ok": 0, "no_url": 0, "not_on_doctolib": 0, "no_motive": 0, "http_error": 0, "exception": 0}

    for i, c in enumerate(companies, 1):
        url = _company_doctolib_url(c)
        if not url:
            full = sellsy.get_company_full(int(c["id"]))
            url = _company_doctolib_url(full or {})

        nb_slots, status = count_doctolib_slots(sess, url, days=days) if url else (0, "no_url")
        prefix = status.split(":", 1)[0]
        stats[prefix] = stats.get(prefix, 0) + 1

        results.append({
            "sellsy_id": int(c["id"]),
            "nom": c.get("name") or c.get("display_name") or "",
            "tag": ",".join(tag_names),
            "ville": _company_city(c),
            "code_postal": _company_postal(c),
            "telephone": _company_phone(c),
            "doctolib_url": url,
            "nb_creneaux": nb_slots,
            "scrape_status": status,
            "scrape_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "min_slots_threshold": min_slots,
        })

        if i % 50 == 0:
            qcount = sum(1 for r in results if r["nb_creneaux"] >= min_slots)
            log.info(f"  [{i}/{len(companies)}] cumul : ok={stats['ok']} >=seuil={qcount}")

        time.sleep(DELAY_MIN + (DELAY_MAX - DELAY_MIN) * 0.5)

    qualified = sorted(
        [r for r in results if r["nb_creneaux"] >= min_slots],
        key=lambda r: r["nb_creneaux"],
        reverse=True,
    )

    ws_name = write_results_to_gsheet(qualified, tag_names)

    summary = {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "tag_names": tag_names,
        "tag_ids": tag_ids,
        "total_companies": len(companies),
        "scraped": len(results),
        "qualified_over_threshold": len(qualified),
        "min_slots": min_slots,
        "days": days,
        "stats": stats,
        "gsheet_tab": ws_name,
        "top10": [
            {"sellsy_id": r["sellsy_id"], "nom": r["nom"], "ville": r["ville"], "nb_creneaux": r["nb_creneaux"]}
            for r in qualified[:10]
        ],
    }
    log.info(f"=== Termine : {len(qualified)} fauteuils vides identifies / {len(companies)} scannes ===")
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", required=True, help="Smart-tag names, comma-separated")
    parser.add_argument("--min-slots", type=int, default=5)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    tag_names = [t.strip() for t in args.tags.split(",") if t.strip()]
    summary = run(tag_names=tag_names, min_slots=args.min_slots, days=args.days)
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
