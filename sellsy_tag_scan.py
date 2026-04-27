"""
sellsy_tag_scan.py — Scan ciblé des prospects Sellsy pour identifier
les "fauteuils vides" (>= seuil de créneaux Doctolib sur N jours).

Deux modes d'entrée :
  - tags=NAMES      : essaie 4 stratégies pour résoudre le filtre smart-tags
  - sellsy_ids=IDS  : prend directement une liste d'IDs (POST body ou query)
                      → bypass total du filtre Sellsy (mode debug/fallback)

Usage CLI :
    python sellsy_tag_scan.py --tags "new cab avril 26,new centre avril 26" --min-slots 5 --days 7
    python sellsy_tag_scan.py --ids 46905613,46905614 --min-slots 5

Usage webhook :
    POST /trigger/sellsy-tag-scan?tags=new+cab+avril+26&min_slots=5&days=7
    POST /trigger/sellsy-tag-scan?sellsy_ids=46905613,46905614&min_slots=5
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
    def __init__(self):
        self.token = None
        self.token_expires = 0
        self.session = requests.Session()
        self.debug_log = []  # accumule les essais pour debug

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

    def get_company(self, company_id):
        """GET /companies/{id} — récupère une fiche complète."""
        self._ensure_token()
        resp = self.session.get(
            f"{SELLSY_API_URL}/companies/{company_id}",
            headers=self.headers,
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        body = resp.json()
        return body.get("data") or body

    def list_companies_paginated(self, page_size=100):
        """GET /companies?order=created_at&direction=desc paginé. Yield les companies."""
        self._ensure_token()
        offset = 0
        while True:
            resp = self.session.get(
                f"{SELLSY_API_URL}/companies",
                params={
                    "limit": page_size,
                    "offset": offset,
                    "order": "created_at",
                    "direction": "desc",
                },
                headers=self.headers,
                timeout=60,
            )
            if resp.status_code != 200:
                self.debug_log.append(f"GET /companies offset={offset} -> {resp.status_code}: {resp.text[:200]}")
                break
            body = resp.json()
            data = body.get("data", []) or []
            if not data:
                break
            for c in data:
                yield c
            if len(data) < page_size:
                break
            offset += page_size
            time.sleep(0.2)

    def search_filter_attempts(self, tag_names):
        """Essaie plusieurs syntaxes de filtre smart-tags via POST /companies/search."""
        self._ensure_token()
        attempts = [
            ("smart_tags_names", {"smart_tags": tag_names}),
            ("smart_tags_objects", {"smart_tags": [{"value": n} for n in tag_names]}),
            ("smartTags", {"smartTags": tag_names}),
            ("tags", {"tags": tag_names}),
        ]
        for label, filter_body in attempts:
            try:
                resp = self.session.post(
                    f"{SELLSY_API_URL}/companies/search",
                    json={"filters": filter_body, "limit": 5},
                    headers=self.headers,
                    timeout=30,
                )
                if resp.status_code == 200:
                    body = resp.json()
                    pagination = body.get("pagination", {})
                    total = pagination.get("count") or pagination.get("total") or len(body.get("data", []))
                    self.debug_log.append(f"  filter[{label}] HTTP200 total={total}")
                    if total and total > 0 and total < 50000:  # vraisemblable
                        return label, filter_body
                else:
                    self.debug_log.append(f"  filter[{label}] HTTP{resp.status_code}: {resp.text[:120]}")
            except Exception as e:
                self.debug_log.append(f"  filter[{label}] exception: {e}")
        return None, None

    def search_companies_with_filter(self, filter_body, page_size=100):
        """Pagine /companies/search avec un filter qui marche."""
        self._ensure_token()
        out = []
        offset = 0
        while True:
            resp = self.session.post(
                f"{SELLSY_API_URL}/companies/search",
                json={"filters": filter_body, "limit": page_size, "offset": offset},
                headers=self.headers,
                timeout=60,
            )
            if resp.status_code != 200:
                self.debug_log.append(f"page offset={offset} HTTP{resp.status_code}: {resp.text[:200]}")
                break
            data = resp.json().get("data", []) or []
            if not data:
                break
            out.extend(data)
            log.info(f"  page offset={offset} -> +{len(data)} (cumul {len(out)})")
            if len(data) < page_size:
                break
            offset += page_size
            time.sleep(0.2)
        return out


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


def write_results_to_gsheet(rows, tag_names_or_ids):
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
                ",".join(str(x) for x in tag_names_or_ids),
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


def _scrape_companies(companies, sellsy, min_slots, days):
    sess = _doctolib_session()
    results = []
    stats = {"ok": 0, "no_url": 0, "not_on_doctolib": 0, "no_motive": 0, "http_error": 0, "exception": 0}

    for i, c in enumerate(companies, 1):
        url = _company_doctolib_url(c)
        if not url:
            full = sellsy.get_company(int(c["id"])) if c.get("id") else None
            url = _company_doctolib_url(full or {})
            if full:
                # enrichir c avec les détails de full pour ville/cp/tel
                for k in ("name", "phone_number", "address", "billing_address"):
                    if k not in c and k in full:
                        c[k] = full[k]

        nb_slots, status = count_doctolib_slots(sess, url, days=days) if url else (0, "no_url")
        prefix = status.split(":", 1)[0]
        stats[prefix] = stats.get(prefix, 0) + 1

        results.append({
            "sellsy_id": int(c["id"]) if c.get("id") else None,
            "nom": c.get("name") or c.get("display_name") or "",
            "tag": c.get("_tag_label", ""),
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

    return results, stats


def run_with_ids(sellsy_ids, min_slots=5, days=7, label="ids"):
    """Mode IDs : prend directement une liste d'IDs Sellsy, GET chaque fiche, scrape."""
    started_at = datetime.now(timezone.utc).isoformat()
    log.info(f"=== sellsy_tag_scan IDS mode : {len(sellsy_ids)} ids min_slots={min_slots} days={days} ===")

    sellsy = SellsySync()
    sellsy._ensure_token()
    companies = []
    for sid in sellsy_ids:
        c = sellsy.get_company(int(sid))
        if c:
            c["_tag_label"] = label
            companies.append(c)
        else:
            log.warning(f"  Sellsy id {sid} introuvable")
    log.info(f"  {len(companies)} fiches récupérées sur {len(sellsy_ids)} demandées")

    if not companies:
        return {"error": "no_companies_resolved", "ids_count": len(sellsy_ids), "started_at": started_at}

    results, stats = _scrape_companies(companies, sellsy, min_slots, days)
    qualified = sorted(
        [r for r in results if r["nb_creneaux"] >= min_slots],
        key=lambda r: r["nb_creneaux"], reverse=True,
    )
    ws_name = write_results_to_gsheet(qualified, [label])
    return {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "mode": "ids",
        "ids_count": len(sellsy_ids),
        "scraped": len(results),
        "qualified_over_threshold": len(qualified),
        "min_slots": min_slots, "days": days,
        "stats": stats, "gsheet_tab": ws_name,
        "top10": [
            {"sellsy_id": r["sellsy_id"], "nom": r["nom"], "ville": r["ville"], "nb_creneaux": r["nb_creneaux"]}
            for r in qualified[:10]
        ],
    }


def run(tag_names, min_slots=5, days=7):
    started_at = datetime.now(timezone.utc).isoformat()
    log.info(f"=== sellsy_tag_scan TAGS mode : tags={tag_names} min_slots={min_slots} days={days} ===")

    sellsy = SellsySync()
    log.info("--- Probe : tester les syntaxes de filtre smart-tags ---")
    label, filter_body = sellsy.search_filter_attempts(tag_names)
    if not filter_body:
        return {
            "error": "no_working_filter_found",
            "tag_names": tag_names,
            "debug_log": sellsy.debug_log,
            "hint": "Aucune syntaxe de filtre smart-tags ne marche. Utilise le mode `sellsy_ids=...` à la place. Récupère les IDs depuis l'export CSV Sellsy filtré par tag.",
            "started_at": started_at,
        }
    log.info(f"  Filter qui marche : `{label}` -> {filter_body}")
    companies = sellsy.search_companies_with_filter(filter_body)
    log.info(f"  -> {len(companies)} companies")

    if not companies:
        return {
            "error": "filter_works_but_no_results",
            "tag_names": tag_names, "filter_used": filter_body,
            "debug_log": sellsy.debug_log,
            "started_at": started_at,
        }

    for c in companies:
        c["_tag_label"] = ",".join(tag_names)

    results, stats = _scrape_companies(companies, sellsy, min_slots, days)
    qualified = sorted(
        [r for r in results if r["nb_creneaux"] >= min_slots],
        key=lambda r: r["nb_creneaux"], reverse=True,
    )
    ws_name = write_results_to_gsheet(qualified, tag_names)

    return {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "mode": "tags",
        "tag_names": tag_names,
        "filter_used": filter_body,
        "total_companies": len(companies),
        "scraped": len(results),
        "qualified_over_threshold": len(qualified),
        "min_slots": min_slots, "days": days,
        "stats": stats, "gsheet_tab": ws_name,
        "top10": [
            {"sellsy_id": r["sellsy_id"], "nom": r["nom"], "ville": r["ville"], "nb_creneaux": r["nb_creneaux"]}
            for r in qualified[:10]
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", help="Smart-tag names, comma-separated")
    parser.add_argument("--ids", help="Sellsy company IDs, comma-separated")
    parser.add_argument("--min-slots", type=int, default=5)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    if args.ids:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
        summary = run_with_ids(ids, min_slots=args.min_slots, days=args.days)
    elif args.tags:
        tag_names = [t.strip() for t in args.tags.split(",") if t.strip()]
        summary = run(tag_names=tag_names, min_slots=args.min_slots, days=args.days)
    else:
        parser.error("--tags ou --ids requis")
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
