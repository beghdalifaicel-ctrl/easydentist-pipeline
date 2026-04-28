"""
sellsy_tag_scan.py — Scan ciblé des prospects Sellsy pour identifier
les "fauteuils vides" (>= seuil de créneaux Doctolib sur N jours).

V4 — Utilise Playwright + Bright Data Browser API (CDP) pour rendre les pages SPA.
Le HTML brut Doctolib n'expose plus practice_id ni visit_motive_ids depuis 2025
→ obligatoire de rendre le JS pour extraire les slots.

Modes :
  - sellsy_ids=IDS  : prend une liste d'IDs Sellsy en input
  - tags=NAMES      : tente le filtre smart-tags (probablement KO côté API v2)

Usage CLI :
    python sellsy_tag_scan.py --ids 46905613,46905614 --min-slots 5 --days 7

Usage webhook :
    POST /trigger/sellsy-tag-scan?sellsy_ids=46905613,46905614&min_slots=5&days=7
    POST /trigger/sellsy-tag-scan
      body: {"sellsy_ids":[...], "min_slots":5, "days":7, "label":"fauteuils_vides"}
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
import argparse
import asyncio
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SELLSY_API_URL = os.getenv("SELLSY_API_URL", "https://api.sellsy.com/v2")
SELLSY_CLIENT_ID = os.getenv("SELLSY_CLIENT_ID", "")
SELLSY_CLIENT_SECRET = os.getenv("SELLSY_CLIENT_SECRET", "")

SBR_WS = os.getenv(
    "BRIGHT_DATA_BROWSER_WS",
    "wss://brd-customer-hl_dbe515e1-zone-doctolib_browser:ub9zsp721noa@brd.superproxy.io:9222"
)

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

DELAY_MIN = float(os.getenv("DELAY_MIN", "0.5"))


JS_COUNT_DOCTOLIB_SLOTS = r"""
() => {
    const body = (document.body && document.body.innerText) ? document.body.innerText : "";
    const title = document.title || "";

    // 1) Cas "pas sur Doctolib"
    const notOnDoctolib = body.includes("n'est pas sur Doctolib")
        || body.includes("n’est pas sur Doctolib")
        || body.includes("Revendiquer mon profil")
        || body.includes("pas réservable en ligne")
        || body.includes("pas réservable en ligne");
    if (notOnDoctolib) return {status: "not_on_doctolib", nb_slots: 0, mode: "negative_signal"};

    // 2) Compter les boutons de créneau visibles dans le widget Doctolib
    // Plusieurs sélecteurs car Doctolib change ses classes
    const selectors = [
        'button.availabilities-slot',
        'button.dl-button-slot',
        '[data-test="availability-slot"]',
        'button[class*="slot"][class*="availability"]',
        'button[class*="time-slot"]',
        'a[href*="/booking/"]',
    ];
    let slot_count = 0;
    let used_selector = null;
    for (const sel of selectors) {
        const els = document.querySelectorAll(sel);
        if (els.length > 0) {
            // Filtrer les visibles
            let visible = 0;
            els.forEach(e => {
                const r = e.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) visible++;
            });
            if (visible > 0) {
                slot_count = visible;
                used_selector = sel;
                break;
            }
        }
    }

    if (slot_count > 0) {
        return {status: "ok", nb_slots: slot_count, mode: "dom_count", selector: used_selector};
    }

    // 3) Fallback texte : "Aucune disponibilité"
    if (body.match(/Aucune disponibilit[eé]/i)) {
        return {status: "ok", nb_slots: 0, mode: "text_no_avail"};
    }

    // 4) Fallback : "Prochain rendez-vous : <date>" → on a au moins 1 slot, mais on retourne 1
    //    (impossible de savoir le nb exact sans cliquer sur un motif)
    const m = body.match(/Prochain (?:rendez-vous|RDV)\s*:?\s*([A-Za-zéè]+\s+\d+\s+[a-zéû]+)/i);
    if (m) {
        return {status: "ok", nb_slots: 1, mode: "text_proximate", date: m[1]};
    }

    // 5) Si on a au moins un mot-clé Doctolib mais pas de slots détectés → 0
    if (body.includes("Prendre rendez-vous") || body.includes("Prenez RDV")) {
        return {status: "ok", nb_slots: 0, mode: "registered_no_slots"};
    }

    return {status: "no_data", nb_slots: 0, mode: "unknown", title_snip: title.slice(0, 60)};
}
"""


class SellsySync:
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
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def get_company(self, company_id):
        self._ensure_token()
        resp = self.session.get(
            f"{SELLSY_API_URL}/companies/{company_id}", headers=self.headers, timeout=30,
        )
        if resp.status_code != 200:
            return None
        body = resp.json()
        return body.get("data") or body


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


async def _scrape_with_browser(companies, min_slots):
    """Lance Chromium via Bright Data Browser et scrape chaque URL Doctolib."""
    from playwright.async_api import async_playwright

    results = []
    stats = {"ok": 0, "no_url": 0, "not_on_doctolib": 0, "no_data": 0, "exception": 0}

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.connect_over_cdp(SBR_WS, timeout=60_000)
        except Exception as e:
            log.error(f"Connexion Bright Data Browser impossible: {e}")
            return results, stats

        try:
            page = await browser.new_page()

            for i, c in enumerate(companies, 1):
                url = _company_doctolib_url(c)
                if not url:
                    results.append({
                        "sellsy_id": int(c["id"]) if c.get("id") else None,
                        "nom": c.get("name") or "",
                        "tag": c.get("_tag_label", ""),
                        "ville": _company_city(c),
                        "code_postal": _company_postal(c),
                        "telephone": _company_phone(c),
                        "doctolib_url": "",
                        "nb_creneaux": 0,
                        "scrape_status": "no_url",
                        "scrape_mode": "",
                        "scrape_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "min_slots_threshold": min_slots,
                    })
                    stats["no_url"] += 1
                    continue

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    # Attendre le rendu React + le widget de booking
                    await page.wait_for_timeout(5000)
                    # Scroll léger pour trigger les chargements lazy
                    try:
                        await page.evaluate("window.scrollBy(0, 600)")
                        await page.wait_for_timeout(1500)
                    except Exception:
                        pass

                    res = await page.evaluate(JS_COUNT_DOCTOLIB_SLOTS)
                    status = res.get("status") or "no_data"
                    nb = int(res.get("nb_slots") or 0)
                    mode = res.get("mode") or ""

                    stats[status if status in stats else "no_data"] = stats.get(status, 0) + 1
                    results.append({
                        "sellsy_id": int(c["id"]) if c.get("id") else None,
                        "nom": c.get("name") or "",
                        "tag": c.get("_tag_label", ""),
                        "ville": _company_city(c),
                        "code_postal": _company_postal(c),
                        "telephone": _company_phone(c),
                        "doctolib_url": url,
                        "nb_creneaux": nb,
                        "scrape_status": status,
                        "scrape_mode": mode,
                        "scrape_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "min_slots_threshold": min_slots,
                    })
                except Exception as e:
                    stats["exception"] += 1
                    results.append({
                        "sellsy_id": int(c["id"]) if c.get("id") else None,
                        "nom": c.get("name") or "",
                        "tag": c.get("_tag_label", ""),
                        "ville": _company_city(c),
                        "code_postal": _company_postal(c),
                        "telephone": _company_phone(c),
                        "doctolib_url": url,
                        "nb_creneaux": 0,
                        "scrape_status": "exception",
                        "scrape_mode": str(e)[:100],
                        "scrape_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "min_slots_threshold": min_slots,
                    })

                if i % 20 == 0:
                    qcount = sum(1 for r in results if r["nb_creneaux"] >= min_slots)
                    log.info(f"  [{i}/{len(companies)}] ok={stats['ok']} excep={stats['exception']} >=seuil={qcount}")

                await page.wait_for_timeout(int(DELAY_MIN * 1000))

        finally:
            try:
                await browser.close()
            except Exception:
                pass

    return results, stats


def write_results_to_gsheet(rows, label):
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
        "sellsy_id", "nom", "tag", "ville", "code_postal", "telephone",
        "doctolib_url", "nb_creneaux", "scrape_status", "scrape_mode",
        "scrape_date", "min_slots_threshold",
    ]
    ws.update("A1", [headers])

    if rows:
        values = [
            [
                r.get("sellsy_id"), r.get("nom") or "", r.get("tag") or "",
                r.get("ville") or "", r.get("code_postal") or "", r.get("telephone") or "",
                r.get("doctolib_url") or "", r.get("nb_creneaux", 0),
                r.get("scrape_status") or "", r.get("scrape_mode") or "",
                r.get("scrape_date") or "", r.get("min_slots_threshold", 0),
            ]
            for r in rows
        ]
        ws.update("A2", values, value_input_option="USER_ENTERED")

    log.info(f"GSheet : onglet `{ws_name}` ecrit ({len(rows)} lignes)")
    return ws_name


def run_with_ids(sellsy_ids, min_slots=5, days=7, label="ids"):
    started_at = datetime.now(timezone.utc).isoformat()
    log.info(f"=== sellsy_tag_scan PLAYWRIGHT IDS mode : {len(sellsy_ids)} ids min_slots={min_slots} days={days} ===")

    sellsy = SellsySync()
    sellsy._ensure_token()
    companies = []
    for i, sid in enumerate(sellsy_ids, 1):
        c = sellsy.get_company(int(sid))
        if c:
            c["_tag_label"] = label
            companies.append(c)
        if i % 200 == 0:
            log.info(f"  Sellsy fetch [{i}/{len(sellsy_ids)}]")
    log.info(f"  {len(companies)} fiches récupérées sur {len(sellsy_ids)} demandées")

    if not companies:
        return {"error": "no_companies_resolved", "ids_count": len(sellsy_ids), "started_at": started_at}

    # Lancer le scraping browser dans un event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        results, stats = loop.run_until_complete(_scrape_with_browser(companies, min_slots))
    finally:
        loop.close()

    qualified = sorted(
        [r for r in results if r["nb_creneaux"] >= min_slots],
        key=lambda r: r["nb_creneaux"], reverse=True,
    )
    ws_name = write_results_to_gsheet(qualified, label)

    return {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "mode": "ids_playwright",
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
    """Mode tags : on ne sait pas filtrer par tag via API v2 — retourne erreur explicite."""
    return {
        "error": "filter_by_smart_tags_unsupported_api_v2",
        "hint": "Utilise le mode `sellsy_ids=...` à la place. Récupère les IDs depuis l'export CSV Sellsy filtré par tag (Actions > Export CSV > format import).",
        "tag_names": tag_names,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", help="Smart-tag names (mode non supporté actuellement)")
    parser.add_argument("--ids", help="Sellsy company IDs, comma-separated")
    parser.add_argument("--min-slots", type=int, default=5)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--label", default="ids_batch")
    args = parser.parse_args()

    if args.ids:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
        summary = run_with_ids(ids, min_slots=args.min_slots, days=args.days, label=args.label)
    elif args.tags:
        tag_names = [t.strip() for t in args.tags.split(",") if t.strip()]
        summary = run(tag_names=tag_names, min_slots=args.min_slots, days=args.days)
    else:
        parser.error("--tags ou --ids requis")
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
