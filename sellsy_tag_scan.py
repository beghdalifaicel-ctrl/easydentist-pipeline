"""
sellsy_tag_scan.py — Scan ciblé des prospects Sellsy pour identifier
les "fauteuils vides" (>= seuil de créneaux Doctolib sur N jours).

V5 — Playwright + Bright Data Browser CDP, robuste :
  - Try/except par URL (1 fail ne tue plus le scan)
  - Reconnexion browser tous les BROWSER_REFRESH_EVERY prospects
  - Checkpoint progressif dans GSheet (perd plus rien en cas de crash)
"""

from __future__ import annotations

import os
import json
import time
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

# Reconnect browser every N prospects to avoid Bright Data session timeouts
BROWSER_REFRESH_EVERY = int(os.getenv("BROWSER_REFRESH_EVERY", "50"))
# Flush checkpoint to GSheet every N prospects
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "50"))


JS_COUNT_DOCTOLIB_SLOTS = r"""
() => {
    const body = (document.body && document.body.innerText) ? document.body.innerText : "";
    const title = document.title || "";

    const notOnDoctolib = body.includes("n'est pas sur Doctolib")
        || body.includes("n’est pas sur Doctolib")
        || body.includes("Revendiquer mon profil")
        || body.includes("pas réservable en ligne");
    if (notOnDoctolib) return {status: "not_on_doctolib", nb_slots: 0, mode: "negative_signal"};

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
            let visible = 0;
            els.forEach(e => {
                const r = e.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) visible++;
            });
            if (visible > 0) { slot_count = visible; used_selector = sel; break; }
        }
    }
    if (slot_count > 0) return {status: "ok", nb_slots: slot_count, mode: "dom_count", selector: used_selector};

    if (body.match(/Aucune disponibilit[eé]/i)) return {status: "ok", nb_slots: 0, mode: "text_no_avail"};

    const m = body.match(/Prochain (?:rendez-vous|RDV)\s*:?\s*([A-Za-zéè]+\s+\d+\s+[a-zéû]+)/i);
    if (m) return {status: "ok", nb_slots: 1, mode: "text_proximate", date: m[1]};

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
            json={"grant_type": "client_credentials",
                  "client_id": SELLSY_CLIENT_ID, "client_secret": SELLSY_CLIENT_SECRET},
            headers={"Content-Type": "application/json"}, timeout=30,
        )
        resp.raise_for_status()
        d = resp.json()
        self.token = d["access_token"]
        self.token_expires = time.time() + d.get("expires_in", 3600)
        log.info("Sellsy token obtenu")

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def get_company(self, company_id):
        self._ensure_token()
        try:
            r = self.session.get(f"{SELLSY_API_URL}/companies/{company_id}",
                                 headers=self.headers, timeout=30)
            if r.status_code != 200:
                return None
            b = r.json()
            return b.get("data") or b
        except Exception:
            return None


def _company_doctolib_url(company):
    for k in ("website", "web", "site_web", "url"):
        v = (company or {}).get(k)
        if v and "doctolib" in str(v).lower():
            return str(v)
    for nk in ("contact_information", "informations"):
        nest = (company or {}).get(nk) or {}
        for k in ("website", "web"):
            v = nest.get(k)
            if v and "doctolib" in str(v).lower():
                return str(v)
    return ""


def _company_phone(c):
    for k in ("phone_number", "phone", "tel", "mobile"):
        v = (c or {}).get(k)
        if v: return str(v)
    return ""


def _company_city(c):
    for n in ("address", "billing_address", "primary_address"):
        a = (c or {}).get(n) or {}
        v = a.get("city") or a.get("ville")
        if v: return str(v)
    return ""


def _company_postal(c):
    for n in ("address", "billing_address", "primary_address"):
        a = (c or {}).get(n) or {}
        v = a.get("postal_code") or a.get("zipcode") or a.get("zip_code")
        if v: return str(v)
    return ""


def _row_for_company(c, url, nb, status, mode, min_slots):
    return {
        "sellsy_id": int(c["id"]) if c.get("id") else None,
        "nom": c.get("name") or c.get("display_name") or "",
        "tag": c.get("_tag_label", ""),
        "ville": _company_city(c),
        "code_postal": _company_postal(c),
        "telephone": _company_phone(c),
        "doctolib_url": url or "",
        "nb_creneaux": int(nb or 0),
        "scrape_status": status or "",
        "scrape_mode": mode or "",
        "scrape_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "min_slots_threshold": min_slots,
    }


# ─── GSheet checkpoint ────────────────────────────────────────────────────────
def _gsheet_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_REFRESH_TOKEN:
        return None
    try:
        import gspread
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request as GRequest
    except ImportError:
        return None
    creds = Credentials(
        token=None, refresh_token=GOOGLE_REFRESH_TOKEN,
        client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(GRequest())
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)


GSHEET_HEADERS = [
    "sellsy_id", "nom", "tag", "ville", "code_postal", "telephone",
    "doctolib_url", "nb_creneaux", "scrape_status", "scrape_mode",
    "scrape_date", "min_slots_threshold",
]


def _gsheet_init_tab(sheet, ws_name):
    try:
        existing = sheet.worksheet(ws_name)
        sheet.del_worksheet(existing)
    except Exception:
        pass
    ws = sheet.add_worksheet(title=ws_name, rows=5000, cols=12)
    ws.update("A1", [GSHEET_HEADERS])
    return ws


def _gsheet_append_rows(ws, rows):
    """Append batch of rows to gsheet."""
    if not rows:
        return
    values = [[r.get(h) for h in GSHEET_HEADERS] for r in rows]
    ws.append_rows(values, value_input_option="USER_ENTERED")


# ─── Sellsy fetch (phase 1) ───────────────────────────────────────────────────
def fetch_companies(sellsy, sellsy_ids, label):
    log.info(f"=== Phase 1 : Fetch {len(sellsy_ids)} fiches Sellsy ===")
    sellsy._ensure_token()
    companies = []
    for i, sid in enumerate(sellsy_ids, 1):
        c = sellsy.get_company(int(sid))
        if c:
            c["_tag_label"] = label
            companies.append(c)
        if i % 200 == 0:
            log.info(f"  Sellsy fetch [{i}/{len(sellsy_ids)}] (got {len(companies)} valid)")
    log.info(f"=== Phase 1 done : {len(companies)} fiches récupérées ===")
    return companies


# ─── Browser scraping with reconnect (phase 2) ────────────────────────────────
async def _new_browser_page(pw):
    browser = await pw.chromium.connect_over_cdp(SBR_WS, timeout=60_000)
    page = await browser.new_page()
    return browser, page


async def _scrape_with_browser_robust(companies, min_slots, ws, results_acc, stats):
    from playwright.async_api import async_playwright

    pending_ckpt = []  # rows depuis le dernier checkpoint
    n = len(companies)

    async with async_playwright() as pw:
        browser = None
        page = None
        scraped_in_session = 0

        for i, c in enumerate(companies, 1):
            url = _company_doctolib_url(c)

            if not url:
                row = _row_for_company(c, "", 0, "no_url", "", min_slots)
                results_acc.append(row); pending_ckpt.append(row)
                stats["no_url"] = stats.get("no_url", 0) + 1
            else:
                # (Re)connect browser if needed
                if browser is None or scraped_in_session >= BROWSER_REFRESH_EVERY:
                    if browser is not None:
                        try: await browser.close()
                        except Exception: pass
                    try:
                        browser, page = await _new_browser_page(pw)
                        scraped_in_session = 0
                        log.info(f"  [{i}/{n}] Browser session refreshed")
                    except Exception as e:
                        log.error(f"  [{i}/{n}] Cannot connect Bright Data: {e}")
                        row = _row_for_company(c, url, 0, "exception", f"connect:{type(e).__name__}", min_slots)
                        results_acc.append(row); pending_ckpt.append(row)
                        stats["exception"] = stats.get("exception", 0) + 1
                        # Wait + retry next iteration
                        await asyncio.sleep(5)
                        continue

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    await page.wait_for_timeout(5000)
                    try:
                        await page.evaluate("window.scrollBy(0, 600)")
                        await page.wait_for_timeout(1500)
                    except Exception:
                        pass

                    res = await page.evaluate(JS_COUNT_DOCTOLIB_SLOTS)
                    status = res.get("status") or "no_data"
                    nb = int(res.get("nb_slots") or 0)
                    mode = res.get("mode") or ""
                    if status not in stats: stats[status] = 0
                    stats[status] += 1
                    row = _row_for_company(c, url, nb, status, mode, min_slots)
                    results_acc.append(row); pending_ckpt.append(row)
                    scraped_in_session += 1
                except Exception as e:
                    err = type(e).__name__
                    log.warning(f"  [{i}/{n}] {url[:60]} -> {err}")
                    row = _row_for_company(c, url, 0, "exception", f"{err}:{str(e)[:80]}", min_slots)
                    results_acc.append(row); pending_ckpt.append(row)
                    stats["exception"] = stats.get("exception", 0) + 1
                    # Browser may be dead → force reconnect next iter
                    try: await browser.close()
                    except Exception: pass
                    browser = None
                    page = None
                    scraped_in_session = 0
                    await asyncio.sleep(2)

            # Checkpoint
            if ws is not None and len(pending_ckpt) >= CHECKPOINT_EVERY:
                try:
                    _gsheet_append_rows(ws, pending_ckpt)
                    log.info(f"  [{i}/{n}] checkpoint +{len(pending_ckpt)} rows GSheet (cumul {len(results_acc)})")
                    pending_ckpt = []
                except Exception as e:
                    log.warning(f"  GSheet checkpoint failed: {e}")

            # Periodic progress log
            if i % 25 == 0:
                qcount = sum(1 for r in results_acc if r["nb_creneaux"] >= min_slots)
                log.info(f"  [{i}/{n}] cumul stats={dict(stats)} >=seuil={qcount}")

            await asyncio.sleep(0.3)

        # Final flush
        if ws is not None and pending_ckpt:
            try:
                _gsheet_append_rows(ws, pending_ckpt)
                log.info(f"  Final checkpoint +{len(pending_ckpt)} rows")
            except Exception as e:
                log.warning(f"  Final GSheet flush failed: {e}")

        if browser is not None:
            try: await browser.close()
            except Exception: pass


# ─── Orchestrator ─────────────────────────────────────────────────────────────
def run_with_ids(sellsy_ids, min_slots=5, days=7, label="ids"):
    started_at = datetime.now(timezone.utc).isoformat()
    log.info(f"=== sellsy_tag_scan v5 : {len(sellsy_ids)} ids min_slots={min_slots} ===")

    sellsy = SellsySync()
    companies = fetch_companies(sellsy, sellsy_ids, label)
    if not companies:
        return {"error": "no_companies_resolved", "ids_count": len(sellsy_ids), "started_at": started_at}

    # Init GSheet tab for checkpointing
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ws_name = f"Fauteuils_Vides_{today}"
    sheet = _gsheet_open()
    ws = _gsheet_init_tab(sheet, ws_name) if sheet else None
    if ws is None:
        log.warning("GSheet indisponible → pas de checkpoint, résultat seulement en mémoire")

    results = []
    stats = {"ok": 0, "no_url": 0, "not_on_doctolib": 0, "no_data": 0, "exception": 0}

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_scrape_with_browser_robust(companies, min_slots, ws, results, stats))
    except Exception as e:
        log.error(f"Fatal in scrape loop: {e}")
    finally:
        loop.close()

    qualified = sorted([r for r in results if r["nb_creneaux"] >= min_slots],
                       key=lambda r: r["nb_creneaux"], reverse=True)

    return {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "mode": "ids_playwright_v5",
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
    return {
        "error": "filter_by_smart_tags_unsupported_api_v2",
        "hint": "Utilise sellsy_ids depuis l'export CSV.",
        "tag_names": tag_names,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", help="Smart-tag names (mode non supporté)")
    parser.add_argument("--ids", help="Sellsy company IDs, comma-separated")
    parser.add_argument("--min-slots", type=int, default=5)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--label", default="ids_batch")
    args = parser.parse_args()

    if args.ids:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
        summary = run_with_ids(ids, min_slots=args.min_slots, days=args.days, label=args.label)
    elif args.tags:
        tags = [t.strip() for t in args.tags.split(",") if t.strip()]
        summary = run(tag_names=tags, min_slots=args.min_slots, days=args.days)
    else:
        parser.error("--tags ou --ids requis")
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
