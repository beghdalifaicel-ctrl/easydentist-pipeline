"""
audit_clients_doctolib.py — Audit hebdomadaire dispo Doctolib des clients Easydentist.

V3.1 — Fix Cloudflare 403 (navigate to booking URL before XHR fetch).
       Le cf_clearance Doctolib est path-bound : un fetch XHR vers /booking/<slug>.json
       depuis la homepage seule était rejeté en 403. On navigue à la page de booking
       réelle d'abord (CF clear le challenge JS, cookies + Referer corrects), puis
       fetch JSON depuis le même contexte.

V3 — API JSON Doctolib directe (fini le scraping click-by-click) :
  1. Parse l'URL Doctolib pour extraire slug + IDs (placeId, practitionerId, motiveIds)
  2. Bootstrap session via Bright Data Browser (cookies Doctolib homepage)
  3. PER URL: navigate to booking page (établit cf_clearance + cookies booking-specific)
  4. Si motive_ids manquants → fetch /booking/{slug}.json pour avoir agenda_ids + motive_ids
  5. Fetch /availabilities.json?practitioner_ids[]=X&motive_ids[]=Y&start_date=Z&limit=14
  6. Score = slots 7j × 2 + slots 8-14j × 1 → reco UP/HOLD/DOWN/PAUSE
  7. Output : onglet Audit_YYYY-MM-DD + colonne historique sur le sheet principal

Source sheet : "Cron dépenses vs dispo clients" (env AUDIT_CLIENTS_SHEET_ID).
"""

from __future__ import annotations

import os, json, time, logging, asyncio, re
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SBR_WS = os.getenv(
    "BRIGHT_DATA_BROWSER_WS",
    "wss://brd-customer-hl_dbe515e1-zone-doctolib_browser:ub9zsp721noa@brd.superproxy.io:9222",
)
AUDIT_SHEET_ID = os.getenv(
    "AUDIT_CLIENTS_SHEET_ID",
    "1tR3DTTITPr3RGb5iL19F8eroEOCiMTX98h7xPMIFXaM",
)
AUDIT_SHEET_TAB = os.getenv("AUDIT_CLIENTS_SHEET_TAB", "")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

BROWSER_REFRESH_EVERY = int(os.getenv("BROWSER_REFRESH_EVERY", "40"))
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))
CF_CLEARANCE_WAIT_MS = int(os.getenv("CF_CLEARANCE_WAIT_MS", "3500"))


# ─── Google Sheets helpers ──────────────────────────────────────────────────
def _gsheet_open():
    if not GOOGLE_REFRESH_TOKEN:
        log.error("GOOGLE_REFRESH_TOKEN manquant")
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
    return gspread.authorize(creds).open_by_key(AUDIT_SHEET_ID)


def _read_clients(sheet):
    ws = sheet.worksheet(AUDIT_SHEET_TAB) if AUDIT_SHEET_TAB else sheet.get_worksheet(0)
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return ws, []

    header = [h.strip().lower() for h in rows[0]]

    def col(name_substr):
        for i, h in enumerate(header):
            if name_substr in h:
                return i
        return -1

    idx_client = col("client")
    idx_budget = col("budget")
    idx_target_dep = col("target dépense") if col("target dépense") >= 0 else col("target depense")
    idx_target_conv = col("target conversion")
    idx_landing = col("landing")
    idx_doctolib = col("planning") if col("planning") >= 0 else col("docto")

    clients = []
    for i, row in enumerate(rows[1:], start=2):
        if not row or all(not c.strip() for c in row):
            continue
        url = row[idx_doctolib].strip() if 0 <= idx_doctolib < len(row) else ""
        if not url or "doctolib.fr" not in url.lower():
            continue
        clients.append({
            "row_index": i,
            "client": row[idx_client].strip() if 0 <= idx_client < len(row) else "",
            "budget_raw": row[idx_budget].strip() if 0 <= idx_budget < len(row) else "",
            "target_depense": row[idx_target_dep].strip() if 0 <= idx_target_dep < len(row) else "",
            "target_conversion": row[idx_target_conv].strip() if 0 <= idx_target_conv < len(row) else "",
            "landing": row[idx_landing].strip() if 0 <= idx_landing < len(row) else "",
            "doctolib_url": url,
        })
    return ws, clients


def _parse_budget(raw: str) -> int:
    if not raw:
        return 0
    s = re.sub(r"[^\d]", "", raw)
    return int(s) if s else 0


# ─── Scoring ────────────────────────────────────────────────────────────────
def compute_reco(slots_7j, slots_8_14j, next_rdv_days, budget):
    if budget <= 0:
        return {"score": 0, "reco": "Budget manquant", "action": "À renseigner",
                "budget_recommande": 0, "color": "GRAY"}
    score = slots_7j * 2 + slots_8_14j * 1
    if score == 0 and (next_rdv_days is None or next_rdv_days > 21):
        return {"score": score, "reco": "PAUSE",
                "action": "Couper provisoirement (saturé)",
                "budget_recommande": 0, "color": "RED"}
    if score >= 30:
        new_b = round(budget * 1.50)
        return {"score": score, "reco": "UP+50%",
                "action": f"Augmenter de {budget}€ à {new_b}€",
                "budget_recommande": new_b, "color": "PURPLE"}
    if score >= 15:
        new_b = round(budget * 1.20)
        return {"score": score, "reco": "UP+20%",
                "action": f"Augmenter de {budget}€ à {new_b}€",
                "budget_recommande": new_b, "color": "BLUE"}
    if score >= 5:
        return {"score": score, "reco": "HOLD", "action": "Maintenir",
                "budget_recommande": budget, "color": "GREEN"}
    if score >= 1:
        new_b = round(budget * 0.70)
        return {"score": score, "reco": "DOWN-30%",
                "action": f"Baisser de {budget}€ à {new_b}€",
                "budget_recommande": new_b, "color": "ORANGE"}
    new_b = round(budget * 0.80)
    return {"score": score, "reco": "DOWN-20%",
            "action": f"Baisser de {budget}€ à {new_b}€ (faible dispo)",
            "budget_recommande": new_b, "color": "ORANGE"}


# ─── Doctolib API helpers ───────────────────────────────────────────────────
def _parse_doctolib_url(url: str) -> dict:
    """Extract slug + IDs from any Doctolib booking URL."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    path = parsed.path
    if "/booking" in path:
        slug = path.split("/booking", 1)[0].lstrip("/")
    else:
        slug = path.lstrip("/").rstrip("/")

    def _ids(key):
        # Doctolib accepts both `key[]=` and `key=` for arrays
        out = []
        for k in (f"{key}[]", key):
            for v in qs.get(k, []):
                try:
                    out.append(int(v))
                except (ValueError, TypeError):
                    pass
        return out

    practitioner_id = None
    pid_raw = (qs.get("practitionerId") or qs.get("practitioner_id") or [None])[0]
    if pid_raw:
        try:
            practitioner_id = int(pid_raw)
        except (ValueError, TypeError):
            pass

    return {
        "slug": slug,
        "place_id": (qs.get("placeId") or qs.get("place_id") or [None])[0],
        "practitioner_id": practitioner_id,
        "motive_ids": _ids("motiveIds") + _ids("motive_ids"),
        "motive_category_ids": _ids("motiveCategoryIds") + _ids("motive_category_ids"),
        "speciality_id": (qs.get("specialityId") or qs.get("speciality_id") or [None])[0],
    }


async def _fetch_json(page, url: str):
    """
    Fetch JSON via the Bright Data browser. Page must be on a doctolib.fr/<booking>
    URL first (cf_clearance + Referer flow). Adds explicit Referer header for safety
    in case browser context strips it.
    """
    try:
        result = await page.evaluate("""async (u) => {
            try {
                const r = await fetch(u, {
                    headers: {
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Referer': location.href
                    },
                    credentials: 'include'
                });
                const status = r.status;
                if (!r.ok) return {__error: 'http_' + status};
                const text = await r.text();
                try { return JSON.parse(text); }
                catch(e) { return {__error: 'parse_fail', __body: text.slice(0, 200)}; }
            } catch(e) { return {__error: 'fetch_fail', __detail: String(e)}; }
        }""", url)
        return result if isinstance(result, dict) else {"__error": "non_dict"}
    except Exception as e:
        return {"__error": "evaluate_fail", "__detail": str(e)[:80]}


async def _bootstrap_session(page) -> bool:
    """Navigate to doctolib homepage to set up baseline cookies + JS context."""
    try:
        await page.goto("https://www.doctolib.fr/", wait_until="domcontentloaded",
                        timeout=PAGE_TIMEOUT_MS)
        await page.wait_for_timeout(2500)
        return True
    except Exception as e:
        log.warning(f"Bootstrap fail: {e}")
        return False


async def _new_browser_page(pw):
    browser = await pw.chromium.connect_over_cdp(SBR_WS, timeout=60_000)
    page = await browser.new_page()
    return browser, page


async def _scrape_one(page, url: str):
    """
    Fetch availabilities via Doctolib's JSON API.
    Returns: (slots_7j, slots_8_14j, next_rdv_iso, status, mode)

    V3.1 fix: navigate to actual booking URL FIRST. The cf_clearance cookie is
    path-bound, so XHR to /booking/<slug>.json from homepage alone gets 403.
    """
    parsed = _parse_doctolib_url(url)
    slug = parsed["slug"]
    if not slug:
        return 0, 0, None, "no_slug", "url_unparseable"

    # ─── Navigate to booking page → CF challenge resolved + cookies + Referer set ───
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await page.wait_for_timeout(CF_CLEARANCE_WAIT_MS)
    except Exception as e:
        return 0, 0, None, "navigate_fail", f"{type(e).__name__}:{str(e)[:60]}"

    motive_ids = list(parsed["motive_ids"])
    practitioner_id = parsed["practitioner_id"]
    agenda_ids = []
    visit_motives_lookup = {}  # id -> name, for picking right motives

    # Step 1: get booking config
    config_url = f"https://www.doctolib.fr/booking/{slug}.json"
    cfg = await _fetch_json(page, config_url)
    if "__error" in cfg:
        return 0, 0, None, "config_fail", cfg.get("__error", "?")

    try:
        data = cfg.get("data") or {}
        agendas = data.get("agendas") or []
        if not agendas:
            return 0, 0, None, "no_agenda", "config_empty"

        # Match agenda(s) to practitioner_id if specified, else take all
        for a in agendas:
            aid = a.get("id")
            apid = a.get("practitioner_id")
            if practitioner_id and apid != practitioner_id:
                continue
            if aid:
                agenda_ids.append(aid)
            if not practitioner_id and apid:
                practitioner_id = apid

        if not agenda_ids:
            # Fallback: take all agendas
            agenda_ids = [a.get("id") for a in agendas if a.get("id")]

        # Build motive lookup
        for vm in (data.get("visit_motives") or []):
            mid = vm.get("id")
            if mid:
                visit_motives_lookup[mid] = (vm.get("name") or "").lower()

        # If we don't have motive_ids from URL, pick "consultation/première"
        if not motive_ids:
            re_consult = re.compile(r"consultation|premi[èe]re|soins|d[ée]tartrage|examen|nouveau patient|nouvelle patient", re.I)
            for mid, name in visit_motives_lookup.items():
                if re_consult.search(name):
                    motive_ids.append(mid)
            # Fallback to first 3 if nothing matched
            if not motive_ids and visit_motives_lookup:
                motive_ids = list(visit_motives_lookup.keys())[:3]

    except Exception as e:
        return 0, 0, None, "config_parse_fail", f"{type(e).__name__}:{str(e)[:60]}"

    if not motive_ids:
        return 0, 0, None, "no_motive_ids", "after_lookup"
    if not agenda_ids:
        return 0, 0, None, "no_agenda_ids", "after_lookup"

    # Step 2: fetch availabilities
    today = datetime.now(timezone.utc).date()
    params = [f"start_date={today.strftime('%Y-%m-%d')}", "limit=14"]
    for mid in motive_ids[:5]:
        params.append(f"motive_ids[]={mid}")
    for aid in agenda_ids[:5]:
        params.append(f"agenda_ids[]={aid}")
    if practitioner_id:
        params.append(f"practitioner_ids[]={practitioner_id}")

    avail_url = f"https://www.doctolib.fr/availabilities.json?{'&'.join(params)}"
    avail = await _fetch_json(page, avail_url)
    if "__error" in avail:
        return 0, 0, None, "avail_fail", avail.get("__error", "?")

    # Step 3: count slots
    slots_7j = 0
    slots_8_14j = 0
    next_iso = None
    ns = avail.get("next_slot")
    if isinstance(ns, str):
        next_iso = ns[:10]

    for d in (avail.get("availabilities") or []):
        date_str = (d.get("date") or "")[:10]
        slots = d.get("slots") or []
        if not date_str or not slots:
            continue
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        delta = (day - today).days
        if 0 <= delta <= 7:
            slots_7j += len(slots)
        elif 8 <= delta <= 14:
            slots_8_14j += len(slots)
        if not next_iso:
            next_iso = date_str

    total = avail.get("total", 0)
    return slots_7j, slots_8_14j, next_iso, "ok", f"api_total={total}_motives={len(motive_ids)}"


# ─── Loop ──────────────────────────────────────────────────────────────────
async def _scrape_loop(clients, results_acc, stats):
    from playwright.async_api import async_playwright
    n = len(clients)

    async with async_playwright() as pw:
        browser = None
        page = None
        in_session = 0

        for i, c in enumerate(clients, 1):
            url = c["doctolib_url"]
            budget = _parse_budget(c["budget_raw"])

            # (Re)connect browser + bootstrap session
            if browser is None or in_session >= BROWSER_REFRESH_EVERY:
                if browser is not None:
                    try: await browser.close()
                    except Exception: pass
                try:
                    browser, page = await _new_browser_page(pw)
                    ok = await _bootstrap_session(page)
                    if not ok:
                        log.warning(f"  [{i}/{n}] bootstrap fail")
                    in_session = 0
                    log.info(f"  [{i}/{n}] browser refreshed")
                except Exception as e:
                    log.warning(f"  [{i}/{n}] connect fail: {e}")
                    results_acc.append({**c, "budget": budget,
                        "slots_7j": 0, "slots_8_14j": 0, "next_rdv": None,
                        "status": "exception", "mode": f"connect:{type(e).__name__}",
                        **compute_reco(0, 0, None, budget)})
                    stats["exception"] = stats.get("exception", 0) + 1
                    await asyncio.sleep(5)
                    continue

            try:
                s7, s14, next_iso, status, mode = await _scrape_one(page, url)
                stats[status] = stats.get(status, 0) + 1

                next_rdv_days = None
                if next_iso:
                    try:
                        d = datetime.strptime(next_iso, "%Y-%m-%d").date()
                        next_rdv_days = (d - datetime.now(timezone.utc).date()).days
                    except ValueError:
                        pass

                reco = compute_reco(s7, s14, next_rdv_days, budget)
                row = {**c, "budget": budget,
                       "slots_7j": s7, "slots_8_14j": s14,
                       "next_rdv": next_iso, "next_rdv_days": next_rdv_days,
                       "status": status, "mode": mode, **reco}
                results_acc.append(row)
                in_session += 1
                log.info(f"  [{i}/{n}] {c['client'][:30]:30s} 7j={s7:3d} 14j={s14:3d} score={reco['score']:3d} -> {reco['reco']} ({status})")
            except Exception as e:
                log.warning(f"  [{i}/{n}] {c['client'][:30]} exception: {e}")
                results_acc.append({**c, "budget": budget,
                    "slots_7j": 0, "slots_8_14j": 0, "next_rdv": None,
                    "status": "exception", "mode": f"{type(e).__name__}:{str(e)[:60]}",
                    **compute_reco(0, 0, None, budget)})
                stats["exception"] = stats.get("exception", 0) + 1
                try: await browser.close()
                except Exception: pass
                browser = None
                page = None
                in_session = 0
                await asyncio.sleep(2)

            await asyncio.sleep(0.4)

        if browser is not None:
            try: await browser.close()
            except Exception: pass


# ─── GSheet write ──────────────────────────────────────────────────────────
AUDIT_HEADERS = [
    "Client", "URL Doctolib", "Budget actuel (€)",
    "Créneaux 7j", "Créneaux 8-14j", "Prochain RDV", "Score",
    "Reco", "Action", "Budget recommandé (€)", "Color",
    "Status scrape", "Mode", "Date audit",
]


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _write_audit_tab(sheet, results, today_str):
    name = f"Audit_{today_str}"
    try:
        ex = sheet.worksheet(name)
        sheet.del_worksheet(ex)
    except Exception:
        pass
    ws = sheet.add_worksheet(title=name, rows=max(200, len(results) + 10), cols=len(AUDIT_HEADERS))
    ws.update("A1", [AUDIT_HEADERS])
    rows = []
    for r in results:
        rows.append([
            r.get("client", ""), r.get("doctolib_url", ""), r.get("budget", 0),
            r.get("slots_7j", 0), r.get("slots_8_14j", 0),
            r.get("next_rdv") or "", r.get("score", 0),
            r.get("reco", ""), r.get("action", ""),
            r.get("budget_recommande", 0), r.get("color", ""),
            r.get("status", ""), r.get("mode", ""), today_str,
        ])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    return name


def _write_history_column(ws_main, results, today_str):
    try:
        existing = ws_main.row_values(1)
        new_col_idx = len(existing) + 1
        col_letter = _col_letter(new_col_idx)
        ws_main.update(f"{col_letter}1", [[f"Reco {today_str}"]])
        idx_to_reco = {r["row_index"]: f"{r.get('reco','')} (score {r.get('score',0)})"
                       for r in results}
        last_row = max([r["row_index"] for r in results]) if results else 1
        col_data = [[idx_to_reco.get(ri, "")] for ri in range(2, last_row + 1)]
        if col_data:
            ws_main.update(f"{col_letter}2", col_data, value_input_option="USER_ENTERED")
        return col_letter
    except Exception as e:
        log.warning(f"history column write failed: {e}")
        return None


# ─── Entry point ───────────────────────────────────────────────────────────
def run_audit(min_slots: int = 5, days: int = 14, dry_run: bool = False):
    started = datetime.now(timezone.utc).isoformat()
    log.info(f"=== audit_clients_doctolib v3.1 (API JSON + CF fix) start (dry_run={dry_run}) ===")

    sheet = _gsheet_open()
    if sheet is None:
        return {"error": "no_gsheet_creds", "started_at": started}

    ws_main, clients = _read_clients(sheet)
    log.info(f"Clients lus : {len(clients)}")
    if not clients:
        return {"error": "no_clients_found", "started_at": started}

    if dry_run:
        return {"dry_run": True, "clients_count": len(clients),
                "sample": clients[:5], "started_at": started}

    results = []
    stats = {}
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_scrape_loop(clients, results, stats))
    except Exception as e:
        log.error(f"Fatal loop: {e}")
    finally:
        loop.close()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    audit_tab = _write_audit_tab(sheet, results, today)
    hist_col = _write_history_column(ws_main, results, today)

    by_reco = {}
    for r in results:
        by_reco[r.get("reco", "?")] = by_reco.get(r.get("reco", "?"), 0) + 1
    eco = sum(max(0, r.get("budget", 0) - r.get("budget_recommande", 0))
              for r in results if r.get("reco", "").startswith(("DOWN", "PAUSE")))
    invest = sum(max(0, r.get("budget_recommande", 0) - r.get("budget", 0))
                 for r in results if r.get("reco", "").startswith("UP"))

    return {
        "started_at": started,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "version": "v3.1_api_json_cf_fix",
        "clients_count": len(clients),
        "scraped": len(results),
        "audit_tab": audit_tab,
        "history_column": hist_col,
        "by_reco": by_reco,
        "economie_potentielle_eur": eco,
        "investissement_recommande_eur": invest,
        "stats": stats,
        "top_alertes": [
            {"client": r["client"], "reco": r.get("reco"),
             "score": r.get("score"), "budget": r.get("budget"),
             "budget_reco": r.get("budget_recommande"),
             "next_rdv": r.get("next_rdv")}
            for r in sorted(results, key=lambda x: x.get("score", 0))[:10]
        ],
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    out = run_audit(dry_run=a.dry_run)
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
