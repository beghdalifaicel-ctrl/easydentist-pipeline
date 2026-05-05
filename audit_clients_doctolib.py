"""
audit_clients_doctolib.py — Audit hebdomadaire dispo Doctolib des clients Easydentist.

V3.2 — DOM scraping via Bright Data Browser (Playwright CDP).
       Le V3.1 essayait fetch JSON XHR (/booking/<slug>.json) mais Cloudflare niveau 2
       bloquait systématiquement (config_fail/http_403). Le V3.2 abandonne le JSON et
       parse directement le DOM rendu de la page de booking après navigation + clicks
       à travers le funnel motif/place. Le Bright Data Browser bypass Cloudflare niveau 1
       (validé en MCP). On lit les boutons de slot dans le DOM et on agrège par date.

Flow :
  1. Navigate to booking URL (auto-bypass CF level 1 via BD Browser)
  2. Auto-accept cookie consent (didomi)
  3. Loop max N steps :
     - Detect page state (not_bookable / slots / picker)
     - If slots → parse aria-labels, count by date bucket (7j / 8-14j)
     - If picker (motif ou place) → click first valid option, wait, retry
     - If not_bookable → return 0
  4. Output : onglet Audit_YYYY-MM-DD + colonne historique sur le sheet principal.

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
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "45000"))
SPA_INITIAL_WAIT_MS = int(os.getenv("SPA_INITIAL_WAIT_MS", "4000"))
SPA_TRANSITION_WAIT_MS = int(os.getenv("SPA_TRANSITION_WAIT_MS", "2500"))
MAX_FUNNEL_STEPS = int(os.getenv("MAX_FUNNEL_STEPS", "4"))

NO_BOOKING_KEYWORDS = [
    "n'est pas réservable",
    "pas réservable en ligne",
    "pas de disponibilités",
    "aucune disponibilité",
    "indisponible",
    "ne peut pas prendre rendez-vous",
]
SKIP_BUTTON_KEYWORDS = [
    "accepter", "refuser", "en savoir plus", "se connecter", "menu",
    "fermer", "retour", "étape précédente", "rechercher", "passer cette étape",
]
FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


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


# ─── URL parsing (toujours utile pour le log) ───────────────────────────────
def _parse_doctolib_url(url: str) -> dict:
    parsed = urlparse(url)
    path = parsed.path
    if "/booking" in path:
        slug = path.split("/booking", 1)[0].lstrip("/")
    else:
        slug = path.lstrip("/").rstrip("/")
    return {"slug": slug}


# ─── DOM scraping helpers (V3.2) ────────────────────────────────────────────
async def _accept_cookies(page):
    """Accepte le bandeau cookies didomi si présent (silencieux si absent)."""
    try:
        btn = await page.query_selector('#didomi-notice-agree-button')
        if btn:
            await btn.click()
            await page.wait_for_timeout(800)
            return True
    except Exception:
        pass
    return False


async def _detect_no_booking(page) -> bool:
    """Renvoie True si la page indique 'pas réservable / pas de dispo'."""
    try:
        body_text = (await page.inner_text("body")).lower()
    except Exception:
        return False
    return any(kw in body_text for kw in NO_BOOKING_KEYWORDS)


def _parse_french_date(text: str):
    """Extrait une date d'un texte type 'lundi 5 mai 2026' ou '2026-05-05'."""
    if not text:
        return None
    iso_m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if iso_m:
        try:
            return datetime.strptime(iso_m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    fr_m = re.search(r"(\d{1,2})\s+([a-zéûâïôê]+)\s+(\d{4})", text.lower())
    if fr_m:
        try:
            d = int(fr_m.group(1))
            mo = FR_MONTHS.get(fr_m.group(2))
            y = int(fr_m.group(3))
            if mo:
                return datetime(y, mo, d).date()
        except (ValueError, KeyError):
            pass
    return None


async def _count_slots(page) -> dict:
    """
    Cherche les boutons de slot horaire dans le DOM, parse leurs aria-labels
    (ou textes parents) pour extraire la date, et bucket par 7j / 8-14j.
    """
    today = datetime.now(timezone.utc).date()
    s7 = 0
    s14 = 0
    next_iso = None
    found_any = False

    # Sélecteurs ordonnés par spécificité — Doctolib utilise différents patterns
    selectors = [
        'button[data-test-id^="slot-"]',
        'button[data-test*="slot"]',
        'button[data-test*="availability"]',
        'a[data-test-id^="slot-"]',
        'button[aria-label*=":"][aria-label*="20"]',  # heure + année
    ]

    seen_aria = set()
    for sel in selectors:
        try:
            els = await page.query_selector_all(sel)
        except Exception:
            continue
        for el in els:
            try:
                aria = (await el.get_attribute("aria-label")) or ""
                if not aria or aria in seen_aria:
                    continue
                seen_aria.add(aria)
                # Extract date — d'abord depuis l'aria, sinon depuis le parent (heading de jour)
                day = _parse_french_date(aria)
                if day is None:
                    # Try parent's aria-label or text
                    try:
                        parent = await el.evaluate_handle("el => el.closest('[data-test-id*=\"day\"], [data-test-id*=\"date\"], section, article')")
                        if parent:
                            ptext = await parent.evaluate("el => el.getAttribute('aria-label') || el.getAttribute('data-test-id') || el.innerText.slice(0, 80)")
                            day = _parse_french_date(ptext or "")
                    except Exception:
                        pass
                if day is None:
                    # Fallback: si on n'a pas la date mais le bouton existe, on compte au moins 1 slot dispo
                    found_any = True
                    continue
                found_any = True
                delta = (day - today).days
                if 0 <= delta <= 7:
                    s7 += 1
                elif 8 <= delta <= 14:
                    s14 += 1
                if next_iso is None or day.isoformat() < next_iso:
                    if delta >= 0:
                        next_iso = day.isoformat()
            except Exception:
                continue
        if found_any:
            break  # Sélecteur trouvé, ne pas itérer sur les suivants

    return {"found": found_any, "s7": s7, "s14": s14, "next_iso": next_iso}


async def _click_first_picker_option(page) -> bool:
    """
    Détecte un picker (motif/place) et clique la 1ʳᵉ option valide pour avancer.
    Retourne True si un click a été effectué.
    """
    selectors = [
        # Doctolib React design system cards (cliquables)
        '[data-design-system-component="Card"]:not([aria-disabled="true"])',
        'button[data-design-system-component="Button"][aria-label*="motif"]',
        'button.dl-card-content:not([disabled])',
        'a.dl-card:not([aria-disabled="true"])',
        'div[data-test*="motive"] button',
        'div[data-test*="place"] button',
        # Fallback: bouton dans main avec texte > 5 chars (skip nav)
        'main button:not([disabled])',
        'main a[role="button"]',
    ]
    for sel in selectors:
        try:
            els = await page.query_selector_all(sel)
        except Exception:
            continue
        for el in els:
            try:
                if not await el.is_visible():
                    continue
                txt = ((await el.inner_text()) or "").strip()
                low = txt.lower()
                if len(txt) < 3 or len(txt) > 200:
                    continue
                if any(s in low for s in SKIP_BUTTON_KEYWORDS):
                    continue
                await el.click()
                return True
            except Exception:
                continue
    return False


async def _new_browser_page(pw):
    browser = await pw.chromium.connect_over_cdp(SBR_WS, timeout=60_000)
    page = await browser.new_page()
    return browser, page


async def _scrape_one(page, url: str):
    """
    V3.2 DOM scraping.
    Returns: (slots_7j, slots_8_14j, next_rdv_iso, status, mode)
    """
    parsed = _parse_doctolib_url(url)
    if not parsed["slug"]:
        return 0, 0, None, "no_slug", "url_unparseable"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await page.wait_for_timeout(SPA_INITIAL_WAIT_MS)
    except Exception as e:
        return 0, 0, None, "navigate_fail", f"{type(e).__name__}:{str(e)[:60]}"

    # Cookies (silencieux si pas de bandeau)
    await _accept_cookies(page)

    # Loop : détecter état et avancer
    last_step_status = "unknown"
    for step in range(MAX_FUNNEL_STEPS):
        # 1) Page indique "non réservable" ?
        if await _detect_no_booking(page):
            return 0, 0, None, "ok", f"not_bookable_step_{step}"

        # 2) On voit des slots ?
        slots = await _count_slots(page)
        if slots["found"]:
            return slots["s7"], slots["s14"], slots["next_iso"], "ok", f"slots_step_{step}_total={slots['s7']+slots['s14']}"

        # 3) Sinon on est sur un picker → click 1ʳᵉ option
        clicked = await _click_first_picker_option(page)
        if not clicked:
            last_step_status = f"no_clickable_step_{step}"
            break
        await page.wait_for_timeout(SPA_TRANSITION_WAIT_MS)
        last_step_status = f"clicked_step_{step}"

    # Re-vérif après dernier click
    if await _detect_no_booking(page):
        return 0, 0, None, "ok", f"not_bookable_after_{last_step_status}"
    slots = await _count_slots(page)
    if slots["found"]:
        return slots["s7"], slots["s14"], slots["next_iso"], "ok", f"slots_after_{last_step_status}"

    return 0, 0, None, "no_slots_found", last_step_status


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

            # (Re)connect browser quand nécessaire
            if browser is None or in_session >= BROWSER_REFRESH_EVERY:
                if browser is not None:
                    try: await browser.close()
                    except Exception: pass
                try:
                    browser, page = await _new_browser_page(pw)
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

            await asyncio.sleep(0.5)

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
    log.info(f"=== audit_clients_doctolib v3.2 (DOM scraping) start (dry_run={dry_run}) ===")

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
        "version": "v3.2_dom_scraping",
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
