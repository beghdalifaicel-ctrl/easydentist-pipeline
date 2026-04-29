"""
audit_clients_doctolib.py — Audit hebdomadaire de la dispo Doctolib des CLIENTS Easydentist
pour piloter leur budget Google Ads (UP / HOLD / DOWN / PAUSE).

Source : Google Sheet "Cron dépenses vs dispo clients" (ID configurable via env)
  Colonnes attendues (header en ligne 1) :
    Clients | Budget | Target dépense € | Target conversion hebdo |
    Landing Rdv | Planning docto/planity | Nb de créneaux à remplir sur 14 jours glissants |
    Action (augmenter, baisser couper)

Workflow :
  1. Lit toutes les lignes (skip vides + lignes sans URL Doctolib)
  2. Pour chaque URL : Playwright + Bright Data Browser
       a. Navigate vers l'URL TELLE QUELLE (les URLs du sheet contiennent déjà
          motiveIds / motive-categories / etc.)
       b. Selon la page atteinte :
            - /booking/motive-categories -> click 1ère catégorie
            - /booking/motives           -> click 1er motif "consultation/soins/..."
            - /booking/availabilities ou autre -> attendre les XHR direct
       c. Capture les XHR /availabilities.json
       d. Compte les slots sur J0..J7 et J8..J14, prochaine date dispo
  3. Score = slots_7j * 2 + slots_8_14j * 1
  4. Reco selon seuils :
       Score ≥ 30           -> UP+50%
       Score 15..29         -> UP+20%
       Score 5..14          -> HOLD
       Score 1..4           -> DOWN
       Score 0 et next>21j  -> PAUSE
       Pas de budget        -> "Budget manquant"
  5. Écrit un nouvel onglet daté "Audit_YYYY-MM-DD" + ajoute une colonne datée
     dans l'onglet principal pour historiser.

Endpoint Flask : voir make_webhook.py (route /trigger/audit-clients-doctolib).
"""

from __future__ import annotations

import os, json, time, logging, asyncio, re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── ENV ────────────────────────────────────────────────────────────────────
SBR_WS = os.getenv(
    "BRIGHT_DATA_BROWSER_WS",
    "wss://brd-customer-hl_dbe515e1-zone-doctolib_browser:ub9zsp721noa@brd.superproxy.io:9222",
)

# Sheet "Cron dépenses vs dispo clients"
AUDIT_SHEET_ID = os.getenv(
    "AUDIT_CLIENTS_SHEET_ID",
    "1tR3DTTITPr3RGb5iL19F8eroEOCiMTX98h7xPMIFXaM",
)
AUDIT_SHEET_TAB = os.getenv("AUDIT_CLIENTS_SHEET_TAB", "")  # vide => premier onglet

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

BROWSER_REFRESH_EVERY = int(os.getenv("BROWSER_REFRESH_EVERY", "30"))
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))


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
        log.error("gspread/google-auth manquants")
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
        # Filtre doctolib seulement
        if not url or "doctolib.fr" not in url.lower():
            continue
        clients.append({
            "row_index": i,  # 1-based dans la feuille source
            "client": row[idx_client].strip() if 0 <= idx_client < len(row) else "",
            "budget_raw": row[idx_budget].strip() if 0 <= idx_budget < len(row) else "",
            "target_depense": row[idx_target_dep].strip() if 0 <= idx_target_dep < len(row) else "",
            "target_conversion": row[idx_target_conv].strip() if 0 <= idx_target_conv < len(row) else "",
            "landing": row[idx_landing].strip() if 0 <= idx_landing < len(row) else "",
            "doctolib_url": url,
        })
    return ws, clients


def _parse_budget(raw: str) -> int:
    """Parse '2 000 €' -> 2000. Retourne 0 si vide/invalide."""
    if not raw:
        return 0
    s = re.sub(r"[^\d]", "", raw)
    return int(s) if s else 0


# ─── Scoring ───────────────────────────────────────────────────────────────
def compute_reco(slots_7j: int, slots_8_14j: int, next_rdv_days: int | None,
                 budget: int) -> dict:
    """
    Score = slots_7j * 2 + slots_8_14j * 1
    Retourne dict {score, reco, action, budget_recommande, color}
    """
    if budget <= 0:
        return {
            "score": 0, "reco": "Budget manquant", "action": "À renseigner",
            "budget_recommande": 0, "color": "GRAY",
        }

    score = slots_7j * 2 + slots_8_14j * 1

    # PAUSE si saturé : 0 slot 14j ET prochain RDV > 21j (ou inconnu)
    if score == 0 and (next_rdv_days is None or next_rdv_days > 21):
        return {
            "score": score, "reco": "PAUSE",
            "action": "Couper provisoirement (saturé)",
            "budget_recommande": 0, "color": "RED",
        }

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
        return {"score": score, "reco": "HOLD",
                "action": "Maintenir",
                "budget_recommande": budget, "color": "GREEN"}
    if score >= 1:
        new_b = round(budget * 0.70)
        return {"score": score, "reco": "DOWN-30%",
                "action": f"Baisser de {budget}€ à {new_b}€",
                "budget_recommande": new_b, "color": "ORANGE"}

    # score == 0 mais next_rdv connu et <= 21j : DOWN soft
    new_b = round(budget * 0.80)
    return {"score": score, "reco": "DOWN-20%",
            "action": f"Baisser de {budget}€ à {new_b}€ (faible dispo)",
            "budget_recommande": new_b, "color": "ORANGE"}


# ─── Scraper ────────────────────────────────────────────────────────────────
MOTIVE_KEYWORDS = re.compile(
    r"consultation|soins|d[ée]tartrage|premi[èe]re|examen|d[ée]brid|nouvelle? patient",
    re.I,
)


def _normalize_url(url: str) -> str:
    """
    Strip query string + tout ce qui est après /booking, et reconstruit en
    /booking?source=profile pour forcer Doctolib à passer par le funnel motif standard.

    Ex:
      /dentiste/paris/talia-guez/booking/motives?motiveCategoryIds=...
      → /dentiste/paris/talia-guez/booking?source=profile

      /centre-dentaire/.../booking/availabilities?...
      → /centre-dentaire/.../booking?source=profile

      /dentiste/marseille/foo/booking
      → /dentiste/marseille/foo/booking?source=profile
    """
    if "/booking" in url:
        base = url.split("/booking", 1)[0]
    else:
        base = url.split("?", 1)[0].rstrip("/")
    return base + "/booking?source=profile"


async def _new_browser_page(pw):
    browser = await pw.chromium.connect_over_cdp(SBR_WS, timeout=60_000)
    page = await browser.new_page()
    return browser, page


async def _scrape_one(page, url: str, max_days: int = 14):
    """
    Navigate vers `url` (normalisée en /booking?source=profile), gère les étapes
    motive-categories / motives, capture les XHR /availabilities.json, retourne :
       (slots_7j, slots_8_14j, next_rdv_iso, status, mode)
    """
    captured = []

    async def handle_response(response):
        try:
            if "availabilities.json" in response.url:
                ct = response.headers.get("content-type", "")
                if "json" in ct.lower():
                    try:
                        data = await response.json()
                        captured.append(data)
                    except Exception:
                        pass
        except Exception:
            pass

    page.on("response", handle_response)

    nav_url = _normalize_url(url)

    try:
        await page.goto(nav_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    except Exception as e:
        return 0, 0, None, "exception", f"goto:{type(e).__name__}"

    await page.wait_for_timeout(4000)

    try:
        cur_url = page.url
    except Exception:
        cur_url = ""

    # Détection page erreur / pas sur Doctolib
    try:
        body = await page.evaluate("() => document.body.innerText || ''")
    except Exception:
        body = ""

    if "n'est pas sur Doctolib" in body or "Page introuvable" in body:
        return 0, 0, None, "not_on_doctolib", "negative_signal"

    # Étape : motive-categories ?
    if "/booking/motive-categories" in cur_url:
        try:
            clicked = await page.evaluate("""() => {
                const cands = Array.from(document.querySelectorAll('a, button, [role="button"], li'))
                    .filter(e => {
                        const t = (e.innerText || '').trim();
                        return t.length > 3 && t.length < 80;
                    })
                    .sort((a,b) => a.innerText.length - b.innerText.length);
                const target = cands.find(e => e.innerText.trim().length > 6);
                if (!target) return null;
                target.click();
                return target.innerText.trim();
            }""")
            await page.wait_for_timeout(3000)
            cur_url = page.url
        except Exception as e:
            return 0, 0, None, "exception", f"cat_click:{type(e).__name__}"

    # Étape : motives ?
    if "/booking/motives" in cur_url:
        try:
            clicked = await page.evaluate(r"""() => {
                const re = /consultation|soins|d.tartrage|premi.re|examen|d.brid|nouvelle? patient/i;
                let cands = Array.from(document.querySelectorAll('a, button, [role="button"], li'))
                    .filter(e => {
                        const t = (e.innerText || '').trim();
                        return t.length > 3 && t.length < 60 && re.test(t);
                    })
                    .sort((a,b) => a.innerText.length - b.innerText.length);
                if (cands.length === 0) {
                    cands = Array.from(document.querySelectorAll('a, button'))
                        .filter(e => {
                            const t = (e.innerText || '').trim();
                            return t.length > 6 && t.length < 60;
                        })
                        .sort((a,b) => a.innerText.length - b.innerText.length);
                }
                if (cands.length === 0) return null;
                cands[0].click();
                return cands[0].innerText.trim();
            }""")
            if not clicked:
                return 0, 0, None, "no_motives", "no_button_found"
            await page.wait_for_timeout(8000)
        except Exception as e:
            return 0, 0, None, "exception", f"motive_click:{type(e).__name__}"
    else:
        # Pas de page motives -> on attend simplement les XHR
        await page.wait_for_timeout(6000)

    # Compte les slots
    if not captured:
        return 0, 0, None, "no_data", "no_xhr"

    slots_7j = 0
    slots_8_14j = 0
    next_rdv_iso = None
    today = datetime.now(timezone.utc).date()

    for data in captured:
        if not isinstance(data, dict):
            continue
        # Cherche `next_slot` ou `next_availability` au top level
        for key in ("next_slot", "next_availability", "nextAvailability"):
            v = data.get(key)
            if v and not next_rdv_iso:
                if isinstance(v, str):
                    next_rdv_iso = v[:10]
                elif isinstance(v, dict):
                    next_rdv_iso = v.get("date") or v.get("start_date")
        # Slots par jour
        for d in (data.get("availabilities") or []):
            date_str = d.get("date") or ""
            slots = d.get("slots") or []
            if not date_str or not slots:
                continue
            try:
                day = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            delta = (day - today).days
            if 0 <= delta <= 7:
                slots_7j += len(slots)
            elif 8 <= delta <= 14:
                slots_8_14j += len(slots)
            if not next_rdv_iso and slots:
                next_rdv_iso = date_str[:10]

    return slots_7j, slots_8_14j, next_rdv_iso, "ok", f"xhr_{len(captured)}resp"


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

            if browser is None or in_session >= BROWSER_REFRESH_EVERY:
                if browser is not None:
                    try:
                        await browser.close()
                    except Exception:
                        pass
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
                row = {
                    **c, "budget": budget,
                    "slots_7j": s7, "slots_8_14j": s14,
                    "next_rdv": next_iso, "next_rdv_days": next_rdv_days,
                    "status": status, "mode": mode, **reco,
                }
                results_acc.append(row)
                in_session += 1
                log.info(
                    f"  [{i}/{n}] {c['client'][:30]:30s} "
                    f"7j={s7:3d} 14j={s14:3d} score={reco['score']:3d} -> {reco['reco']}"
                )
            except Exception as e:
                log.warning(f"  [{i}/{n}] {c['client'][:30]} exception: {e}")
                results_acc.append({**c, "budget": budget,
                    "slots_7j": 0, "slots_8_14j": 0, "next_rdv": None,
                    "status": "exception", "mode": f"{type(e).__name__}:{str(e)[:60]}",
                    **compute_reco(0, 0, None, budget)})
                stats["exception"] = stats.get("exception", 0) + 1
                try:
                    await browser.close()
                except Exception:
                    pass
                browser = None
                page = None
                in_session = 0
                await asyncio.sleep(2)

            await asyncio.sleep(0.5)

        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass


# ─── GSheet write ──────────────────────────────────────────────────────────
AUDIT_HEADERS = [
    "Client", "URL Doctolib", "Budget actuel (€)",
    "Créneaux 7j", "Créneaux 8-14j", "Prochain RDV", "Score",
    "Reco", "Action", "Budget recommandé (€)", "Color",
    "Status scrape", "Mode", "Date audit",
]


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
            r.get("client", ""),
            r.get("doctolib_url", ""),
            r.get("budget", 0),
            r.get("slots_7j", 0),
            r.get("slots_8_14j", 0),
            r.get("next_rdv") or "",
            r.get("score", 0),
            r.get("reco", ""),
            r.get("action", ""),
            r.get("budget_recommande", 0),
            r.get("color", ""),
            r.get("status", ""),
            r.get("mode", ""),
            today_str,
        ])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    return name


def _write_history_column(ws_main, results, today_str):
    """Ajoute une colonne 'Reco YYYY-MM-DD' en bout de l'onglet principal pour historiser."""
    try:
        existing = ws_main.row_values(1)
        new_col_idx = len(existing) + 1
        col_letter = _col_letter(new_col_idx)
        # header
        ws_main.update(f"{col_letter}1", [[f"Reco {today_str}"]])
        # data : on doit aligner sur row_index de chaque résultat
        rows_to_write = []
        # Map row_index -> reco string
        idx_to_reco = {r["row_index"]: f"{r.get('reco','')} (score {r.get('score',0)})"
                       for r in results}
        # Récupérer la hauteur de la feuille
        last_row = max([r["row_index"] for r in results]) if results else 1
        col_data = []
        for ri in range(2, last_row + 1):
            col_data.append([idx_to_reco.get(ri, "")])
        if col_data:
            ws_main.update(f"{col_letter}2", col_data, value_input_option="USER_ENTERED")
        return col_letter
    except Exception as e:
        log.warning(f"history column write failed: {e}")
        return None


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ─── Entry point ───────────────────────────────────────────────────────────
def run_audit(min_slots: int = 5, days: int = 14, dry_run: bool = False):
    """
    Exécute l'audit complet :
      - Lit le sheet "Cron dépenses vs dispo clients"
      - Scrape chaque URL Doctolib
      - Calcule score + reco
      - Écrit un onglet Audit_YYYY-MM-DD + colonne historique sur l'onglet principal
    """
    started = datetime.now(timezone.utc).isoformat()
    log.info(f"=== audit_clients_doctolib start (dry_run={dry_run}) ===")

    sheet = _gsheet_open()
    if sheet is None:
        return {"error": "no_gsheet_creds", "started_at": started}

    ws_main, clients = _read_clients(sheet)
    log.info(f"Clients lus : {len(clients)}")
    if not clients:
        return {"error": "no_clients_found", "started_at": started}

    if dry_run:
        return {
            "dry_run": True,
            "clients_count": len(clients),
            "sample": clients[:5],
            "started_at": started,
        }

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

    # Synthèse
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
             "budget_reco": r.get("budget_recommande")}
            for r in sorted(results, key=lambda x: x.get("score", 0))[:5]
        ],
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    out = run_audit(dry_run=a.dry_run)
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
