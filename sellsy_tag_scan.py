"""
sellsy_tag_scan.py — Scan ciblé des prospects Sellsy pour identifier
les "fauteuils vides" (>= seuil de créneaux Doctolib sur N jours).

V7 — Workflow réel Doctolib (booking + cliquer motif) :
  1. Navigate vers URL_PROFIL/booking?source=profile
     → redirige vers /booking/motives (résa active)
     → ou retourne 404/page profil (résa inactive)
  2. Si /booking/motives : cliquer le 1er motif "consultation"
  3. Attendre 8s → intercepte les XHR /availabilities.json
  4. Lit `total` du JSON pour le compte de slots sur 7 jours

Statuts :
  - ok           : XHR capturé, nb_creneaux = total
  - no_booking   : redirige pas vers /motives → résa Doctolib désactivée
  - no_motives   : page motives mais aucun motif clickable
  - exception    : erreur réseau / timeout
"""

from __future__ import annotations

import os, json, time, logging, argparse, asyncio
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SELLSY_API_URL = os.getenv("SELLSY_API_URL", "https://api.sellsy.com/v2")
SELLSY_CLIENT_ID = os.getenv("SELLSY_CLIENT_ID", "")
SELLSY_CLIENT_SECRET = os.getenv("SELLSY_CLIENT_SECRET", "")
SBR_WS = os.getenv("BRIGHT_DATA_BROWSER_WS",
    "wss://brd-customer-hl_dbe515e1-zone-doctolib_browser:ub9zsp721noa@brd.superproxy.io:9222")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

BROWSER_REFRESH_EVERY = int(os.getenv("BROWSER_REFRESH_EVERY", "30"))
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "25"))
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))


class SellsySync:
    def __init__(self):
        self.token = None
        self.token_expires = 0
        self.session = requests.Session()

    def _ensure_token(self):
        if self.token and time.time() < self.token_expires - 60: return
        r = self.session.post("https://login.sellsy.com/oauth2/access-tokens",
            json={"grant_type":"client_credentials","client_id":SELLSY_CLIENT_ID,"client_secret":SELLSY_CLIENT_SECRET},
            headers={"Content-Type":"application/json"}, timeout=30)
        r.raise_for_status()
        d = r.json()
        self.token = d["access_token"]
        self.token_expires = time.time() + d.get("expires_in", 3600)
        log.info("Sellsy token obtenu")

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def get_company(self, cid):
        self._ensure_token()
        try:
            r = self.session.get(f"{SELLSY_API_URL}/companies/{cid}", headers=self.headers, timeout=30)
            if r.status_code != 200: return None
            b = r.json()
            return b.get("data") or b
        except Exception:
            return None


def _doctolib_url(c):
    for k in ("website", "web", "site_web", "url"):
        v = (c or {}).get(k)
        if v and "doctolib" in str(v).lower(): return str(v)
    for nk in ("contact_information", "informations"):
        nest = (c or {}).get(nk) or {}
        for k in ("website", "web"):
            v = nest.get(k)
            if v and "doctolib" in str(v).lower(): return str(v)
    return ""


def _phone(c):
    for k in ("phone_number","phone","tel","mobile"):
        v = (c or {}).get(k)
        if v: return str(v)
    return ""


def _city(c):
    for n in ("address","billing_address","primary_address"):
        a = (c or {}).get(n) or {}
        v = a.get("city") or a.get("ville")
        if v: return str(v)
    return ""


def _postal(c):
    for n in ("address","billing_address","primary_address"):
        a = (c or {}).get(n) or {}
        v = a.get("postal_code") or a.get("zipcode") or a.get("zip_code")
        if v: return str(v)
    return ""


def _row(c, url, nb, status, mode, min_slots):
    return {
        "sellsy_id": int(c["id"]) if c.get("id") else None,
        "nom": c.get("name") or c.get("display_name") or "",
        "tag": c.get("_tag_label",""),
        "ville": _city(c),
        "code_postal": _postal(c),
        "telephone": _phone(c),
        "doctolib_url": url or "",
        "nb_creneaux": int(nb or 0),
        "scrape_status": status or "",
        "scrape_mode": mode or "",
        "scrape_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "min_slots_threshold": min_slots,
    }


def _gsheet_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_REFRESH_TOKEN: return None
    try:
        import gspread
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request as GRequest
    except ImportError:
        return None
    creds = Credentials(token=None, refresh_token=GOOGLE_REFRESH_TOKEN,
        client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    creds.refresh(GRequest())
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)


GHEADERS = ["sellsy_id","nom","tag","ville","code_postal","telephone","doctolib_url",
            "nb_creneaux","scrape_status","scrape_mode","scrape_date","min_slots_threshold"]


def _gsheet_init(sheet, name):
    try:
        ex = sheet.worksheet(name)
        sheet.del_worksheet(ex)
    except Exception:
        pass
    ws = sheet.add_worksheet(title=name, rows=5000, cols=12)
    ws.update("A1", [GHEADERS])
    return ws


def _gsheet_append(ws, rows):
    if not rows: return
    ws.append_rows([[r.get(h) for h in GHEADERS] for r in rows], value_input_option="USER_ENTERED")


def fetch_companies(sellsy, ids, label):
    log.info(f"=== Phase 1 : Fetch {len(ids)} fiches Sellsy ===")
    sellsy._ensure_token()
    out = []
    for i, sid in enumerate(ids, 1):
        c = sellsy.get_company(int(sid))
        if c:
            c["_tag_label"] = label
            out.append(c)
        if i % 200 == 0:
            log.info(f"  Sellsy fetch [{i}/{len(ids)}] (got {len(out)})")
    log.info(f"=== Phase 1 done : {len(out)} fiches ===")
    return out


# Mots-clés pour identifier le motif "consultation" (à cliquer en priorité)
MOTIVE_KEYWORDS = [
    "première consultation",
    "premiere consultation",
    "consultation dentaire",
    "soins dentaires",
    "détartrage",
    "detartrage",
    "consultation",
    "soins",
]


async def _scrape_one(page, profile_url, days):
    """Workflow Doctolib v7 : profile → /booking → motives → click → XHR.
    Retourne (nb_slots, status, mode)."""
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

    # Construire l'URL booking
    base = profile_url.rstrip("/")
    booking_url = base + "/booking?source=profile"

    try:
        await page.goto(booking_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    except Exception as e:
        return 0, "exception", f"goto:{type(e).__name__}"

    await page.wait_for_timeout(4000)

    # Vérifier où on est arrivé
    try:
        cur_url = page.url
    except Exception:
        cur_url = ""

    if "/booking/motives" not in cur_url and "/booking/availabilities" not in cur_url:
        # Probablement redirigé vers profil → pas de résa activée
        try:
            body = await page.evaluate("() => document.body.innerText || ''")
            if "n'est pas sur Doctolib" in body or "Revendiquer mon profil" in body:
                return 0, "not_on_doctolib", "negative_signal"
            if "réservation n'est pas disponible" in body or "ne propose pas la prise de rendez-vous" in body:
                return 0, "ok", "no_booking_active"
            return 0, "ok", "no_booking_redirect"
        except Exception:
            return 0, "no_data", "no_redirect_no_body"

    # On est sur /booking/motives → cliquer le 1er motif (v8 fix: bon filtre length + tri asc)
    if "/booking/motives" in cur_url:
        clicked_label = None
        try:
            # v8 : filtre strict + tri par length pour prendre le BOUTON, pas un parent
            clicked_label = await page.evaluate("""() => {
                const candidates = Array.from(document.querySelectorAll('a, button, [role="button"], li[class*="motive"]'))
                    .filter(e => {
                        const t = (e.innerText || '').trim();
                        if (t.length < 3 || t.length > 60) return false;
                        return /consultation|soins|d.tartrage|premi.re|examen|d.brid/i.test(t);
                    })
                    .sort((a, b) => a.innerText.length - b.innerText.length);
                if (candidates.length === 0) return null;
                candidates[0].click();
                return candidates[0].innerText.trim();
            }""")
            if not clicked_label:
                # Fallback : prendre n'importe quel bouton court (1er motif quel qu'il soit)
                clicked_label = await page.evaluate("""() => {
                    const candidates = Array.from(document.querySelectorAll('a, button'))
                        .filter(e => {
                            const t = (e.innerText || '').trim();
                            return t.length > 3 && t.length < 60;
                        })
                        .sort((a, b) => a.innerText.length - b.innerText.length);
                    // Skip headers/nav (souvent < 6 chars)
                    const target = candidates.find(e => e.innerText.trim().length > 6);
                    if (!target) return null;
                    target.click();
                    return target.innerText.trim();
                }""")

            if not clicked_label:
                return 0, "no_motives", "no_button_found"

        except Exception as e:
            return 0, "exception", f"motive_click:{type(e).__name__}"

        # Attendre les XHR
        await page.wait_for_timeout(8000)

    # Compter les slots sur les `days` premiers jours
    if not captured:
        return 0, "no_data", "no_xhr_after_click"

    total_slots = 0
    days_seen = 0
    for data in captured:
        if not isinstance(data, dict): continue
        # Doctolib retourne `total` (count global) + `availabilities` (par jour)
        for d in (data.get("availabilities") or []):
            slots = d.get("slots") or []
            total_slots += len(slots)
            days_seen += 1
            if days_seen >= days:
                break
        if days_seen >= days:
            break

    return total_slots, "ok", f"xhr_{len(captured)}resp_{days_seen}days"


async def _new_browser_page(pw):
    browser = await pw.chromium.connect_over_cdp(SBR_WS, timeout=60_000)
    page = await browser.new_page()
    return browser, page


async def _scrape_loop(companies, min_slots, days, ws, results_acc, stats):
    from playwright.async_api import async_playwright
    pending = []
    n = len(companies)

    async with async_playwright() as pw:
        browser = None
        page = None
        in_session = 0

        for i, c in enumerate(companies, 1):
            url = _doctolib_url(c)
            if not url:
                row = _row(c, "", 0, "no_url", "", min_slots)
                results_acc.append(row); pending.append(row)
                stats["no_url"] = stats.get("no_url", 0) + 1
            else:
                if browser is None or in_session >= BROWSER_REFRESH_EVERY:
                    if browser is not None:
                        try: await browser.close()
                        except Exception: pass
                    try:
                        browser, page = await _new_browser_page(pw)
                        in_session = 0
                        log.info(f"  [{i}/{n}] browser refreshed")
                    except Exception as e:
                        row = _row(c, url, 0, "exception", f"connect:{type(e).__name__}", min_slots)
                        results_acc.append(row); pending.append(row)
                        stats["exception"] = stats.get("exception", 0) + 1
                        await asyncio.sleep(5)
                        continue

                try:
                    nb, status, mode = await _scrape_one(page, url, days)
                    stats[status] = stats.get(status, 0) + 1
                    row = _row(c, url, nb, status, mode, min_slots)
                    results_acc.append(row); pending.append(row)
                    in_session += 1
                    if nb >= min_slots:
                        log.info(f"  ✓ FAUTEUIL VIDE [{i}/{n}]: {row['nom'][:40]} {row['ville'][:20]} = {nb} créneaux")
                except Exception as e:
                    err = type(e).__name__
                    log.warning(f"  [{i}/{n}] {url[:50]} -> {err}")
                    row = _row(c, url, 0, "exception", f"{err}:{str(e)[:80]}", min_slots)
                    results_acc.append(row); pending.append(row)
                    stats["exception"] = stats.get("exception", 0) + 1
                    try: await browser.close()
                    except Exception: pass
                    browser = None; page = None; in_session = 0
                    await asyncio.sleep(2)

            if ws is not None and len(pending) >= CHECKPOINT_EVERY:
                try:
                    _gsheet_append(ws, pending)
                    log.info(f"  [{i}/{n}] +{len(pending)} GSheet (cumul {len(results_acc)}, stats={dict(stats)})")
                    pending = []
                except Exception as e:
                    log.warning(f"  GSheet flush failed: {e}")

            await asyncio.sleep(0.3)

        if ws is not None and pending:
            try: _gsheet_append(ws, pending)
            except Exception as e: log.warning(f"  Final flush failed: {e}")

        if browser is not None:
            try: await browser.close()
            except Exception: pass


def run_with_ids(sellsy_ids, min_slots=5, days=7, label="ids"):
    started = datetime.now(timezone.utc).isoformat()
    log.info(f"=== sellsy_tag_scan v7 (booking flow) : {len(sellsy_ids)} ids ===")

    sellsy = SellsySync()
    companies = fetch_companies(sellsy, sellsy_ids, label)
    if not companies:
        return {"error":"no_companies","ids_count":len(sellsy_ids),"started_at":started}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ws_name = f"Fauteuils_Vides_{today}"
    sheet = _gsheet_open()
    ws = _gsheet_init(sheet, ws_name) if sheet else None

    results = []
    stats = {}

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_scrape_loop(companies, min_slots, days, ws, results, stats))
    except Exception as e:
        log.error(f"Fatal loop: {e}")
    finally:
        loop.close()

    qualified = sorted([r for r in results if r["nb_creneaux"] >= min_slots],
                       key=lambda r: r["nb_creneaux"], reverse=True)

    return {
        "started_at": started,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "mode": "ids_playwright_v7_booking",
        "ids_count": len(sellsy_ids),
        "scraped": len(results),
        "qualified_over_threshold": len(qualified),
        "min_slots": min_slots, "days": days,
        "stats": stats, "gsheet_tab": ws_name,
        "top10": [{"sellsy_id":r["sellsy_id"],"nom":r["nom"],"ville":r["ville"],"nb_creneaux":r["nb_creneaux"]}
                  for r in qualified[:10]],
    }


def run(tag_names, min_slots=5, days=7):
    return {"error":"filter_unsupported","hint":"use sellsy_ids","tag_names":tag_names}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ids"); p.add_argument("--tags")
    p.add_argument("--min-slots", type=int, default=5)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--label", default="ids_batch")
    a = p.parse_args()
    if a.ids:
        ids = [int(x.strip()) for x in a.ids.split(",") if x.strip()]
        s = run_with_ids(ids, a.min_slots, a.days, a.label)
    elif a.tags:
        s = run([t.strip() for t in a.tags.split(",")], a.min_slots, a.days)
    else:
        p.error("--ids ou --tags requis")
    print(json.dumps(s, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
