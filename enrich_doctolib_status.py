#!/usr/bin/env python3
"""
enrich_doctolib_status.py — Enrichit le Google Sheet avec Est_Sur_Doctolib / Pas_Sur_Doctolib.

Visite chaque URL Doctolib via HTTP GET simple et détecte si le dentiste est
réellement inscrit sur Doctolib ou non.

Critères :
  - HTTP 200 + "isProfileShowOrEdit" dans le HTML → Est sur Doctolib (Oui)
  - HTTP 200 sans "isProfileShowOrEdit"           → Pas sur Doctolib (Oui)
  - HTTP 410 / 404                                → Profil supprimé
  - HTTP 403 / timeout                            → Erreur (retry plus tard)

Usage:
    python enrich_doctolib_status.py                    # Enrichit tout le Sheet
    python enrich_doctolib_status.py --batch-size 500   # Par lots de 500
    python enrich_doctolib_status.py --start-row 1000   # Reprendre à la ligne 1000
    python enrich_doctolib_status.py --concurrency 20   # 20 requêtes parallèles
"""

import os
import asyncio
import logging
import argparse
import time
from datetime import datetime

import httpx
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("enrich")

# ─── Config ──────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

BRIGHT_DATA_HOST = os.getenv("BRIGHT_DATA_HOST", "brd.superproxy.io")
BRIGHT_DATA_PORT = os.getenv("BRIGHT_DATA_PORT", "33335")
BRIGHT_DATA_USER = os.getenv("BRIGHT_DATA_USER", "")
BRIGHT_DATA_PASS = os.getenv("BRIGHT_DATA_PASS", "")

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

def get_proxy_url() -> str | None:
    """Construit l'URL proxy Bright Data si configuré."""
    if BRIGHT_DATA_USER and BRIGHT_DATA_PASS:
        return f"http://{BRIGHT_DATA_USER}:{BRIGHT_DATA_PASS}@{BRIGHT_DATA_HOST}:{BRIGHT_DATA_PORT}"
    return None


# ─── Google Sheets ───────────────────────────────────────────────────────────
def get_worksheet():
    """Connexion au Google Sheet."""
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
    creds.refresh(Request())
    client = gspread.authorize(creds)
    sheet = client.open_by_key(GOOGLE_SHEET_ID)
    return sheet.worksheet("Dentistes")


# ─── Doctolib check ─────────────────────────────────────────────────────────
async def check_doctolib_url(client: httpx.AsyncClient, url: str, max_retries: int = 2) -> tuple[str, str]:
    """
    Vérifie si un dentiste est inscrit sur Doctolib via HTTP GET.
    Retourne (est_sur_doctolib, pas_sur_doctolib).
    Retry automatique sur 403 avec backoff.
    """
    if not url or not url.strip():
        return ("", "")

    for attempt in range(max_retries + 1):
        try:
            resp = await client.get(url.strip(), headers=HTTP_HEADERS, follow_redirects=True, timeout=15)

            if resp.status_code == 200:
                html = resp.text
                if "isProfileShowOrEdit" in html:
                    return ("Oui", "Non")
                else:
                    return ("Non", "Oui")
            elif resp.status_code in (404, 410):
                return ("Non", "Oui - Profil supprimé")
            elif resp.status_code == 403:
                if attempt < max_retries:
                    wait = 3 * (attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                return ("Erreur 403", "Erreur 403")
            else:
                return (f"Erreur {resp.status_code}", f"Erreur {resp.status_code}")

        except httpx.TimeoutException:
            if attempt < max_retries:
                await asyncio.sleep(2)
                continue
            return ("Timeout", "Timeout")
        except Exception as e:
            return ("Erreur", "Erreur")

    return ("Erreur", "Erreur")


async def check_batch_http(urls: list[tuple[int, str]], concurrency: int = 3) -> list[tuple[int, str, str]]:
    """Vérifie un batch d'URLs via HTTP direct (rapide mais peut être bloqué)."""
    semaphore = asyncio.Semaphore(concurrency)
    results = []

    async with httpx.AsyncClient(timeout=20.0) as client:

        async def check_one(row_idx: int, url: str):
            async with semaphore:
                est, pas = await check_doctolib_url(client, url)
                results.append((row_idx, est, pas))
                await asyncio.sleep(1)

        tasks = [check_one(idx, url) for idx, url in urls]
        await asyncio.gather(*tasks)

    return results


async def check_batch_browser(urls: list[tuple[int, str]]) -> list[tuple[int, str, str]]:
    """Vérifie un batch via Bright Data Browser API (lent mais fiable)."""
    from playwright.async_api import async_playwright

    SBR_WS = os.getenv(
        "BRIGHT_DATA_BROWSER_WS",
        "wss://brd-customer-hl_dbe515e1-zone-doctolib_browser:ub9zsp721noa@brd.superproxy.io:9222"
    )

    JS_CHECK = """
    () => {
        const body = document.body.innerText || '';
        const title = document.title || '';
        const notOn = body.includes("n'est pas sur Doctolib")
            || body.includes("n\\u2019est pas sur Doctolib")
            || body.includes("pas réservable en ligne")
            || body.includes("Revendiquer mon profil") && !body.includes("Prendre rendez-vous");
        const isOn = body.includes("Prendre rendez-vous")
            || body.includes("Carte Vitale")
            || body.includes("Tarifs et remboursement")
            || title.includes("Prenez RDV");
        return { isOn: isOn && !notOn, notOn: notOn };
    }
    """

    results = []
    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(SBR_WS)
        page = await browser.new_page()

        for row_idx, url in urls:
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                status = resp.status if resp else 0
                await page.wait_for_timeout(4000)

                if status in (404, 410):
                    results.append((row_idx, "Non", "Oui - Profil supprimé"))
                    log.debug(f"  {url.split('/')[-1]}: {status} supprimé")
                elif status == 200:
                    r = await page.evaluate(JS_CHECK)
                    if r.get("notOn"):
                        results.append((row_idx, "Non", "Oui"))
                    elif r.get("isOn"):
                        results.append((row_idx, "Oui", "Non"))
                    else:
                        results.append((row_idx, "?", "?"))
                else:
                    results.append((row_idx, f"Erreur {status}", f"Erreur {status}"))
            except Exception as e:
                log.debug(f"  {url.split('/')[-1]}: erreur {e}")
                results.append((row_idx, "Erreur", "Erreur"))

            await page.wait_for_timeout(500)

        await browser.close()

    return results


async def check_batch(urls: list[tuple[int, str]], concurrency: int = 3, use_browser: bool = False) -> list[tuple[int, str, str]]:
    """Dispatcher : HTTP ou Browser API."""
    if use_browser:
        return await check_batch_browser(urls)
    return await check_batch_http(urls, concurrency)


# ─── Main ────────────────────────────────────────────────────────────────────
async def run(batch_size: int = 200, start_row: int = 2, concurrency: int = 3,
              skip_filled: bool = True, use_browser: bool = False):
    """Enrichit le Google Sheet avec le statut Doctolib."""

    log.info("═══════════════════════════════════════════")
    log.info("  ENRICHISSEMENT EST/PAS SUR DOCTOLIB")
    log.info(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"  Batch: {batch_size} | Concurrency: {concurrency}")
    log.info("═══════════════════════════════════════════")

    # Connexion Google Sheet
    ws = get_worksheet()
    headers = ws.row_values(1)
    log.info(f"Headers: {headers}")

    # Trouver les colonnes
    url_col = headers.index("Doctolib_URL") + 1 if "Doctolib_URL" in headers else None
    est_col = headers.index("Est_Sur_Doctolib") + 1 if "Est_Sur_Doctolib" in headers else None
    pas_col = headers.index("Pas_Sur_Doctolib") + 1 if "Pas_Sur_Doctolib" in headers else None

    if not url_col:
        log.error("Colonne Doctolib_URL introuvable !")
        return

    if not est_col or not pas_col:
        log.error("Colonnes Est_Sur_Doctolib / Pas_Sur_Doctolib introuvables !")
        return

    # Lire toutes les données
    log.info("📥 Lecture du Google Sheet...")
    all_data = ws.get_all_values()
    total_rows = len(all_data) - 1  # sans header
    log.info(f"  {total_rows} lignes trouvées")

    # Construire la liste de travail
    work = []
    for i, row in enumerate(all_data[1:], start=2):  # row 2 = première donnée
        if i < start_row:
            continue

        url = row[url_col - 1] if len(row) >= url_col else ""
        if not url.strip():
            continue

        # Skip si déjà rempli
        if skip_filled:
            est_val = row[est_col - 1] if len(row) >= est_col else ""
            if est_val.strip() and est_val.strip() not in ("?", "Erreur", "Timeout"):
                continue

        work.append((i, url.strip()))

    log.info(f"  {len(work)} lignes à vérifier (skip_filled={skip_filled})")

    if not work:
        log.info("✅ Rien à faire !")
        return

    # Traiter par batch
    total_checked = 0
    total_on = 0
    total_off = 0
    total_errors = 0
    t_start = time.time()

    for batch_start in range(0, len(work), batch_size):
        batch = work[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(work) + batch_size - 1) // batch_size

        log.info(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} URLs)...")

        # Vérifier le batch
        results = await check_batch(batch, concurrency=concurrency, use_browser=use_browser)

        # Préparer les mises à jour (batch update pour performance)
        cells_est = []
        cells_pas = []
        for row_idx, est, pas in sorted(results, key=lambda x: x[0]):
            cells_est.append(gspread.Cell(row=row_idx, col=est_col, value=est))
            cells_pas.append(gspread.Cell(row=row_idx, col=pas_col, value=pas))

            if est == "Oui":
                total_on += 1
            elif pas.startswith("Oui"):
                total_off += 1
            else:
                total_errors += 1

        # Écrire en batch dans le Sheet
        if cells_est:
            ws.update_cells(cells_est)
        if cells_pas:
            ws.update_cells(cells_pas)

        total_checked += len(batch)
        elapsed = time.time() - t_start
        rate = total_checked / elapsed if elapsed > 0 else 0
        eta_seconds = (len(work) - total_checked) / rate if rate > 0 else 0
        eta_min = eta_seconds / 60

        log.info(
            f"  ✅ {total_on} sur Doctolib | ❌ {total_off} pas sur Doctolib | "
            f"⚠️ {total_errors} erreurs"
        )
        log.info(
            f"  📊 Progression: {total_checked}/{len(work)} "
            f"({total_checked/len(work)*100:.1f}%) | "
            f"Vitesse: {rate:.1f}/s | ETA: {eta_min:.0f} min"
        )

        # Petit délai entre les batchs pour ne pas surcharger l'API Google
        await asyncio.sleep(1)

    # Résumé
    total_time = time.time() - t_start
    log.info(f"\n{'═'*50}")
    log.info(f"  RÉSUMÉ ENRICHISSEMENT")
    log.info(f"{'═'*50}")
    log.info(f"  Total vérifié : {total_checked}")
    log.info(f"  ✅ Sur Doctolib : {total_on}")
    log.info(f"  ❌ Pas sur Doctolib : {total_off}")
    log.info(f"  ⚠️  Erreurs : {total_errors}")
    log.info(f"  ⏱️  Durée : {total_time/60:.1f} min")
    log.info(f"{'═'*50}")


def main():
    parser = argparse.ArgumentParser(description="Enrichit le Google Sheet avec Est/Pas sur Doctolib")
    parser.add_argument("--batch-size", type=int, default=200, help="Taille des batchs (défaut: 200)")
    parser.add_argument("--start-row", type=int, default=2, help="Ligne de départ (défaut: 2)")
    parser.add_argument("--concurrency", type=int, default=15, help="Requêtes parallèles (défaut: 15)")
    parser.add_argument("--force", action="store_true", help="Re-vérifier même les lignes déjà remplies")
    parser.add_argument("--browser", action="store_true", help="Utiliser Bright Data Browser API (lent mais fiable)")

    args = parser.parse_args()

    asyncio.run(run(
        batch_size=args.batch_size,
        start_row=args.start_row,
        concurrency=args.concurrency,
        skip_filled=not args.force,
        use_browser=args.browser,
    ))


if __name__ == "__main__":
    main()
