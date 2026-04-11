#!/usr/bin/env python3
"""
extract_phones_doctolib.py — Extrait les téléphones depuis les profils Doctolib.

Pour chaque dentiste "Oui" (présent sur Doctolib), visite le profil et
récupère le numéro de téléphone affiché.

Usage:
    python extract_phones_doctolib.py
    python extract_phones_doctolib.py --batch-size 100 --concurrency 3
"""

import os
import re
import asyncio
import logging
import argparse
import time
import json
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
log = logging.getLogger("extract_phones")

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

# Regex pour les numéros FR (01-09, +33, etc.)
PHONE_PATTERNS = [
    r'tel:(\+?[\d\s\.\-]{10,15})',
    r'"telephone"\s*:\s*"(\+?[\d\s\.\-]{10,15})"',
    r'"phone"\s*:\s*"(\+?[\d\s\.\-]{10,15})"',
    r'itemprop="telephone"[^>]*content="([^"]+)"',
    r'class="[^"]*phone[^"]*"[^>]*>[\s]*(\+?[\d\s\.\-]{10,15})',
    r'(?:Tél|Tel|Téléphone|Phone)\s*[.:]\s*(\+?[\d\s\.\-]{10,15})',
]

# Regex plus large pour capter les numéros FR dans le HTML
FR_PHONE_REGEX = re.compile(
    r'(?:(?:\+33|0033)\s*[1-9](?:[\s\.\-]?\d{2}){4}|0[1-9](?:[\s\.\-]?\d{2}){4})'
)


def get_proxy_url() -> str | None:
    if BRIGHT_DATA_USER and BRIGHT_DATA_PASS:
        return f"http://{BRIGHT_DATA_USER}:{BRIGHT_DATA_PASS}@{BRIGHT_DATA_HOST}:{BRIGHT_DATA_PORT}"
    return None


def get_worksheet():
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


def normalize_phone(phone: str) -> str:
    """Normalise un numéro FR au format 0X XX XX XX XX."""
    digits = re.sub(r'[^\d+]', '', phone)
    if digits.startswith('+33'):
        digits = '0' + digits[3:]
    elif digits.startswith('0033'):
        digits = '0' + digits[4:]
    if len(digits) == 10 and digits.startswith('0'):
        return f"{digits[0:2]} {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:10]}"
    return ""


def extract_phone_from_html(html: str, url: str) -> str:
    """Extrait le numéro de téléphone depuis le HTML d'un profil Doctolib."""
    phones_found = []

    for pattern in PHONE_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for m in matches:
            normalized = normalize_phone(m)
            if normalized:
                phones_found.append(normalized)

    if not phones_found:
        all_phones = FR_PHONE_REGEX.findall(html)
        for p in all_phones:
            normalized = normalize_phone(p)
            if normalized:
                phones_found.append(normalized)

    json_ld_matches = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
    for json_str in json_ld_matches:
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                tel = data.get("telephone") or data.get("phone") or ""
                if tel:
                    normalized = normalize_phone(tel)
                    if normalized:
                        phones_found.insert(0, normalized)
        except (json.JSONDecodeError, ValueError):
            pass

    seen = set()
    unique = []
    for p in phones_found:
        if p not in seen:
            seen.add(p)
            unique.append(p)

    return unique[0] if unique else ""


async def fetch_phone(client: httpx.AsyncClient, url: str, max_retries: int = 2) -> str:
    """Récupère le téléphone depuis un profil Doctolib."""
    if not url or not url.strip():
        return ""

    for attempt in range(max_retries + 1):
        try:
            resp = await client.get(url.strip(), headers=HTTP_HEADERS, follow_redirects=True, timeout=15)

            if resp.status_code == 200:
                return extract_phone_from_html(resp.text, url)
            elif resp.status_code == 429:
                if attempt < max_retries:
                    wait = 5 * (attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                return "Erreur 429"
            elif resp.status_code in (403,):
                if attempt < max_retries:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                return "Erreur 403"
            else:
                return ""

        except httpx.TimeoutException:
            if attempt < max_retries:
                await asyncio.sleep(2)
                continue
            return "Timeout"
        except Exception:
            return ""

    return ""


async def extract_batch(urls: list[tuple[int, str]], concurrency: int = 3) -> list[tuple[int, str]]:
    """Extrait les téléphones d'un batch d'URLs."""
    semaphore = asyncio.Semaphore(concurrency)
    results = []

    proxy_url = get_proxy_url()
    log.info(f"  Proxy: {'Bright Data' if proxy_url else 'AUCUN (direct)'}")

    async with httpx.AsyncClient(timeout=20.0, proxy=proxy_url) as client:
        async def fetch_one(row_idx: int, url: str):
            async with semaphore:
                phone = await fetch_phone(client, url)
                results.append((row_idx, phone))
                await asyncio.sleep(1)

        tasks = [fetch_one(idx, url) for idx, url in urls]
        await asyncio.gather(*tasks)

    return results


async def run(batch_size: int = 100, start_row: int = 2, concurrency: int = 3,
              skip_filled: bool = True):
    """Extrait les téléphones des profils Doctolib et met à jour le Google Sheet."""

    log.info("═══════════════════════════════════════════")
    log.info("  EXTRACTION TÉLÉPHONES DOCTOLIB")
    log.info(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"  Batch: {batch_size} | Concurrency: {concurrency}")
    log.info("═══════════════════════════════════════════")

    ws = get_worksheet()
    headers = ws.row_values(1)
    log.info(f"Headers: {headers}")

    url_col = headers.index("Doctolib_URL") + 1 if "Doctolib_URL" in headers else None
    est_col = headers.index("Est_Sur_Doctolib") + 1 if "Est_Sur_Doctolib" in headers else None
    tel_col = headers.index("Telephone") + 1 if "Telephone" in headers else None

    if not url_col or not est_col or not tel_col:
        log.error("Colonnes manquantes ! (Doctolib_URL, Est_Sur_Doctolib, Telephone)")
        return

    log.info("Lecture du Google Sheet...")
    all_data = ws.get_all_values()
    total_rows = len(all_data) - 1
    log.info(f"  {total_rows} lignes trouvées")

    work = []
    for i, row in enumerate(all_data[1:], start=2):
        if i < start_row:
            continue

        est_val = row[est_col - 1] if len(row) >= est_col else ""
        if est_val.strip() != "Oui":
            continue

        url = row[url_col - 1] if len(row) >= url_col else ""
        if not url.strip():
            continue

        if skip_filled:
            tel_val = row[tel_col - 1] if len(row) >= tel_col else ""
            if tel_val.strip() and not tel_val.strip().startswith("Erreur"):
                continue

        work.append((i, url.strip()))

    log.info(f"  {len(work)} profils Doctolib à scraper (skip_filled={skip_filled})")

    if not work:
        log.info("Rien à faire !")
        return

    total_checked = 0
    total_found = 0
    total_empty = 0
    total_errors = 0
    t_start = time.time()

    for batch_start in range(0, len(work), batch_size):
        batch = work[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(work) + batch_size - 1) // batch_size

        log.info(f"Batch {batch_num}/{total_batches} ({len(batch)} profils)...")

        results = await extract_batch(batch, concurrency=concurrency)

        cells_tel = []
        for row_idx, phone in sorted(results, key=lambda x: x[0]):
            if phone and not phone.startswith("Erreur") and phone != "Timeout":
                cells_tel.append(gspread.Cell(row=row_idx, col=tel_col, value=phone))
                total_found += 1
            elif phone.startswith("Erreur") or phone == "Timeout":
                total_errors += 1
            else:
                total_empty += 1

        if cells_tel:
            for attempt in range(5):
                try:
                    ws.update_cells(cells_tel)
                    break
                except gspread.exceptions.APIError as api_err:
                    err_code = str(api_err)
                    if "500" in err_code or "503" in err_code or "429" in err_code:
                        wait = 10 * (attempt + 1)
                        log.warning(f"  Google API error (attempt {attempt+1}/5): {err_code[:80]}. Retry in {wait}s...")
                        await asyncio.sleep(wait)
                        if attempt >= 2:
                            log.info("  Reconnecting to Google Sheets...")
                            ws = get_worksheet()
                    else:
                        raise
            else:
                log.error(f"  Failed to write phones after 5 attempts, skipping batch {batch_num}")

        total_checked += len(batch)
        elapsed = time.time() - t_start
        rate = total_checked / elapsed if elapsed > 0 else 0
        eta_seconds = (len(work) - total_checked) / rate if rate > 0 else 0
        eta_min = eta_seconds / 60

        log.info(
            f"  {total_found} trouvés | {total_empty} vides | "
            f"{total_errors} erreurs"
        )
        log.info(
            f"  Progression: {total_checked}/{len(work)} "
            f"({total_checked/len(work)*100:.1f}%) | "
            f"Vitesse: {rate:.1f}/s | ETA: {eta_min:.0f} min"
        )

        await asyncio.sleep(1)

    total_time = time.time() - t_start
    log.info(f"RÉSUMÉ EXTRACTION TÉLÉPHONES")
    log.info(f"  Total scrapé : {total_checked}")
    log.info(f"  Téléphones trouvés : {total_found}")
    log.info(f"  Pas de téléphone : {total_empty}")
    log.info(f"  Erreurs : {total_errors}")
    log.info(f"  Durée : {total_time/60:.1f} min")


def main():
    parser = argparse.ArgumentParser(description="Extrait les téléphones des profils Doctolib")
    parser.add_argument("--batch-size", type=int, default=100, help="Taille des batchs (défaut: 100)")
    parser.add_argument("--start-row", type=int, default=2, help="Ligne de départ (défaut: 2)")
    parser.add_argument("--concurrency", type=int, default=3, help="Requêtes parallèles (défaut: 3)")
    parser.add_argument("--force", action="store_true", help="Re-scraper même si téléphone déjà rempli")

    args = parser.parse_args()

    asyncio.run(run(
        batch_size=args.batch_size,
        start_row=args.start_row,
        concurrency=args.concurrency,
        skip_filled=not args.force,
    ))


if __name__ == "__main__":
    main()
