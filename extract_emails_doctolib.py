#!/usr/bin/env python3
"""
extract_emails_doctolib.py — Extrait les emails depuis les profils/sites Doctolib.

Pour chaque dentiste "Oui" (présent sur Doctolib), visite le profil et
récupère l'email affiché ou le site web, puis extrait l'email du site.

Usage:
    python extract_emails_doctolib.py
    python extract_emails_doctolib.py --batch-size 100 --concurrency 3
"""

import os
import re
import asyncio
import logging
import argparse
import time
import json
from datetime import datetime
from urllib.parse import urljoin, urlparse

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
log = logging.getLogger("extract_emails")

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

# Regex email
EMAIL_REGEX = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)

# Domaines email à ignorer
IGNORED_EMAIL_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "googleapis.com",
    "w3.org", "schema.org", "facebook.com", "twitter.com",
    "instagram.com", "linkedin.com", "google.com", "youtube.com",
    "apple.com", "microsoft.com", "mozilla.org", "jquery.com",
    "cloudflare.com", "gravatar.com", "wordpress.org", "wordpress.com",
    "bootstrapcdn.com", "fontawesome.com", "gstatic.com",
}

# Patterns email spécifiques dans le HTML
EMAIL_PATTERNS = [
    r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})',
    r'"email"\s*:\s*"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})"',
    r'itemprop="email"[^>]*content="([^"]+)"',
    r'class="[^"]*email[^"]*"[^>]*href="mailto:([^"]+)"',
]

# Patterns URL de site web dans le HTML Doctolib
WEBSITE_PATTERNS = [
    r'href="(https?://(?!(?:www\.)?doctolib\.fr)[^"]*)"[^>]*>\s*(?:Site\s*web|Website|Visiter)',
    r'"url"\s*:\s*"(https?://(?!(?:www\.)?doctolib\.fr)[^"]*)"',
    r'class="[^"]*website[^"]*"[^>]*href="(https?://[^"]*)"',
]


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


def is_valid_email(email: str) -> bool:
    """Vérifie si un email est valide et utile."""
    email = email.lower().strip()
    if not email or len(email) < 5:
        return False
    domain = email.split("@")[-1]
    if domain in IGNORED_EMAIL_DOMAINS:
        return False
    if any(x in email for x in ["noreply", "no-reply", "unsubscribe", "tracking", "pixel", "analytics"]):
        return False
    tld = domain.split(".")[-1]
    if tld not in ("fr", "com", "net", "org", "eu", "io", "info", "biz", "pro", "dental", "dentiste"):
        return False
    return True


def extract_emails_from_html(html: str) -> list[str]:
    """Extrait les emails depuis le HTML."""
    emails_found = []

    for pattern in EMAIL_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for m in matches:
            if is_valid_email(m):
                emails_found.append(m.lower().strip())

    json_ld_matches = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
    for json_str in json_ld_matches:
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                email = data.get("email") or ""
                if email and is_valid_email(email):
                    emails_found.insert(0, email.lower().strip())
        except (json.JSONDecodeError, ValueError):
            pass

    if not emails_found:
        all_emails = EMAIL_REGEX.findall(html)
        for e in all_emails:
            if is_valid_email(e):
                emails_found.append(e.lower().strip())

    seen = set()
    unique = []
    for e in emails_found:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    return unique


def extract_website_from_html(html: str) -> str | None:
    """Extrait l'URL du site web depuis le HTML d'un profil Doctolib."""
    for pattern in WEBSITE_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for url in matches:
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                domain = parsed.netloc.lower()
                if not any(social in domain for social in [
                    "facebook", "twitter", "instagram", "linkedin",
                    "youtube", "tiktok", "pinterest", "doctolib"
                ]):
                    return url
    return None


async def fetch_email(client: httpx.AsyncClient, url: str, max_retries: int = 2) -> tuple[str, str]:
    """Récupère l'email depuis un profil Doctolib et éventuellement son site web."""
    if not url or not url.strip():
        return "", ""

    for attempt in range(max_retries + 1):
        try:
            resp = await client.get(url.strip(), headers=HTTP_HEADERS, follow_redirects=True, timeout=15)

            if resp.status_code == 200:
                emails = extract_emails_from_html(resp.text)
                if emails:
                    return emails[0], "doctolib"

                website_url = extract_website_from_html(resp.text)
                if website_url:
                    try:
                        site_resp = await client.get(
                            website_url, headers=HTTP_HEADERS,
                            follow_redirects=True, timeout=10
                        )
                        if site_resp.status_code == 200:
                            site_emails = extract_emails_from_html(site_resp.text)
                            if site_emails:
                                return site_emails[0], "website"
                    except Exception:
                        pass

                return "", ""

            elif resp.status_code == 429:
                if attempt < max_retries:
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                return "Erreur 429", ""
            elif resp.status_code == 403:
                if attempt < max_retries:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                return "Erreur 403", ""
            else:
                return "", ""

        except httpx.TimeoutException:
            if attempt < max_retries:
                await asyncio.sleep(2)
                continue
            return "Timeout", ""
        except Exception:
            return "", ""

    return "", ""


async def extract_batch(urls: list[tuple[int, str]], concurrency: int = 3) -> list[tuple[int, str, str]]:
    """Extrait les emails d'un batch d'URLs."""
    semaphore = asyncio.Semaphore(concurrency)
    results = []

    proxy_url = get_proxy_url()
    log.info(f"  Proxy: {'Bright Data' if proxy_url else 'AUCUN (direct)'}")

    async with httpx.AsyncClient(timeout=20.0, proxy=proxy_url) as client:
        async def fetch_one(row_idx: int, url: str):
            async with semaphore:
                email, source = await fetch_email(client, url)
                results.append((row_idx, email, source))
                await asyncio.sleep(1)

        tasks = [fetch_one(idx, url) for idx, url in urls]
        await asyncio.gather(*tasks)

    return results


async def run(batch_size: int = 100, start_row: int = 2, concurrency: int = 3,
              skip_filled: bool = True):
    """Extrait les emails des profils Doctolib et met à jour le Google Sheet."""

    log.info("═══════════════════════════════════════════")
    log.info("  EXTRACTION EMAILS DOCTOLIB")
    log.info(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"  Batch: {batch_size} | Concurrency: {concurrency}")
    log.info("═══════════════════════════════════════════")

    ws = get_worksheet()
    headers = ws.row_values(1)
    log.info(f"Headers: {headers}")

    url_col = headers.index("Doctolib_URL") + 1 if "Doctolib_URL" in headers else None
    est_col = headers.index("Est_Sur_Doctolib") + 1 if "Est_Sur_Doctolib" in headers else None
    email_col = headers.index("Email") + 1 if "Email" in headers else None

    if not url_col or not est_col or not email_col:
        log.error("Colonnes manquantes ! (Doctolib_URL, Est_Sur_Doctolib, Email)")
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
            email_val = row[email_col - 1] if len(row) >= email_col else ""
            if email_val.strip() and not email_val.strip().startswith("Erreur"):
                continue

        work.append((i, url.strip()))

    log.info(f"  {len(work)} profils Doctolib à scraper (skip_filled={skip_filled})")

    if not work:
        log.info("Rien à faire !")
        return

    total_checked = 0
    total_found = 0
    total_from_doctolib = 0
    total_from_website = 0
    total_empty = 0
    total_errors = 0
    t_start = time.time()

    for batch_start in range(0, len(work), batch_size):
        batch = work[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(work) + batch_size - 1) // batch_size

        log.info(f"Batch {batch_num}/{total_batches} ({len(batch)} profils)...")

        results = await extract_batch(batch, concurrency=concurrency)

        cells_email = []
        for row_idx, email, source in sorted(results, key=lambda x: x[0]):
            if email and not email.startswith("Erreur") and email != "Timeout":
                cells_email.append(gspread.Cell(row=row_idx, col=email_col, value=email))
                total_found += 1
                if source == "doctolib":
                    total_from_doctolib += 1
                elif source == "website":
                    total_from_website += 1
            elif email.startswith("Erreur") or email == "Timeout":
                total_errors += 1
            else:
                total_empty += 1

        if cells_email:
            for attempt in range(5):
                try:
                    ws.update_cells(cells_email)
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
                log.error(f"  Failed to write emails after 5 attempts, skipping batch {batch_num}")

        total_checked += len(batch)
        elapsed = time.time() - t_start
        rate = total_checked / elapsed if elapsed > 0 else 0
        eta_seconds = (len(work) - total_checked) / rate if rate > 0 else 0
        eta_min = eta_seconds / 60

        log.info(
            f"  {total_found} trouvés (doctolib:{total_from_doctolib}, website:{total_from_website}) | "
            f"{total_empty} vides | {total_errors} erreurs"
        )
        log.info(
            f"  Progression: {total_checked}/{len(work)} "
            f"({total_checked/len(work)*100:.1f}%) | "
            f"Vitesse: {rate:.1f}/s | ETA: {eta_min:.0f} min"
        )

        await asyncio.sleep(1)

    total_time = time.time() - t_start
    log.info(f"RÉSUMÉ EXTRACTION EMAILS")
    log.info(f"  Total scrapé : {total_checked}")
    log.info(f"  Emails trouvés : {total_found}")
    log.info(f"    - Depuis Doctolib : {total_from_doctolib}")
    log.info(f"    - Depuis site web : {total_from_website}")
    log.info(f"  Pas d'email : {total_empty}")
    log.info(f"  Erreurs : {total_errors}")
    log.info(f"  Durée : {total_time/60:.1f} min")


def main():
    parser = argparse.ArgumentParser(description="Extrait les emails des profils Doctolib")
    parser.add_argument("--batch-size", type=int, default=100, help="Taille des batchs (défaut: 100)")
    parser.add_argument("--start-row", type=int, default=2, help="Ligne de départ (défaut: 2)")
    parser.add_argument("--concurrency", type=int, default=3, help="Requêtes parallèles (défaut: 3)")
    parser.add_argument("--force", action="store_true", help="Re-scraper même si email déjà rempli")

    args = parser.parse_args()

    asyncio.run(run(
        batch_size=args.batch_size,
        start_row=args.start_row,
        concurrency=args.concurrency,
        skip_filled=not args.force,
    ))


if __name__ == "__main__":
    main()
