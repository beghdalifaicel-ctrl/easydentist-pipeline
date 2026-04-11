"""
Extract cabinet/centre names from Doctolib profiles.
For each structure in 'Centres Doctolib' and 'Cabinets Doctolib' tabs,
visits the first practitioner's Doctolib URL and extracts the practice name.
Writes to a new column 'Nom_Etablissement_Doctolib'.
"""

import asyncio
import gspread
import httpx
import json
import os
import re
import time
import logging
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
BRIGHT_USER = os.getenv("BRIGHT_DATA_USERNAME")
BRIGHT_PASS = os.getenv("BRIGHT_DATA_PASSWORD")
PROXY_URL = f"http://{BRIGHT_USER}:{BRIGHT_PASS}@brd.superproxy.io:33335"

TABS = ["Centres Doctolib", "Cabinets Doctolib"]
COL_NAME = "Nom_Etablissement_Doctolib"


def get_gspread_client():
    creds = Credentials(
        token=None,
        refresh_token=os.getenv("GOOGLE_REFRESH_TOKEN"),
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        token_uri="https://oauth2.googleapis.com/token",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    creds.refresh(Request())
    return gspread.authorize(creds)


def extract_name_from_html(html: str, url: str) -> str:
    """Extract the practice/cabinet name from a Doctolib profile page."""

    # Method 1: JSON-LD data (most reliable)
    ld_matches = re.findall(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    for m in ld_matches:
        try:
            data = json.loads(m)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        # Look for MedicalOrganization or similar
                        if item.get("@type") in ("MedicalBusiness", "MedicalOrganization", "Dentist", "MedicalClinic"):
                            name = item.get("name", "")
                            if name:
                                return name.strip()
                        # Look for worksFor
                        works_for = item.get("worksFor")
                        if works_for:
                            if isinstance(works_for, dict) and works_for.get("name"):
                                return works_for["name"].strip()
                            if isinstance(works_for, list):
                                for w in works_for:
                                    if isinstance(w, dict) and w.get("name"):
                                        return w["name"].strip()
            elif isinstance(data, dict):
                if data.get("@type") in ("MedicalBusiness", "MedicalOrganization", "Dentist", "MedicalClinic"):
                    name = data.get("name", "")
                    if name:
                        return name.strip()
                works_for = data.get("worksFor")
                if works_for:
                    if isinstance(works_for, dict) and works_for.get("name"):
                        return works_for["name"].strip()
        except (json.JSONDecodeError, TypeError):
            continue

    # Method 2: Look for practice name in specific JSON patterns
    patterns = [
        r'"practiceName"\s*:\s*"([^"]+)"',
        r'"practice_name"\s*:\s*"([^"]+)"',
        r'"organization_name"\s*:\s*"([^"]+)"',
        r'"name"\s*:\s*"((?:Cabinet|Centre|Clinique|CDS|SELARL|SELAS|SCP|SCM)[^"]*)"',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.I)
        if m:
            return m.group(1).strip()

    # Method 3: og:title or title tag
    og_title = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', html)
    if og_title:
        title = og_title.group(1).strip()
        # Extract practice name from title like "Dr X - Cabinet Y - Doctolib"
        parts = title.split(" - ")
        if len(parts) >= 2:
            # Usually format is "Dr Name - Practice Name - City - Doctolib"
            for part in parts[1:]:
                part = part.strip()
                if part.lower() not in ("doctolib", "prenez rendez-vous en ligne") and not re.match(r'^[A-Z][a-zéèêë]+$', part):
                    return part

    # Method 4: Look for the practice name in the address/header section
    practice_h2 = re.search(
        r'<h2[^>]*>([^<]*(?:Cabinet|Centre|Clinique|CDS|Maison)[^<]*)</h2>',
        html, re.I
    )
    if practice_h2:
        return practice_h2.group(1).strip()

    # Method 5: data attribute
    data_attr = re.search(r'data-practice-name="([^"]+)"', html)
    if data_attr:
        return data_attr.group(1).strip()

    return ""


async def fetch_name(client: httpx.AsyncClient, url: str, semaphore: asyncio.Semaphore) -> str:
    """Fetch a Doctolib page and extract the practice name."""
    async with semaphore:
        try:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept-Language": "fr-FR,fr;q=0.9",
                    "Accept": "text/html,application/xhtml+xml",
                },
                follow_redirects=True,
                timeout=15,
            )
            if resp.status_code == 200:
                return extract_name_from_html(resp.text, url)
            else:
                log.warning(f"HTTP {resp.status_code} for {url}")
                return ""
        except Exception as e:
            log.warning(f"Error fetching {url}: {e}")
            return ""
        finally:
            await asyncio.sleep(0.5)


def batch_update_with_retry(ws, updates, max_retries=5):
    """Write batch updates with exponential backoff for rate limits."""
    for attempt in range(max_retries):
        try:
            ws.batch_update(updates, value_input_option="RAW")
            return True
        except gspread.exceptions.APIError as e:
            if "429" in str(e) or "500" in str(e) or "503" in str(e):
                wait = min(2 ** attempt * 5, 120)
                log.warning(f"GSheets API error (attempt {attempt+1}), waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
    return False


async def run(batch_size=100, concurrency=3, skip_filled=True):
    """Main extraction loop."""
    client_gs = get_gspread_client()
    sheet = client_gs.open_by_key(SHEET_ID)

    for tab_name in TABS:
        log.info(f"=== Processing {tab_name} ===")
        ws = sheet.worksheet(tab_name)
        data = ws.get_all_values()
        headers = data[0]

        url_idx = headers.index("Doctolib_URLs")

        # Add column if not exists
        if COL_NAME not in headers:
            new_col = len(headers) + 1
            ws.update_cell(1, new_col, COL_NAME)
            headers.append(COL_NAME)
            log.info(f"  Added column {COL_NAME} at position {new_col}")

        name_idx = headers.index(COL_NAME)
        name_col = name_idx + 1  # 1-based

        # Build list of rows to process
        rows_to_process = []
        for row_num, row in enumerate(data[1:], start=2):
            # Get existing value
            existing = row[name_idx] if len(row) > name_idx else ""
            if skip_filled and existing.strip():
                continue

            # Get first URL from the pipe-separated list
            urls_str = row[url_idx] if len(row) > url_idx else ""
            if not urls_str.strip():
                continue

            first_url = urls_str.split("|")[0].strip()
            if not first_url.startswith("http"):
                continue

            rows_to_process.append((row_num, first_url))

        total = len(rows_to_process)
        log.info(f"  {total} structures to process (skipping already filled)")

        if total == 0:
            log.info(f"  Nothing to do for {tab_name}")
            continue

        # Process in batches
        semaphore = asyncio.Semaphore(concurrency)

        async with httpx.AsyncClient(proxy=PROXY_URL) as http_client:
            for batch_start in range(0, total, batch_size):
                batch = rows_to_process[batch_start:batch_start + batch_size]

                # Fetch all names in parallel
                tasks = [
                    fetch_name(http_client, url, semaphore)
                    for _, url in batch
                ]
                names = await asyncio.gather(*tasks)

                # Prepare batch update
                updates = []
                found = 0
                for (row_num, url), name in zip(batch, names):
                    if name:
                        found += 1
                    cell = gspread.utils.rowcol_to_a1(row_num, name_col)
                    updates.append({
                        "range": cell,
                        "values": [[name if name else "Non trouvé"]]
                    })

                # Write batch
                if updates:
                    batch_update_with_retry(ws, updates)

                processed = min(batch_start + batch_size, total)
                log.info(f"  [{tab_name}] {processed}/{total} ({processed/total*100:.1f}%) — batch found {found}/{len(batch)} names")

                time.sleep(1)  # pause between batches for GSheets API

        log.info(f"  ✅ Done: {tab_name}")

    log.info("🎉 All tabs done!")
    return {"status": "success", "detail": "Cabinet names extracted"}


if __name__ == "__main__":
    asyncio.run(run())
