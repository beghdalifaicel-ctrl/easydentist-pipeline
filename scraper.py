"""
Scraper Doctolib — Vérifie les disponibilités de créneaux dentaires.
Utilise Bright Data Datacenter Proxies (Shared) via requests.
"""

import os
import re
import time
import random
import logging
import requests
import json
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────
BRIGHT_DATA_HOST = os.getenv("BRIGHT_DATA_HOST", "brd.superproxy.io")
BRIGHT_DATA_PORT = os.getenv("BRIGHT_DATA_PORT", "33335")
BRIGHT_DATA_USER = os.getenv("BRIGHT_DATA_USER", "")
BRIGHT_DATA_PASS = os.getenv("BRIGHT_DATA_PASS", "")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

SELLSY_API_URL = os.getenv("SELLSY_API_URL", "https://api.sellsy.com/v2")
SELLSY_API_KEY = os.getenv("SELLSY_API_KEY", "")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
DELAY_MIN = float(os.getenv("DELAY_MIN", "1.0"))
DELAY_MAX = float(os.getenv("DELAY_MAX", "3.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


# ─── Proxy setup ──────────────────────────────────────────────────────────────
def get_proxy_url():
    """Construit l'URL proxy Bright Data datacenter."""
    return f"http://{BRIGHT_DATA_USER}:{BRIGHT_DATA_PASS}@{BRIGHT_DATA_HOST}:{BRIGHT_DATA_PORT}"


def get_session():
    """Crée une session requests avec proxy Bright Data."""
    session = requests.Session()
    proxy_url = get_proxy_url()
    session.proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "fr-FR,fr;q=0.9",
    })
    return session


# ─── Google Sheets ─────────────────────────────────────────────────────────────
def get_google_sheet():
    """Connexion au Google Sheet via OAuth2 (refresh token)."""
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
    return client.open_by_key(GOOGLE_SHEET_ID)


def get_dentists_to_check(sheet):
    """Récupère les dentistes à vérifier depuis l'onglet principal."""
    worksheet = sheet.worksheet("Dentistes")
    records = worksheet.get_all_records()
    logger.info(f"Nombre total de dentistes dans le sheet: {len(records)}")
    return records


def update_sheet_row(worksheet, row_index, dispo, last_checked):
    """Met à jour une ligne du sheet avec le résultat de la vérif."""
    # Colonnes: ... | Doctolib_URL | Dispo_Doctolib | Derniere_Verif
    col_dispo = worksheet.find("Dispo_Doctolib").col
    col_verif = worksheet.find("Derniere_Verif").col
    worksheet.update_cell(row_index, col_dispo, dispo)
    worksheet.update_cell(row_index, col_verif, last_checked)


# ─── Doctolib scraping ────────────────────────────────────────────────────────
def check_doctolib_availability(session, doctolib_url):
    """
    Vérifie si un praticien a des créneaux disponibles sur Doctolib.

    Stratégie :
    1. Récupère la page du praticien
    2. Extrait le profile_id depuis le HTML
    3. Appelle l'API availabilities de Doctolib
    4. Retourne "Oui" / "Non" / "Erreur" / "Pas sur Doctolib"
    """
    if not doctolib_url or doctolib_url.strip() == "":
        return "Pas sur Doctolib"

    try:
        # Étape 1 : Charger la page du praticien
        resp = session.get(doctolib_url, timeout=30)
        if resp.status_code == 404:
            return "Pas sur Doctolib"
        resp.raise_for_status()

        html = resp.text

        # Étape 2 : Extraire le profile_id ou le slug du praticien
        # Doctolib utilise des données JSON embarquées dans la page
        profile_match = re.search(r'"profile":\{"id":(\d+)', html)
        if not profile_match:
            # Essayer une autre pattern
            profile_match = re.search(r'data-profile-id="(\d+)"', html)

        if not profile_match:
            logger.warning(f"Impossible d'extraire le profile_id pour {doctolib_url}")
            return "Erreur"

        profile_id = profile_match.group(1)

        # Étape 3 : Extraire le practice_id et visit_motive_ids
        practice_match = re.search(r'"practice_ids":\[(\d+)', html)
        practice_id = practice_match.group(1) if practice_match else None

        motive_match = re.search(r'"visit_motive_ids":\[([^\]]+)\]', html)
        motive_ids = motive_match.group(1) if motive_match else None

        if not practice_id or not motive_ids:
            # Fallback : chercher des créneaux via un pattern plus simple
            has_slots = re.search(
                r'(Prochain rendez-vous|Prochaine disponibilité|availabilities)',
                html, re.IGNORECASE
            )
            if has_slots:
                return "Oui"
            return "Non"

        # Étape 4 : Appeler l'API availabilities
        today = datetime.now().strftime("%Y-%m-%d")
        next_week = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

        avail_url = (
            f"https://www.doctolib.fr/availabilities.json"
            f"?start_date={today}"
            f"&visit_motive_ids={motive_ids}"
            f"&practice_ids={practice_id}"
            f"&limit=7"
        )

        avail_resp = session.get(avail_url, timeout=30)
        avail_resp.raise_for_status()
        avail_data = avail_resp.json()

        # Vérifier s'il y a des disponibilités
        availabilities = avail_data.get("availabilities", [])
        has_slots = any(
            len(day.get("slots", [])) > 0
            for day in availabilities
        )

        return "Oui" if has_slots else "Non"

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error pour {doctolib_url}: {e}")
        return "Erreur"
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection Error pour {doctolib_url}: {e}")
        return "Erreur"
    except Exception as e:
        logger.error(f"Erreur inattendue pour {doctolib_url}: {e}")
        return "Erreur"


# ─── Sellsy update ────────────────────────────────────────────────────────────
def update_sellsy_prospect(rpps, dispo):
    """Met à jour la fiche Sellsy avec la disponibilité Doctolib."""
    if not SELLSY_API_KEY:
        logger.warning("SELLSY_API_KEY non configurée, skip update Sellsy")
        return

    headers = {
        "Authorization": f"Bearer {SELLSY_API_KEY}",
        "Content-Type": "application/json",
    }

    # Rechercher le prospect par RPPS
    search_resp = requests.get(
        f"{SELLSY_API_URL}/companies",
        headers=headers,
        params={"search": rpps},
        timeout=15,
    )

    if search_resp.status_code != 200:
        logger.error(f"Erreur recherche Sellsy pour RPPS {rpps}: {search_resp.status_code}")
        return

    companies = search_resp.json().get("data", [])
    if not companies:
        return

    company_id = companies[0]["id"]

    # Mettre à jour le champ custom "dispo_doctolib"
    update_resp = requests.put(
        f"{SELLSY_API_URL}/companies/{company_id}",
        headers=headers,
        json={
            "custom_fields": [
                {"code": "dispo_doctolib", "value": dispo},
                {"code": "derniere_verif_doctolib", "value": datetime.now().isoformat()},
            ]
        },
        timeout=15,
    )

    if update_resp.status_code == 200:
        logger.info(f"Sellsy mis à jour pour {rpps}: dispo={dispo}")
    else:
        logger.error(f"Erreur update Sellsy {rpps}: {update_resp.status_code}")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run_scraper():
    """Lance le scraping complet des disponibilités Doctolib."""
    logger.info("=== Démarrage du scraper Doctolib ===")

    # Connexion Google Sheets
    sheet = get_google_sheet()
    worksheet = sheet.worksheet("Dentistes")
    dentists = get_dentists_to_check(sheet)
    logger.info(f"{len(dentists)} dentistes à vérifier")

    # Session avec proxy Bright Data
    session = get_session()
    logger.info(f"Proxy configuré: {BRIGHT_DATA_HOST}:{BRIGHT_DATA_PORT}")

    # Test de connectivité proxy
    try:
        test_resp = session.get("https://geo.brdtest.com/welcome.txt", timeout=15)
        logger.info(f"Test proxy OK: {test_resp.text.strip()[:100]}")
    except Exception as e:
        logger.error(f"Échec test proxy: {e}")
        return

    # Scraping par batch
    total = len(dentists)
    checked = 0
    errors = 0
    disponibles = 0

    for i, dentist in enumerate(dentists):
        rpps = dentist.get("RPPS", "")
        doctolib_url = dentist.get("Doctolib_URL", "")
        nom = dentist.get("Nom", "Inconnu")

        # Vérification avec retry
        dispo = "Erreur"
        for attempt in range(MAX_RETRIES):
            dispo = check_doctolib_availability(session, doctolib_url)
            if dispo != "Erreur":
                break
            logger.warning(f"Retry {attempt + 1}/{MAX_RETRIES} pour {nom}")
            time.sleep(2)

        # Stats
        checked += 1
        if dispo == "Erreur":
            errors += 1
        elif dispo == "Oui":
            disponibles += 1

        # Update Google Sheet (row_index = i + 2 car header en row 1)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        try:
            update_sheet_row(worksheet, i + 2, dispo, now_str)
        except Exception as e:
            logger.error(f"Erreur update sheet pour {nom}: {e}")

        # Update Sellsy
        if rpps:
            try:
                update_sellsy_prospect(rpps, dispo)
            except Exception as e:
                logger.error(f"Erreur update Sellsy pour {nom}: {e}")

        # Log de progression
        if checked % 100 == 0:
            logger.info(
                f"Progression: {checked}/{total} "
                f"({disponibles} dispo, {errors} erreurs)"
            )

        # Délai aléatoire entre chaque requête
        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        time.sleep(delay)

    # Résumé final
    logger.info("=== Scraping terminé ===")
    logger.info(f"Total vérifié: {checked}/{total}")
    logger.info(f"Disponibles: {disponibles}")
    logger.info(f"Erreurs: {errors}")

    return {
        "total": total,
        "checked": checked,
        "disponibles": disponibles,
        "errors": errors,
    }


if __name__ == "__main__":
    run_scraper()
