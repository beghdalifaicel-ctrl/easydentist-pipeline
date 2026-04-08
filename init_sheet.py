"""
init_sheet.py — Initialise le Google Sheet avec les 52 000 dentistes RPPS.
Crée l'onglet "Dentistes" avec les colonnes nécessaires et charge les données.
"""

import os
import csv
import logging
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
RPPS_CSV_FILE = os.getenv("RPPS_CSV_FILE", "dentistes_rpps.csv")

# Colonnes du Google Sheet
HEADERS = [
    "RPPS",
    "Nom",
    "Prenom",
    "Adresse",
    "Code_Postal",
    "Ville",
    "Telephone",
    "Email",
    "Doctolib_URL",
    "Dispo_Doctolib",
    "Est_Sur_Doctolib",
    "Pas_Sur_Doctolib",
    "Derniere_Verif",
    "Sellsy_ID",
]


def get_google_sheet():
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


def init_worksheet(sheet):
    """Crée ou récupère l'onglet Dentistes."""
    try:
        worksheet = sheet.worksheet("Dentistes")
        logger.info("Onglet 'Dentistes' existant trouvé")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title="Dentistes", rows=55000, cols=len(HEADERS))
        logger.info("Onglet 'Dentistes' créé")

    # Écrire les headers
    worksheet.update("A1", [HEADERS])
    logger.info(f"Headers écrits: {HEADERS}")

    return worksheet


def load_rpps_data(csv_file):
    """Charge les données RPPS depuis un fichier CSV."""
    dentists = []

    if not os.path.exists(csv_file):
        logger.error(f"Fichier CSV non trouvé: {csv_file}")
        logger.info("Format attendu: RPPS,Nom,Prenom,Adresse,Code_Postal,Ville,Telephone,Email,Doctolib_URL")
        return dentists

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            dentists.append([
                row.get("RPPS", ""),
                row.get("Nom", ""),
                row.get("Prenom", ""),
                row.get("Adresse", ""),
                row.get("Code_Postal", ""),
                row.get("Ville", ""),
                row.get("Telephone", ""),
                row.get("Email", ""),
                row.get("Doctolib_URL", ""),
                "",  # Dispo_Doctolib — vide au départ
                "",  # Est_Sur_Doctolib — vide au départ
                "",  # Pas_Sur_Doctolib — vide au départ
                "",  # Derniere_Verif — vide au départ
                "",  # Sellsy_ID — vide au départ
            ])

    logger.info(f"{len(dentists)} dentistes chargés depuis {csv_file}")
    return dentists


def upload_to_sheet(worksheet, data, batch_size=1000):
    """Upload les données par batch dans le Google Sheet."""
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i : i + batch_size]
        start_row = i + 2  # +2 car row 1 = headers
        cell_range = f"A{start_row}"
        worksheet.update(cell_range, batch)
        logger.info(f"Batch {i // batch_size + 1}: lignes {start_row} à {start_row + len(batch) - 1} uploadées")

    logger.info(f"Upload terminé: {total} lignes")


def main():
    logger.info("=== Initialisation du Google Sheet ===")

    if not GOOGLE_SHEET_ID:
        logger.error("GOOGLE_SHEET_ID non configuré dans .env")
        return

    # Connexion
    sheet = get_google_sheet()
    logger.info(f"Connecté au Google Sheet: {GOOGLE_SHEET_ID}")

    # Init onglet
    worksheet = init_worksheet(sheet)

    # Charger les données RPPS
    dentists = load_rpps_data(RPPS_CSV_FILE)

    if not dentists:
        logger.warning("Aucune donnée à charger. Vérifiez le fichier CSV.")
        logger.info(f"Fichier attendu: {RPPS_CSV_FILE}")
        return

    # Upload
    upload_to_sheet(worksheet, dentists)

    logger.info("=== Initialisation terminée ===")
    logger.info(f"Google Sheet prêt avec {len(dentists)} dentistes")


if __name__ == "__main__":
    main()
