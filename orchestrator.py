#!/usr/bin/env python3
"""
ORCHESTRATEUR EASYDENTIST — Doctolib → Sellsy → Ringover → Pipeline
====================================================================
Scrape quotidien des disponibilités Doctolib par ville,
qualification via Sellsy + Ringover, injection dans le pipeline.

Usage:
    python orchestrator.py --city "Paris" --max-pages 3
    python orchestrator.py --city "Marseille" --dry-run
    python orchestrator.py --cities-file villes.txt --max-pages 5
"""

import asyncio
import json
import os
import re
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────

# Bright Data Browser API
SBR_WS = os.getenv(
    "BRIGHT_DATA_BROWSER_WS",
    "wss://brd-customer-hl_dbe515e1-zone-doctolib_browser:ub9zsp721noa@brd.superproxy.io:9222"
)

# Sellsy API v2 (OAuth2)
SELLSY_API_URL = os.getenv("SELLSY_API_URL", "https://api.sellsy.com/v2")
SELLSY_CLIENT_ID = os.getenv("SELLSY_CLIENT_ID", "")
SELLSY_CLIENT_SECRET = os.getenv("SELLSY_CLIENT_SECRET", "")

# Ringover API
RINGOVER_API_URL = os.getenv("RINGOVER_API_URL", "https://public-api.ringover.com/v2")
RINGOVER_API_KEY = os.getenv("RINGOVER_API_KEY", "")

# Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# Pipeline config
PIPELINE_ID = int(os.getenv("PIPELINE_ID", "100281"))  # Dispo docto
STEP_INTERESSES_ID = int(os.getenv("STEP_INTERESSES_ID", "774686"))  # À appeler aujourd'hui
STAFF_OWNER_ID = int(os.getenv("STAFF_OWNER_ID", "649062"))

# Seuils
INACTIVITY_DAYS = 60
MAX_DENTISTS_PER_RUN = 200

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("orchestrator")


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 : SCRAPING DOCTOLIB (Bright Data Browser API)
# ═══════════════════════════════════════════════════════════════════════════════

JS_EXTRACT_DENTISTS = """
() => {
    const results = [];
    const cards = document.querySelectorAll('.dl-card-content');
    cards.forEach(card => {
        const text = (card.innerText || '').trim();
        if (!text.includes('Dr') && !text.includes('Cabinet') && !text.includes('Centre')) return;

        const link = card.querySelector('a[href*="/dentiste/"]') || card.querySelector('a[href*="/chirurgien-dentiste/"]');
        if (!link) return;

        const href = link.getAttribute('href').split('?')[0];
        const lines = text.split('\\n').filter(l => l.trim());
        const name = lines[0] || '';

        // Adresse (chercher le pattern code postal)
        let address = '';
        let city = '';
        for (const line of lines) {
            const match = line.match(/(\\d{5})\\s+(.+)/);
            if (match) {
                address = line.trim();
                city = match[2].trim();
                break;
            }
        }

        // Téléphone
        let phone = '';
        for (const line of lines) {
            const phoneMatch = line.match(/(?:0[1-9](?:[\\s.]?\\d{2}){4})/);
            if (phoneMatch) {
                phone = phoneMatch[0].replace(/[\\s.]/g, '');
                break;
            }
        }

        // Disponibilités
        let prochainRdv = null;
        for (const line of lines) {
            if (line.includes('Prochain RDV') || line.includes('Prochain rdv')) {
                prochainRdv = line.trim();
            }
        }

        const timeSlots = text.match(/\\d{1,2}:\\d{2}/g) || [];
        const todayTomorrow = text.includes("Aujourd") || text.includes("Demain");
        const hasSlotsThisWeek = timeSlots.length > 0 || todayTomorrow;

        results.push({
            name,
            href,
            address,
            city,
            phone,
            prochainRdv,
            timeSlotCount: timeSlots.length,
            hasSlotsThisWeek,
            rawText: text.substring(0, 500)
        });
    });

    // Dédupliquer par href
    const seen = new Set();
    return results.filter(r => {
        if (seen.has(r.href)) return false;
        seen.add(r.href);
        return true;
    });
}
"""

JS_CHECK_IS_ON_DOCTOLIB = """
() => {
    const body = document.body.innerText || '';
    const title = document.title || '';

    // Signaux négatifs (pas inscrit)
    const notOnDoctolib = body.includes("n'est pas sur Doctolib")
        || body.includes("n\u2019est pas sur Doctolib")
        || body.includes("pas r\\u00e9servable en ligne")
        || body.includes("pas réservable en ligne")
        || body.includes("Revendiquer mon profil");

    // Signaux positifs (inscrit)
    const isOnDoctolib = body.includes("Prendre rendez-vous")
        || body.includes("Prenez RDV")
        || body.includes("Prochain RDV")
        || body.includes("Prochaine disponibilit")
        || body.includes("Conventionn")
        || body.includes("Carte Vitale")
        || body.includes("Tarifs et remboursement")
        || title.includes("Prenez RDV")
        || !!document.querySelector('[class*="booking"], [class*="availabilities"], [class*="appointment"]');

    return {
        isOnDoctolib: isOnDoctolib && !notOnDoctolib,
        notOnDoctolib: notOnDoctolib,
        title: title,
        bodySnippet: body.substring(0, 300)
    };
}
"""

JS_CHECK_HAS_NEXT_PAGE = """
() => {
    // Stratégie multi: chercher tout type de pagination Doctolib
    const body = document.body.innerText || '';

    // 1. Chercher les boutons/liens de pagination
    const nextBtn = document.querySelector('[class*="pagination"] [class*="next"]:not([class*="disabled"])');
    const nextLink = document.querySelector('a[aria-label*="suivante"], a[aria-label*="next"]');
    const nextArrow = document.querySelector('[class*="pagination"] svg, [class*="pagination"] [class*="arrow"]');

    // 2. Chercher les numéros de page dans la pagination
    const allLinks = document.querySelectorAll('a[href*="page="]');
    let maxPage = 0;
    allLinks.forEach(a => {
        const match = a.href.match(/page=(\\d+)/);
        if (match) maxPage = Math.max(maxPage, parseInt(match[1]));
    });

    // 3. Chercher du texte indicatif
    const hasMoreText = body.includes('Suivant') || body.includes('Page ');

    const totalCards = document.querySelectorAll('.dl-card-content').length;

    return {
        hasNext: !!(nextBtn || nextLink || maxPage > 1 || hasMoreText),
        maxPage,
        totalCards
    };
}
"""


async def scrape_doctolib_city(city: str, max_pages: int = 5) -> list[dict]:
    """Scrape les dentistes avec dispo sur Doctolib pour une ville donnée."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("playwright non installé. Installer: pip install playwright && playwright install chromium")
        return []

    all_dentists = []
    url_base = f"https://www.doctolib.fr/dentiste/{city.lower().replace(' ', '-')}"

    log.info(f"🔍 Scraping Doctolib: {city} (max {max_pages} pages)")

    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(SBR_WS)

        for page_num in range(1, max_pages + 1):
            url = url_base if page_num == 1 else f"{url_base}?page={page_num}"
            log.info(f"  📄 Page {page_num}: {url}")

            page = await browser.new_page()
            try:
                await page.goto(url, timeout=120_000, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                except Exception:
                    pass
                # Attendre le rendu React
                await asyncio.sleep(6)

                # Scroll pour charger tout le contenu
                for _ in range(3):
                    await page.evaluate("window.scrollBy(0, window.innerHeight)")
                    await asyncio.sleep(1)
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(1)

                # Extraire les dentistes
                dentists = await page.evaluate(JS_EXTRACT_DENTISTS)
                log.info(f"    → {len(dentists)} dentistes trouvés")

                for d in dentists:
                    d["source_city"] = city
                    d["source_page"] = page_num
                all_dentists.extend(dentists)

                # Vérifier s'il y a une page suivante
                pagination = await page.evaluate(JS_CHECK_HAS_NEXT_PAGE)
                # Si on a trouvé >= 10 dentistes, il y a probablement une page suivante
                # Doctolib affiche ~17 résultats par page
                has_more = pagination.get("hasNext") or len(dentists) >= 10
                if not has_more:
                    log.info(f"    → Dernière page atteinte (page {page_num}, {len(dentists)} résultats)")
                    break

            except Exception as e:
                log.error(f"    ❌ Erreur page {page_num}: {e}")
            finally:
                await page.close()

        await browser.close()

    # Dédupliquer globalement par href
    seen = set()
    unique = []
    for d in all_dentists:
        if d["href"] not in seen:
            seen.add(d["href"])
            unique.append(d)

    # Filtrer: garder seulement ceux avec plus de 5 créneaux cette semaine
    MIN_SLOTS = int(os.getenv("MIN_SLOTS", "5"))
    with_slots = [d for d in unique if d.get("hasSlotsThisWeek") and d.get("timeSlotCount", 0) > MIN_SLOTS]
    log.info(f"📊 {city}: {len(unique)} dentistes uniques, {len(with_slots)} avec >5 créneaux cette semaine")

    return with_slots


async def check_doctolib_profiles(dentists: list[dict]) -> list[dict]:
    """Visite chaque profil Doctolib pour vérifier si le dentiste est réellement inscrit."""
    if not dentists:
        return dentists

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("Playwright non installé, skip check profils")
        for d in dentists:
            d["est_sur_doctolib"] = ""
            d["pas_sur_doctolib"] = ""
        return dentists

    log.info(f"🔍 Vérification profils Doctolib pour {len(dentists)} dentistes...")

    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(SBR_WS)
        page = await browser.new_page()

        for i, dentist in enumerate(dentists):
            href = dentist.get("href", "")
            url = f"https://www.doctolib.fr{href}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(5000)
                result = await page.evaluate(JS_CHECK_IS_ON_DOCTOLIB)

                if result.get("notOnDoctolib"):
                    dentist["est_sur_doctolib"] = "Non"
                    dentist["pas_sur_doctolib"] = "Oui"
                    log.info(f"  [{i+1}/{len(dentists)}] {dentist['name']} → ❌ N'est PAS sur Doctolib")
                elif result.get("isOnDoctolib"):
                    dentist["est_sur_doctolib"] = "Oui"
                    dentist["pas_sur_doctolib"] = "Non"
                    log.info(f"  [{i+1}/{len(dentists)}] {dentist['name']} → ✅ Est sur Doctolib")
                else:
                    dentist["est_sur_doctolib"] = "?"
                    dentist["pas_sur_doctolib"] = "?"
                    log.info(f"  [{i+1}/{len(dentists)}] {dentist['name']} → ❓ Indéterminé (titre: {result.get('title', '')[:60]})")
                    log.debug(f"    Body: {result.get('bodySnippet', '')[:200]}")

            except Exception as e:
                log.warning(f"  [{i+1}/{len(dentists)}] {dentist['name']} → Erreur: {e}")
                dentist["est_sur_doctolib"] = "Erreur"
                dentist["pas_sur_doctolib"] = "Erreur"

            await page.wait_for_timeout(500)

        await browser.close()

    return dentists


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 : VÉRIFICATION SELLSY
# ═══════════════════════════════════════════════════════════════════════════════

class SellsyClient:
    """Client API Sellsy v2 avec OAuth2 (client_credentials)."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = SELLSY_API_URL
        self.token: str | None = None
        self.token_expires: float = 0
        self.client = httpx.AsyncClient(timeout=30.0)

    async def _ensure_token(self):
        """Obtient ou renouvelle le Bearer token OAuth2."""
        if self.token and time.time() < self.token_expires - 60:
            return
        resp = await self.client.post(
            "https://login.sellsy.com/oauth2/access-tokens",
            json={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/json"}
        )
        if resp.status_code == 200:
            data = resp.json()
            self.token = data["access_token"]
            self.token_expires = time.time() + data.get("expires_in", 3600)
            log.info("🔑 Sellsy token obtenu")
        else:
            log.error(f"Sellsy OAuth2 error: {resp.status_code} {resp.text[:200]}")
            raise RuntimeError("Impossible d'obtenir le token Sellsy")

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def search_contacts_by_lastname(self, last_name: str, limit: int = 25) -> list[dict]:
        """Recherche contacts par nom de famille (filtre exact Sellsy v2)."""
        await self._ensure_token()
        try:
            resp = await self.client.post(
                f"{self.base_url}/contacts/search",
                json={"filters": {"last_name": last_name}, "limit": limit},
                headers=self.headers
            )
            if resp.status_code == 200:
                return resp.json().get("data", [])
            log.debug(f"search_contacts {resp.status_code}: {resp.text[:150]}")
        except Exception as e:
            log.debug(f"Sellsy search_contacts error: {e}")
        return []

    async def search_companies_by_name(self, name: str) -> list[dict]:
        """Recherche entreprises par nom."""
        await self._ensure_token()
        try:
            resp = await self.client.post(
                f"{self.base_url}/companies/search",
                json={"filters": {"name": name}},
                headers=self.headers
            )
            if resp.status_code == 200:
                return resp.json().get("data", [])
        except Exception as e:
            log.debug(f"Sellsy search_companies error: {e}")
        return []

    async def get_company(self, company_id: int) -> dict | None:
        """Récupère les détails d'une entreprise."""
        await self._ensure_token()
        try:
            resp = await self.client.get(
                f"{self.base_url}/companies/{company_id}",
                headers=self.headers
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            log.debug(f"Sellsy get_company error: {e}")
        return None

    async def create_company(self, name: str, phone: str = None,
                              note: str = None) -> dict | None:
        """Crée une nouvelle entreprise prospect dans Sellsy."""
        await self._ensure_token()
        try:
            body = {"name": name, "type": "prospect"}
            if note:
                body["note"] = note
            if phone:
                body["phone_number"] = phone
            resp = await self.client.post(
                f"{self.base_url}/companies",
                json=body,
                headers=self.headers
            )
            if resp.status_code in (200, 201):
                return resp.json()
            log.warning(f"create_company {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.error(f"Sellsy create_company error: {e}")
        return None

    async def get_contact_company(self, contact_id: int) -> dict | None:
        """Récupère la première company liée à un contact."""
        await self._ensure_token()
        try:
            resp = await self.client.get(
                f"{self.base_url}/contacts/{contact_id}/companies",
                headers=self.headers
            )
            if resp.status_code == 200:
                companies = resp.json().get("data", [])
                return companies[0] if companies else None
        except Exception as e:
            log.debug(f"Sellsy get_contact_company error: {e}")
        return None

    async def create_opportunity(self, name: str, company_id: int | None,
                                   pipeline_id: int, step_id: int,
                                   note: str = None,
                                   probability: int = 20) -> dict | None:
        """Crée une opportunité dans le pipeline Sellsy v2."""
        await self._ensure_token()
        try:
            body = {
                "name": name,
                "pipeline": pipeline_id,
                "step": step_id,
                "probability": probability,
                "estimated_closing_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
            }
            if company_id:
                body["related"] = [{"id": company_id, "type": "company"}]
            if note:
                body["note"] = note
            resp = await self.client.post(
                f"{self.base_url}/opportunities",
                json=body,
                headers=self.headers
            )
            if resp.status_code in (200, 201):
                return resp.json()
            log.warning(f"create_opportunity {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.error(f"Sellsy create_opportunity error: {e}")
        return None

    async def create_comment(self, related_type: str, related_id: int,
                               description: str) -> dict | None:
        """Ajoute un commentaire sur un objet Sellsy."""
        await self._ensure_token()
        try:
            body = {
                "related": [{"type": related_type, "id": related_id}],
                "description": description
            }
            resp = await self.client.post(
                f"{self.base_url}/comments",
                json=body,
                headers=self.headers
            )
            if resp.status_code in (200, 201):
                return resp.json()
        except Exception as e:
            log.debug(f"Sellsy create_comment error: {e}")
        return None

    async def close(self):
        await self.client.aclose()


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 : VÉRIFICATION RINGOVER
# ═══════════════════════════════════════════════════════════════════════════════

class RingoverClient:
    """Client API Ringover direct."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = RINGOVER_API_URL
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.client = httpx.AsyncClient(timeout=30.0)
        self._contacts_cache: dict[str, dict] | None = None

    async def load_contacts_cache(self):
        """Charge tous les contacts Ringover en cache (indexés par numéro de téléphone)."""
        if self._contacts_cache is not None:
            return

        self._contacts_cache = {}
        offset = 0
        limit = 100
        while True:
            try:
                resp = await self.client.get(
                    f"{self.base_url}/contacts",
                    params={"limit": limit, "offset": offset},
                    headers=self.headers
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                contacts = data.get("contact_list", [])
                if not contacts:
                    break
                for c in contacts:
                    for num in (c.get("numbers") or []):
                        phone = str(num.get("number", ""))
                        if phone:
                            self._contacts_cache[phone] = c
                            # Aussi indexer en format national
                            if phone.startswith("33"):
                                national = "0" + phone[2:]
                                self._contacts_cache[national] = c
                total = data.get("total_contact_count", 0)
                offset += limit
                if offset >= total or len(contacts) < limit:
                    break
            except Exception as e:
                log.debug(f"Ringover load_contacts error: {e}")
                break

        log.info(f"📞 Ringover: {len(self._contacts_cache)} numéros en cache")

    def find_contact_by_phone(self, phone: str) -> dict | None:
        """Cherche un contact par numéro de téléphone."""
        if not self._contacts_cache:
            return None
        # Normaliser le numéro
        phone_clean = re.sub(r"[\s.+\-]", "", phone)
        if phone_clean.startswith("0"):
            phone_clean_intl = "33" + phone_clean[1:]
        elif phone_clean.startswith("33"):
            phone_clean_intl = phone_clean
        else:
            phone_clean_intl = phone_clean

        return (
            self._contacts_cache.get(phone_clean)
            or self._contacts_cache.get(phone_clean_intl)
        )

    async def get_call_details(self, call_id: str) -> dict | None:
        """Récupère les détails d'un appel."""
        try:
            resp = await self.client.get(
                f"{self.base_url}/calls/{call_id}",
                headers=self.headers
            )
            if resp.status_code == 200:
                data = resp.json()
                calls = data.get("list", [])
                return calls[0] if calls else None
        except Exception as e:
            log.debug(f"Ringover get_call_details error: {e}")
        return None

    async def list_transcriptions(self, start_date: str, end_date: str,
                                    limit: int = 50, offset: int = 0) -> list[dict]:
        """Liste les transcriptions sur une période."""
        try:
            resp = await self.client.get(
                f"{self.base_url}/transcriptions",
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit,
                    "offset": offset
                },
                headers=self.headers
            )
            if resp.status_code == 200:
                return resp.json() if isinstance(resp.json(), list) else []
        except Exception as e:
            log.debug(f"Ringover list_transcriptions error: {e}")
        return []

    async def close(self):
        await self.client.aclose()


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 4 : LOGIQUE DE QUALIFICATION + PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_name(name: str) -> str:
    """Normalise un nom pour la comparaison."""
    name = name.lower().strip()
    # Supprimer les préfixes courants
    for prefix in ["dr ", "dr. ", "docteur ", "cabinet ", "centre ", "cabinet dentaire ",
                    "centre dentaire ", "cabinet du dr ", "cabinet du docteur "]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    return name.strip()


def names_match(doctolib_name: str, sellsy_name: str) -> bool:
    """Vérifie si deux noms correspondent (fuzzy)."""
    n1 = normalize_name(doctolib_name)
    n2 = normalize_name(sellsy_name)

    # Match exact
    if n1 == n2:
        return True

    # Un nom contient l'autre
    if n1 in n2 or n2 in n1:
        return True

    # Match par mots (au moins 2 mots en commun)
    words1 = set(n1.split())
    words2 = set(n2.split())
    common = words1 & words2
    # Exclure les mots trop courts
    common = {w for w in common if len(w) > 2}
    if len(common) >= 2:
        return True

    # Match nom de famille seul
    if len(words1) > 0 and len(words2) > 0:
        last1 = max(words1, key=len)
        last2 = max(words2, key=len)
        if len(last1) > 3 and last1 == last2:
            return True

    return False


async def qualify_dentist(
    dentist: dict,
    sellsy: SellsyClient,
    ringover: RingoverClient,
    dry_run: bool = False
) -> dict:
    """
    Qualifie un dentiste selon les règles:
    - Priority 1: Jamais contacté (absent de Sellsy)
    - Priority 2: Inactif >60 jours dans Sellsy
    - Skip: Contacté récemment ou tag bloquant
    """
    decision = {
        "dentist": dentist["name"],
        "doctolib_url": f"https://www.doctolib.fr{dentist['href']}",
        "city": dentist.get("source_city", ""),
        "phone": dentist.get("phone", ""),
        "has_slots": dentist.get("hasSlotsThisWeek", False),
        "time_slots": dentist.get("timeSlotCount", 0),
        "est_sur_doctolib": dentist.get("est_sur_doctolib", ""),
        "pas_sur_doctolib": dentist.get("pas_sur_doctolib", ""),
        "action": None,
        "priority": None,
        "reason": None,
        "sellsy_match": None,
        "ringover_match": None,
        "created_company_id": None,
        "created_opportunity_id": None,
    }

    name = dentist["name"]
    phone = dentist.get("phone", "")

    # ── Sellsy check ──
    sellsy_contacts = []
    if sellsy:
        # Chercher par nom du dentiste
        search_term = normalize_name(name)
        # Prendre le mot le plus long (souvent le nom de famille)
        search_words = search_term.split()
        if search_words:
            search_query = max(search_words, key=len)
            if len(search_query) > 2:
                sellsy_contacts = await sellsy.search_contacts_by_lastname(search_query)

        # Filtrer pour ne garder que les vrais matchs
        matched_contacts = [
            c for c in sellsy_contacts
            if names_match(name, f"{c.get('first_name', '') or ''} {c.get('last_name', '') or ''}".strip())
            or (phone and phone_matches(phone, c))
        ]

        if matched_contacts:
            contact = matched_contacts[0]
            last_update = contact.get("updated", "")
            decision["sellsy_match"] = {
                "contact_id": contact["id"],
                "name": f"{contact.get('first_name', '') or ''} {contact.get('last_name', '') or ''}".strip(),
                "phone": contact.get("phone_number") or contact.get("mobile_number") or "",
                "last_updated": last_update,
            }

            # Vérifier la date de dernière activité
            if last_update:
                try:
                    update_dt = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                    days_since = (datetime.now(update_dt.tzinfo) - update_dt).days
                    if days_since <= INACTIVITY_DAYS:
                        decision["action"] = "SKIP"
                        decision["reason"] = f"Contacté il y a {days_since} jours (< {INACTIVITY_DAYS}j)"
                        return decision
                    else:
                        decision["action"] = "ADD_TO_PIPELINE"
                        decision["priority"] = 2
                        decision["reason"] = f"Inactif depuis {days_since} jours (> {INACTIVITY_DAYS}j)"
                except Exception:
                    decision["action"] = "ADD_TO_PIPELINE"
                    decision["priority"] = 2
                    decision["reason"] = "Date d'activité non parsable, traité comme inactif"
        else:
            decision["action"] = "ADD_TO_PIPELINE"
            decision["priority"] = 1
            decision["reason"] = "Jamais contacté (absent de Sellsy)"
    else:
        # Pas de clé Sellsy → tout est Priority 1
        decision["action"] = "ADD_TO_PIPELINE"
        decision["priority"] = 1
        decision["reason"] = "Jamais contacté (pas de vérification Sellsy - clé manquante)"

    # ── Ringover check (complémentaire) ──
    if phone and ringover:
        ringover_contact = ringover.find_contact_by_phone(phone)
        if ringover_contact:
            decision["ringover_match"] = {
                "contact_id": ringover_contact.get("contact_id"),
                "company": ringover_contact.get("company", ""),
            }

    # ── Actions: créer dans Sellsy + ajouter au pipeline ──
    if decision["action"] == "ADD_TO_PIPELINE" and not dry_run:
        doctolib_url = f"https://www.doctolib.fr{dentist['href']}"
        note = (
            f"[Auto-Doctolib {datetime.now().strftime('%Y-%m-%d')}] "
            f"Dispo cette semaine ({dentist.get('timeSlotCount', 0)} créneaux). "
            f"Priority {decision['priority']}. {decision['reason']}. "
            f"URL: {doctolib_url}"
        )

        if sellsy:
            if decision["priority"] == 1:
                # Créer le prospect dans Sellsy
                company = await sellsy.create_company(
                    name=name,
                    phone=phone,
                    note=note
                )
                if company:
                    company_id = company.get("id") or company.get("data", {}).get("id")
                    decision["created_company_id"] = company_id

                    if company_id:
                        # Créer l'opportunité
                        opp = await sellsy.create_opportunity(
                            name=f"Doctolib - {name}",
                            company_id=company_id,
                            pipeline_id=PIPELINE_ID,
                            step_id=STEP_INTERESSES_ID,
                            note=note,
                            probability=20 if decision["priority"] == 1 else 30
                        )
                        if opp:
                            opp_id = opp.get("id") or opp.get("data", {}).get("id")
                            decision["created_opportunity_id"] = opp_id

            elif decision["priority"] == 2:
                # Prospect existant → récupérer la company via le contact
                contact_info = decision.get("sellsy_match", {})
                contact_id = contact_info.get("contact_id")
                company_id = None
                if contact_id:
                    company = await sellsy.get_contact_company(contact_id)
                    if company:
                        company_id = company.get("id")

                if company_id:
                    opp = await sellsy.create_opportunity(
                        name=f"Doctolib Relance - {name}",
                        company_id=company_id,
                        pipeline_id=PIPELINE_ID,
                        step_id=STEP_INTERESSES_ID,
                        note=note,
                        probability=30
                    )
                    if opp:
                        opp_id = opp.get("id") or opp.get("data", {}).get("id")
                        decision["created_opportunity_id"] = opp_id
                else:
                    log.warning(f"    ⚠️ Pas de company trouvée pour {name} — opportunité non créée")

    return decision


def phone_matches(phone: str, contact: dict) -> bool:
    """Vérifie si un numéro correspond à un contact Sellsy."""
    phone_clean = re.sub(r"[\s.+\-]", "", phone)
    for field in ["phone_number", "mobile_number"]:
        val = contact.get(field, "") or ""
        val_clean = re.sub(r"[\s.+\-]", "", val)
        if phone_clean and val_clean and (phone_clean in val_clean or val_clean in phone_clean):
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# RAPPORT JSON
# ═══════════════════════════════════════════════════════════════════════════════

def generate_report(decisions: list[dict], city: str) -> dict:
    """Génère le rapport JSON final."""
    now = datetime.now()
    p1 = [d for d in decisions if d.get("priority") == 1]
    p2 = [d for d in decisions if d.get("priority") == 2]
    skipped = [d for d in decisions if d.get("action") == "SKIP"]

    report = {
        "metadata": {
            "date": now.isoformat(),
            "city": city,
            "total_scraped": len(decisions),
            "priority_1_count": len(p1),
            "priority_2_count": len(p2),
            "skipped_count": len(skipped),
        },
        "priority_1": [
            {
                "name": d["dentist"],
                "doctolib_url": d["doctolib_url"],
                "phone": d["phone"],
                "time_slots": d["time_slots"],
                "est_sur_doctolib": d.get("est_sur_doctolib", ""),
                "pas_sur_doctolib": d.get("pas_sur_doctolib", ""),
                "reason": d["reason"],
                "sellsy_company_id": d.get("created_company_id"),
                "sellsy_opportunity_id": d.get("created_opportunity_id"),
            }
            for d in p1
        ],
        "priority_2": [
            {
                "name": d["dentist"],
                "doctolib_url": d["doctolib_url"],
                "phone": d["phone"],
                "time_slots": d["time_slots"],
                "est_sur_doctolib": d.get("est_sur_doctolib", ""),
                "pas_sur_doctolib": d.get("pas_sur_doctolib", ""),
                "reason": d["reason"],
                "sellsy_match": d.get("sellsy_match"),
                "sellsy_opportunity_id": d.get("created_opportunity_id"),
            }
            for d in p2
        ],
        "skipped": [
            {
                "name": d["dentist"],
                "reason": d["reason"],
            }
            for d in skipped
        ],
    }

    return report


# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS — mise à jour des colonnes Est/Pas sur Doctolib
# ═══════════════════════════════════════════════════════════════════════════════

def update_google_sheet(decisions: list[dict]):
    """Met à jour le Google Sheet avec les colonnes Est_Sur_Doctolib / Pas_Sur_Doctolib."""
    if not GOOGLE_SHEET_ID or not GOOGLE_REFRESH_TOKEN:
        log.warning("Google Sheets non configuré, skip mise à jour")
        return

    try:
        import gspread
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request as GRequest
    except ImportError:
        log.warning("gspread non installé, skip mise à jour Google Sheet")
        return

    try:
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
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.worksheet("Dentistes")

        # Lire les headers pour trouver les bons indices de colonnes
        headers = worksheet.row_values(1)

        # Ajouter les colonnes si elles n'existent pas
        cols_to_add = []
        if "Est_Sur_Doctolib" not in headers:
            cols_to_add.append("Est_Sur_Doctolib")
        if "Pas_Sur_Doctolib" not in headers:
            cols_to_add.append("Pas_Sur_Doctolib")

        if cols_to_add:
            # Étendre la grille si nécessaire
            current_cols = worksheet.col_count
            needed_cols = len(headers) + len(cols_to_add)
            if needed_cols > current_cols:
                worksheet.resize(cols=needed_cols)
                log.info(f"  📐 Grille étendue à {needed_cols} colonnes")

            for col_name in cols_to_add:
                headers.append(col_name)
                worksheet.update_cell(1, len(headers), col_name)

        col_est = headers.index("Est_Sur_Doctolib") + 1  # 1-indexed
        col_pas = headers.index("Pas_Sur_Doctolib") + 1

        # Lire la colonne Doctolib_URL pour matcher les dentistes
        col_url_idx = headers.index("Doctolib_URL") + 1 if "Doctolib_URL" in headers else None
        col_nom_idx = headers.index("Nom") + 1 if "Nom" in headers else None

        if col_url_idx:
            all_urls = worksheet.col_values(col_url_idx)
        else:
            all_urls = []

        if col_nom_idx:
            all_noms = worksheet.col_values(col_nom_idx)
        else:
            all_noms = []

        # Construire un index URL → row
        url_to_row = {}
        for row_idx, url in enumerate(all_urls):
            if url and url.strip():
                url_to_row[url.strip()] = row_idx + 1  # 1-indexed

        # Construire un index nom → row (fallback)
        nom_to_row = {}
        for row_idx, nom in enumerate(all_noms):
            if nom and nom.strip():
                nom_to_row[normalize_name(nom.strip())] = row_idx + 1

        updated = 0
        for d in decisions:
            doctolib_url = d.get("doctolib_url", "")
            est = d.get("est_sur_doctolib", "")
            pas = d.get("pas_sur_doctolib", "")

            if not est and not pas:
                continue

            # Trouver la ligne par URL ou par nom
            row = url_to_row.get(doctolib_url)
            if not row:
                # Essayer de matcher par nom
                dentist_name = normalize_name(d.get("dentist", ""))
                row = nom_to_row.get(dentist_name)

            if row:
                worksheet.update_cell(row, col_est, est)
                worksheet.update_cell(row, col_pas, pas)
                updated += 1
                log.info(f"  📝 Sheet mis à jour: {d.get('dentist', '')} → Est={est}, Pas={pas}")

        log.info(f"📊 Google Sheet: {updated}/{len(decisions)} lignes mises à jour")

    except Exception as e:
        log.error(f"Erreur mise à jour Google Sheet: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def run(city: str, max_pages: int = 5, dry_run: bool = False, output_dir: str = "."):
    """Exécute le pipeline complet pour une ville."""
    log.info(f"═══════════════════════════════════════════")
    log.info(f"  ORCHESTRATEUR EASYDENTIST — {city.upper()}")
    log.info(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"  Mode: {'DRY RUN' if dry_run else 'PRODUCTION'}")
    log.info(f"═══════════════════════════════════════════")

    # 1. Scraping Doctolib
    dentists = await scrape_doctolib_city(city, max_pages=max_pages)

    if not dentists:
        log.warning(f"Aucun dentiste avec dispo trouvé à {city}")
        return

    # 1b. Vérifier si chaque dentiste est réellement inscrit sur Doctolib
    log.info(f"\n{'='*50}")
    log.info(f"ÉTAPE 1b : Vérification profils Doctolib")
    log.info(f"{'='*50}")
    dentists = await check_doctolib_profiles(dentists)

    log.info(f"\n{'='*50}")
    log.info(f"ÉTAPE 2-3 : Qualification Sellsy + Ringover")
    log.info(f"{'='*50}")

    # Init clients
    sellsy = SellsyClient(SELLSY_CLIENT_ID, SELLSY_CLIENT_SECRET) if SELLSY_CLIENT_ID else None
    ringover = RingoverClient(RINGOVER_API_KEY) if RINGOVER_API_KEY else None

    if ringover and RINGOVER_API_KEY:
        await ringover.load_contacts_cache()

    # 2-3. Qualifier chaque dentiste
    decisions = []
    for i, dentist in enumerate(dentists[:MAX_DENTISTS_PER_RUN]):
        log.info(f"  [{i+1}/{len(dentists)}] {dentist['name']}...")
        decision = await qualify_dentist(
            dentist,
            sellsy,
            ringover,
            dry_run=dry_run
        )
        decisions.append(decision)
        log.info(f"    → {decision['action']} (P{decision.get('priority', '-')}) — {decision['reason']}")

        # Petit délai pour ne pas surcharger les APIs
        await asyncio.sleep(0.3)

    # 4. Rapport
    report = generate_report(decisions, city)

    # Sauvegarder
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"doctolib_{city.lower().replace(' ', '_')}_{date_str}.json"
    filepath = Path(output_dir) / filename
    filepath.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"\n📁 Rapport sauvegardé: {filepath}")

    # 5. Mise à jour Google Sheet
    log.info(f"\n{'='*50}")
    log.info(f"ÉTAPE 5 : Mise à jour Google Sheet")
    log.info(f"{'='*50}")
    update_google_sheet(decisions)

    # Résumé
    log.info(f"\n{'═'*50}")
    log.info(f"  RÉSUMÉ — {city.upper()} — {date_str}")
    log.info(f"{'═'*50}")
    log.info(f"  📊 Total scrapé avec dispo : {report['metadata']['total_scraped']}")
    log.info(f"  🔴 Priority 1 (jamais contacté) : {report['metadata']['priority_1_count']}")
    log.info(f"  🟠 Priority 2 (inactif >60j) : {report['metadata']['priority_2_count']}")
    log.info(f"  ⏭️  Skippé (récemment contacté) : {report['metadata']['skipped_count']}")
    log.info(f"{'═'*50}")

    # Cleanup
    if sellsy:
        await sellsy.close()
    if ringover:
        await ringover.close()

    return report


def main():
    parser = argparse.ArgumentParser(description="Orchestrateur Easydentist Doctolib → Sellsy")
    parser.add_argument("--city", type=str, help="Ville cible (ex: Paris, Marseille)")
    parser.add_argument("--cities-file", type=str, help="Fichier texte avec une ville par ligne")
    parser.add_argument("--max-pages", type=int, default=5, help="Nombre max de pages Doctolib par ville")
    parser.add_argument("--dry-run", action="store_true", help="Ne rien créer dans Sellsy")
    parser.add_argument("--output-dir", type=str, default=".", help="Dossier de sortie pour les rapports")

    args = parser.parse_args()

    if not args.city and not args.cities_file:
        parser.error("Spécifier --city ou --cities-file")

    cities = []
    if args.city:
        cities = [args.city]
    elif args.cities_file:
        with open(args.cities_file) as f:
            cities = [line.strip() for line in f if line.strip()]

    for city in cities:
        asyncio.run(run(
            city=city,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            output_dir=args.output_dir
        ))


if __name__ == "__main__":
    main()
