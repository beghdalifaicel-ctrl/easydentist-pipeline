"""
make_webhook.py — Serveur Flask exposant des webhooks pour Railway.
Endpoints:
  /trigger/enrich    — Lance l'enrichissement Est/Pas sur Doctolib (bulk)
  /trigger/orchestrator — Lance le pipeline complet (scrape + qualify)
  /status            — État des tâches en cours
  /health            — Health check Railway
"""

import os
import asyncio
import logging
import threading
import traceback
from datetime import datetime
from flask import Flask, jsonify, request
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ─── État global ────────────────────────────────────────────────────────────
tasks_status = {
    "enrich": {"running": False, "last_run": None, "last_result": None},
    "orchestrator": {"running": False, "last_run": None, "last_result": None},
    "daily": {"running": False, "last_run": None, "last_result": None},
    "extract_phones": {"running": False, "last_run": None, "last_result": None},
    "extract_emails": {"running": False, "last_run": None, "last_result": None},
    "extract_cabinet_name": {"running": False, "last_run": None, "last_result": None},
}


# ─── Helpers pour lancer les scripts async dans un thread ───────────────────
def run_async_in_thread(coro_func, task_name, **kwargs):
    """Lance une coroutine dans un nouveau event loop dans un thread."""
    tasks_status[task_name]["running"] = True
    tasks_status[task_name]["last_run"] = datetime.now().isoformat()

    def target():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(coro_func(**kwargs))
            tasks_status[task_name]["last_result"] = {"status": "success", "detail": str(result)}
            logger.info(f"[{task_name}] Terminé avec succès")
        except Exception as e:
            tb = traceback.format_exc()
            tasks_status[task_name]["last_result"] = {"status": "error", "detail": str(e), "traceback": tb}
            logger.error(f"[{task_name}] Erreur: {e}")
            logger.error(f"[{task_name}] Traceback:\n{tb}")
        finally:
            tasks_status[task_name]["running"] = False

    thread = threading.Thread(target=target, daemon=True)
    thread.start()


# ─── Endpoints ──────────────────────────────────────────────────────────────
@app.route("/trigger/enrich", methods=["POST", "GET"])
def trigger_enrich():
    """Lance l'enrichissement Est/Pas sur Doctolib sur le Google Sheet."""
    if tasks_status["enrich"]["running"]:
        return jsonify({
            "status": "already_running",
            "message": "L'enrichissement est déjà en cours",
            "started_at": tasks_status["enrich"]["last_run"],
        }), 409

    # Paramètres optionnels via query string
    batch_size = int(request.args.get("batch_size", 200))
    start_row = int(request.args.get("start_row", 2))
    concurrency = int(request.args.get("concurrency", 10))
    force = request.args.get("force", "false").lower() == "true"
    use_browser = request.args.get("browser", "false").lower() == "true"

    from enrich_doctolib_status import run as enrich_run

    run_async_in_thread(
        enrich_run,
        "enrich",
        batch_size=batch_size,
        start_row=start_row,
        concurrency=concurrency,
        skip_filled=not force,
        use_browser=use_browser,
    )

    return jsonify({
        "status": "started",
        "message": f"Enrichissement lancé (batch={batch_size}, concurrency={concurrency}, force={force})",
        "timestamp": datetime.now().isoformat(),
    }), 200


@app.route("/trigger/orchestrator", methods=["POST", "GET"])
def trigger_orchestrator():
    """Lance le pipeline complet: scrape Doctolib → qualify → Sellsy."""
    if tasks_status["orchestrator"]["running"]:
        return jsonify({
            "status": "already_running",
            "message": "L'orchestrateur est déjà en cours",
            "started_at": tasks_status["orchestrator"]["last_run"],
        }), 409

    # Paramètres
    city = request.args.get("city", "Paris")
    max_pages = int(request.args.get("max_pages", 5))
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    from orchestrator import run as orchestrator_run

    run_async_in_thread(
        orchestrator_run,
        "orchestrator",
        city=city,
        max_pages=max_pages,
        dry_run=dry_run,
        output_dir=".",
    )

    return jsonify({
        "status": "started",
        "message": f"Orchestrateur lancé (city={city}, max_pages={max_pages}, dry_run={dry_run})",
        "timestamp": datetime.now().isoformat(),
    }), 200


@app.route("/trigger/daily", methods=["POST", "GET"])
def trigger_daily():
    """Lance le run quotidien automatique: rotation intelligente sur toute la France."""
    if tasks_status["daily"]["running"]:
        return jsonify({
            "status": "already_running",
            "message": "Le run quotidien est déjà en cours",
            "started_at": tasks_status["daily"]["last_run"],
        }), 409

    # Paramètres
    max_pages = int(request.args.get("max_pages", 5))
    dry_run = request.args.get("dry_run", "false").lower() == "true"
    batch_size = int(request.args.get("batch_size", 30))
    target = int(request.args.get("target", 0))

    from orchestrator import run_daily

    run_async_in_thread(
        run_daily,
        "daily",
        max_pages=max_pages,
        dry_run=dry_run,
        output_dir=".",
        batch_size=batch_size,
        target_prospects=target,
    )

    return jsonify({
        "status": "started",
        "message": f"Run quotidien lancé (batch={batch_size}, max_pages={max_pages}, target={target}, dry_run={dry_run})",
        "timestamp": datetime.now().isoformat(),
    }), 200


@app.route("/rotation-state", methods=["GET"])
def rotation_state():
    """Retourne l'état de rotation des villes."""
    from orchestrator import load_rotation_state, ALL_CITIES_FRANCE
    state = load_rotation_state()
    total = len(ALL_CITIES_FRANCE)
    scraped = len([c for c in ALL_CITIES_FRANCE if c in state])
    never = len([c for c in ALL_CITIES_FRANCE if c not in state])
    return jsonify({
        "total_cities": total,
        "cities_scraped_at_least_once": scraped,
        "cities_never_scraped": never,
        "state": state,
    }), 200


@app.route("/trigger/extract-phones", methods=["POST", "GET"])
def trigger_extract_phones():
    """Lance l'extraction des téléphones depuis les profils Doctolib."""
    if tasks_status["extract_phones"]["running"]:
        return jsonify({
            "status": "already_running",
            "message": "L'extraction des téléphones est déjà en cours",
            "started_at": tasks_status["extract_phones"]["last_run"],
        }), 409

    batch_size = int(request.args.get("batch_size", 100))
    start_row = int(request.args.get("start_row", 2))
    concurrency = int(request.args.get("concurrency", 3))
    force = request.args.get("force", "false").lower() == "true"

    from extract_phones_doctolib import run as extract_phones_run

    run_async_in_thread(
        extract_phones_run,
        "extract_phones",
        batch_size=batch_size,
        start_row=start_row,
        concurrency=concurrency,
        skip_filled=not force,
    )

    return jsonify({
        "status": "started",
        "message": f"Extraction téléphones lancée (batch={batch_size}, concurrency={concurrency}, force={force})",
        "timestamp": datetime.now().isoformat(),
    }), 200


@app.route("/trigger/extract-emails", methods=["POST", "GET"])
def trigger_extract_emails():
    """Lance l'extraction des emails depuis les profils Doctolib."""
    if tasks_status["extract_emails"]["running"]:
        return jsonify({
            "status": "already_running",
            "message": "L'extraction des emails est déjà en cours",
            "started_at": tasks_status["extract_emails"]["last_run"],
        }), 409

    batch_size = int(request.args.get("batch_size", 100))
    start_row = int(request.args.get("start_row", 2))
    concurrency = int(request.args.get("concurrency", 3))
    force = request.args.get("force", "false").lower() == "true"

    from extract_emails_doctolib import run as extract_emails_run

    run_async_in_thread(
        extract_emails_run,
        "extract_emails",
        batch_size=batch_size,
        start_row=start_row,
        concurrency=concurrency,
        skip_filled=not force,
    )

    return jsonify({
        "status": "started",
        "message": f"Extraction emails lancée (batch={batch_size}, concurrency={concurrency}, force={force})",
        "timestamp": datetime.now().isoformat(),
    }), 200


@app.route("/trigger/extract-cabinet-name", methods=["POST", "GET"])
def trigger_extract_cabinet_name():
    """Lance l'extraction des noms d'établissements depuis Doctolib."""
    if tasks_status["extract_cabinet_name"]["running"]:
        return jsonify({
            "status": "already_running",
            "message": "L'extraction des noms est déjà en cours",
            "started_at": tasks_status["extract_cabinet_name"]["last_run"],
        }), 409

    batch_size = int(request.args.get("batch_size", 100))
    concurrency = int(request.args.get("concurrency", 3))
    force = request.args.get("force", "false").lower() == "true"

    from extract_cabinet_name import run as extract_name_run

    run_async_in_thread(
        extract_name_run,
        "extract_cabinet_name",
        batch_size=batch_size,
        concurrency=concurrency,
        skip_filled=not force,
    )

    return jsonify({
        "status": "started",
        "message": f"Extraction noms lancée (batch={batch_size}, concurrency={concurrency}, force={force})",
        "timestamp": datetime.now().isoformat(),
    }), 200


@app.route("/status", methods=["GET"])
def status():
    """État de toutes les tâches."""
    return jsonify(tasks_status), 200


@app.route("/health", methods=["GET"])
def health():
    """Health check Railway."""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200


@app.route("/", methods=["GET"])
def index():
    """Page d'accueil."""
    return jsonify({
        "service": "Easydentist Pipeline — Railway",
        "endpoints": {
            "/trigger/daily": "POST/GET — 🇫🇷 Run quotidien rotation toute la France",
            "/trigger/daily?batch_size=30&max_pages=5&target=100&dry_run=false": "Params daily",
            "/trigger/orchestrator": "POST/GET — Pipeline pour UNE ville",
            "/trigger/orchestrator?city=Paris&max_pages=5&dry_run=false": "Params orchestrator",
            "/trigger/enrich": "POST/GET — Enrichissement Est/Pas sur Doctolib",
            "/trigger/extract-phones": "POST/GET — Extraction téléphones Doctolib",
            "/trigger/extract-emails": "POST/GET — Extraction emails Doctolib",
            "/trigger/extract-cabinet-name": "POST/GET — Extraction noms établissements",
            "/rotation-state": "GET — État de rotation des villes",
            "/status": "GET — État des tâches",
            "/health": "GET — Health check",
        },
    }), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
