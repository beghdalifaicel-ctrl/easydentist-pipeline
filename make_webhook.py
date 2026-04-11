"""\nmake_webhook.py \u2014 Serveur Flask exposant des webhooks pour Railway.\nEndpoints:\n  /trigger/enrich    \u2014 Lance l'enrichissement Est/Pas sur Doctolib (bulk)\n  /trigger/extract-phones \u2014 Lance l'extraction des t\u00e9l\u00e9phones depuis Doctolib\n  /trigger/orchestrator \u2014 Lance le pipeline complet (scrape + qualify)\n  /status            \u2014 \u00c9tat des t\u00e2ches en cours\n  /health            \u2014 Health check Railway\n"""\n\nimport os\nimport asyncio\nimport logging\nimport threading\nimport traceback\nfrom datetime import datetime\nfrom flask import Flask, jsonify, request\nfrom dotenv import load_dotenv\n\nload_dotenv()\n\nlogging.basicConfig(level=logging.INFO, format=\"%(asctime)s [%(levelname)s] %(message)s\")\nlogger = logging.getLogger(__name__)\n\napp = Flask(__name__)\n\n# \u2500\u2500\u2500 \u00c9tat global \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\ntasks_status = {\n    \"enrich\": {\"running\": False, \"last_run\": None, \"last_result\": None},\n    \"orchestrator\": {\"running\": False, \"last_run\": None, \"last_result\": None},\n    \"extract_phones\": {\"running\": False, \"last_run\": None, \"last_result\": None},\n}\n\n\n# \u2500\u2500\u2500 Helpers pour lancer les scripts async dans un thread \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\ndef run_async_in_thread(coro_func, task_name, **kwargs):\n    \"\"\"Lance une coroutine dans un nouveau event loop dans un thread.\"\"\"\n    tasks_status[task_name][\"running\"] = True\n    tasks_status[task_name][\"last_run\"] = datetime.now().isoformat()\n\n    def target():\n        try:\n            loop = asyncio.new_event_loop()\n            asyncio.set_event_loop(loop)\n            result = loop.run_until_complete(coro_func(**kwargs))\n            tasks_status[task_name][\"last_result\"] = {\"status\": \"success\", \"detail\": str(result)}\n            logger.info(f\"[{task_name}] Termin\u00e9 avec succ\u00e8s\")\n        except Exception as e:\n            tb = traceback.format_exc()\n            tasks_status[task_name][\"last_result\"] = {\"status\": \"error\", \"detail\": str(e), \"traceback\": tb}\n            logger.error(f\"[{task_name}] Erreur: {e}\")\n            logger.error(f\"[{task_name}] Traceback:\\n{tb}\")\n        finally:\n            tasks_status[task_name][\"running\"] = False\n\n    thread = threading.Thread(target=target, daemon=True)\n    thread.start()\n

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
            "/trigger/enrich": "POST/GET — Enrichissement Est/Pas sur Doctolib",
            "/trigger/enrich?batch_size=200&concurrency=10&force=false&browser=false": "Params",
            "/trigger/orchestrator": "POST/GET — Pipeline complet Doctolib → Sellsy",
            "/trigger/orchestrator?city=Paris&max_pages=5&dry_run=false": "Params",
            "/trigger/extract-phones": "POST/GET — Extraction téléphones Doctolib",
            "/trigger/extract-phones?batch_size=100&concurrency=3&force=false": "Params",
            "/status": "GET — État des tâches",
            "/health": "GET — Health check",
        },
    }), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
