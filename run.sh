#!/bin/bash
# ─── ORCHESTRATEUR EASYDENTIST ───
# Usage: ./run.sh [ville] [max_pages] [--dry-run]
# Exemple: ./run.sh Paris 5
#          ./run.sh Marseille 3 --dry-run

set -euo pipefail

# Charger les variables d'environnement
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

CITY="${1:-Paris}"
MAX_PAGES="${2:-5}"
DRY_RUN="${3:-}"

echo "══════════════════════════════════════════"
echo "  EASYDENTIST — Orchestrateur Doctolib"
echo "  Ville: $CITY | Pages: $MAX_PAGES"
echo "  Date: $(date '+%Y-%m-%d %H:%M')"
echo "══════════════════════════════════════════"

# Vérifier les dépendances
pip install -q playwright httpx python-dotenv 2>/dev/null || true
playwright install chromium 2>/dev/null || true

# Lancer le script
if [ "$DRY_RUN" = "--dry-run" ]; then
    python orchestrator.py --city "$CITY" --max-pages "$MAX_PAGES" --dry-run --output-dir .
else
    python orchestrator.py --city "$CITY" --max-pages "$MAX_PAGES" --output-dir .
fi
