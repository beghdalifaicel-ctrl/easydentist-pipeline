# ─── Easydentist Pipeline — Dockerfile Railway ─────────────────────────────
# Python 3.11 + Playwright Chromium pour Bright Data Browser API
FROM python:3.11-slim

# Dépendances système pour Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installer Playwright + Chromium
RUN playwright install chromium && playwright install-deps chromium

# Copier le code
COPY . .

# Port exposé (Railway injecte $PORT)
EXPOSE 8080

# Lancer le webhook Flask via gunicorn
CMD ["sh", "-c", "gunicorn make_webhook:app --bind 0.0.0.0:${PORT:-8080} --timeout 600 --workers 2"]
