# scraper/config.py
# ==================
# Toutes les constantes du pipeline.
# Les credentials viennent du .env — jamais hardcodés.

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variable manquante dans .env : '{key}'\n"
            f"→ Copier .env.example en .env et remplir la valeur."
        )
    return value


# ── BigQuery (depuis .env) ────────────────────────────────────
BQ_PROJECT = _require("BQ_PROJECT")

# ── BigQuery (statique) ───────────────────────────────────────
LEADS_DATASET   = "leads"
LEADS_TABLE     = "leads_raw"
REVIEWS_DATASET = "reviews"
REVIEWS_TABLE   = "reviews_raw"
BQ_LOCATION     = "EU"
BQ_BATCH_SIZE   = 500

# ── Scraping ──────────────────────────────────────────────────
# SMB (<$3M GMV) : 10 pages max → 200 reviews — signal suffisant
# Full scrape (--full) : 50 pages → 1000 reviews — pour les gros comptes
MAX_PAGES_PER_DOMAIN      = 10
MAX_PAGES_PER_DOMAIN_FULL = 50

DELAY_BETWEEN_PAGES   = (1.5, 3.0)
DELAY_BETWEEN_DOMAINS = (3.0, 6.0)

# ── User-Agents ───────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]