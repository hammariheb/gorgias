# ai_enrichment/config.py

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


# ── Credentials ───────────────────────────────────────────────
OPENAI_API_KEY = _require("OPENAI_API_KEY")
BQ_PROJECT     = _require("BQ_PROJECT")

# ── BigQuery ──────────────────────────────────────────────────
REVIEWS_DATASET = "reviews"
SOURCE_TABLE    = "reviews_raw"
ENRICHED_TABLE  = "reviews_enriched"

# ── OpenAI ────────────────────────────────────────────────────
MODEL       = "gpt-4o-mini"
TEMPERATURE = 0
MAX_TOKENS  = 1500

# ── Batch ─────────────────────────────────────────────────────
BATCH_SIZE            = 10
MAX_RETRIES           = 3
DELAY_BETWEEN_BATCHES = 0.5

# ── Valeurs acceptées ─────────────────────────────────────────
VALID_SENTIMENTS = {"positive", "neutral", "negative"}
VALID_CATEGORIES = {
    "customer_support", "shipping", "product_quality",
    "pricing", "ux", "returns", "packaging",
    "communication", "stock", "loyalty", "other"
}