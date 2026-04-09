from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent.parent / ".env", override=True)


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing variable in .env: {key}")
    return val


# ── OpenAI ────────────────────────────────────────────────────
OPENAI_API_KEY = _require("OPENAI_API_KEY")
OPENAI_MODEL   = "gpt-4o-mini"

# ── BigQuery ──────────────────────────────────────────────────
BQ_PROJECT       = _require("BQ_PROJECT")
BQ_LOCATION      = "EU"
REVIEWS_DATASET  = "reviews"

# ── Source 1 : original pipeline ─────────────────────────────
SOURCE_TABLE  = "reviews_raw"
ENRICHED_TABLE = "reviews_enriched"

# ── Source 2 : French reference pipeline ─────────────────────
SOURCE_TABLE_FR   = "reviews_raw_fr"
ENRICHED_TABLE_FR = "reviews_enriched_fr"

# ── Batch settings ────────────────────────────────────────────
BATCH_SIZE            = 10
MAX_REVIEW_LENGTH     = 1000
DELAY_BETWEEN_BATCHES = 0.5