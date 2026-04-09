from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent.parent / ".env", override=True)


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Variable manquante dans .env : {key}")
    return val


BQ_PROJECT  = _require("BQ_PROJECT")
BQ_LOCATION = "EU"

# ── Source 1 : target leads (original) ──────────────────────
LEADS_DATASET   = "leads"
LEADS_TABLE     = "leads_table"

# ── Source 2 : BuiltWith French eCommerce leads ───────────────
FR_LEADS_DATASET = "analytics"
FR_LEADS_TABLE   = "leads_builtwith_fr"   # dbt staging model output

# ── Reviews output tables ─────────────────────────────────────
REVIEWS_DATASET     = "reviews"
REVIEWS_TABLE       = "reviews_raw"        # original pipeline
REVIEWS_TABLE_FR    = "reviews_raw_fr"     # new French reference data pipeline

# ── Scraping ──────────────────────────────────────────────────
MAX_PAGES_PER_DOMAIN      = 10
MAX_PAGES_PER_DOMAIN_FULL = 50
DELAY_BETWEEN_PAGES       = (1.5, 3.0)
DELAY_BETWEEN_DOMAINS     = (3.0, 6.0)
BQ_BATCH_SIZE             = 500

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]