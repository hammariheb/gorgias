from pathlib import Path
from dotenv import load_dotenv
import os
 
load_dotenv(Path(__file__).parent.parent / ".env", override=True)
 
# ── Scraping ──────────────────────────────────────────────────
# BuiltWith top-sites URL pattern:
# https://builtwith.com/top-sites/{Country}/eCommerce?p={page}
BUILTWITH_BASE_URL  = "https://builtwith.com/top-sites/France/eCommerce"
 
# 1928 domains / ~50 per page = ~39 pages
# Set to 100 as a safe ceiling — the scraper stops automatically
# when an empty page is returned, so this never over-fetches
MAX_PAGES           = 100
 
DELAY_BETWEEN_PAGES = (2.0, 4.0)   # polite delay — BuiltWith rate-limits aggressively
 
# ── Output ────────────────────────────────────────────────────
OUTPUT_CSV      = "leads_builtwith_fr.csv"
OUTPUT_SEED_CSV = "dbt_transformation/models/seeds/leads_builtwith_fr.csv"
 
# ── User Agents ───────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]
 