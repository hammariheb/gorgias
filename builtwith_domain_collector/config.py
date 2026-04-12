from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=False)

BUILTWITH_BASE_URL  = "https://builtwith.com/top-sites/France/eCommerce"
MAX_PAGES           = 20
DELAY_BETWEEN_PAGES = (2.0, 4.0)
OUTPUT_CSV          = "leads_builtwith_fr.csv"
OUTPUT_SEED_CSV     = "dbt_transformation/seeds/leads_builtwith_fr.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

SEED_COLUMNS = [
    "rank", "domain", "sales_revenue",
    "tech_spend", "social_followers", "traffic_tier", "country", "source",
]
