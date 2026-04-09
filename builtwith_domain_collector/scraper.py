import logging
import random
import time

import httpx

from .config import MAX_PAGES, DELAY_BETWEEN_PAGES
from .http_client import fetch_page
from .parser import parse_table_rows, is_last_page

log = logging.getLogger(__name__)


def scrape_builtwith_france(
    client:    httpx.Client,
    max_pages: int = MAX_PAGES,
) -> list[dict]:
    """
    Scrapes all pages of the BuiltWith French eCommerce top-sites list.

    URL: https://builtwith.com/top-sites/France/eCommerce?p={page}

    Each page contains ~50 ranked domains with metadata:
    rank, domain, sales_revenue, tech_spend, social_followers, traffic_tier.

    Stops when:
    - A page returns 404 or empty table
    - No "Next" pagination link found
    - max_pages reached

    Returns a deduplicated list of domain records sorted by rank.
    """
    log.info("Starting BuiltWith scrape — French Top eCommerce")
    log.info(f"  URL   : https://builtwith.com/top-sites/France/eCommerce")
    log.info(f"  Pages : up to {max_pages}")

    all_records  = []
    seen_domains = set()

    for page in range(1, max_pages + 1):
        log.info(f"\n  Page {page}/{max_pages}")

        soup = fetch_page(client, page)

        if soup is None:
            log.info(f"  Page {page}: no response — stopping")
            break

        records = parse_table_rows(soup)

        if not records:
            log.info(f"  Page {page}: empty table — stopping")
            break

        # Deduplicate across pages
        new_records = []
        for rec in records:
            if rec["domain"] not in seen_domains:
                seen_domains.add(rec["domain"])
                new_records.append(rec)

        all_records.extend(new_records)
        log.info(f"  Page {page}: +{len(new_records)} domains (total: {len(all_records)})")

        # Check if this is the last page
        if is_last_page(soup, page):
            log.info(f"  Page {page}: last page detected — stopping")
            break

        # Polite delay
        delay = random.uniform(*DELAY_BETWEEN_PAGES)
        log.info(f"  Waiting {delay:.1f}s before next page...")
        time.sleep(delay)

    log.info(f"\n  ✅ Total domains scraped: {len(all_records)}")
    return all_records
