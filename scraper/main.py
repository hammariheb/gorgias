# scraper/main.py
# CLI entry point — supports two independent scraping pipelines
#
# Usage:
#   python -m scraper.main                    # original Gorgias pipeline
#   python -m scraper.main --source fr        # French eCommerce reference
#   python -m scraper.main --source fr --limit 50
#   python -m scraper.main --source fr --start-from cartier.com

import argparse
import logging
import random
import sys
import time

import httpx
from tqdm import tqdm

from .bq_client import get_client, ensure_reviews_table, load_domains, upload_reviews
from .scraper import scrape_domain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SOURCE_LABELS = {
    "default": "Gorgias leads            → reviews.reviews_raw",
    "fr":      "BuiltWith FR eCommerce   → reviews.reviews_raw_fr",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Trustpilot scraper")
    parser.add_argument(
        "--source",
        choices=["default", "fr"],
        default="default",
        help="Which leads source to scrape (default or fr)",
    )
    parser.add_argument("--limit",      type=int, default=None, help="Max domains to scrape")
    parser.add_argument("--start-from", type=str, default=None, help="Resume from this domain")
    args = parser.parse_args()

    source_label = SOURCE_LABELS[args.source]

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  Trustpilot Scraper                                  ║")
    log.info("╠══════════════════════════════════════════════════════╣")
    log.info(f"║  Source : {source_label:<43}║")
    log.info(f"║  Limit  : {str(args.limit or 'none'):<43}║")
    log.info(f"║  Resume : {str(args.start_from or 'from beginning'):<43}║")
    log.info("╚══════════════════════════════════════════════════════╝")

    # ── 1. BigQuery ───────────────────────────────────────────
    log.info("\n[1/4] Connecting to BigQuery...")
    bq = get_client()
    ensure_reviews_table(bq, source=args.source)

    # ── 2. Load domains ───────────────────────────────────────
    log.info(f"\n[2/4] Loading domains...")
    domains = load_domains(
        bq,
        source=args.source,
        limit=args.limit,
        start_from=args.start_from,
    )

    if not domains:
        log.info("No domains to scrape.")
        return

    log.info(f"  {len(domains)} domains to scrape")

    # ── 3. Scrape ─────────────────────────────────────────────
    log.info(f"\n[3/4] Scraping Trustpilot ({len(domains)} domains)...")

    total_reviews  = 0
    failed_domains = []
    stats          = {}

    with httpx.Client(
        timeout=httpx.Timeout(20.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
    ) as http_client:

        for i, domain in enumerate(tqdm(domains, desc="Domains", unit="domain"), 1):
            try:
                # scrape_domain takes (http_client, domain, last_scraped_date)
                # No full_scrape argument in this version of scraper.py
                reviews       = scrape_domain(http_client, domain)
                stats[domain] = len(reviews)

                if reviews:
                    upload_reviews(bq, reviews, source=args.source)
                    total_reviews += len(reviews)

            except KeyboardInterrupt:
                log.warning(f"\n⚠️  Interrupted — resume with: --source {args.source} --start-from {domain}")
                break
            except Exception as e:
                log.error(f"  ❌ {domain}: {e}")
                failed_domains.append(domain)
                continue

            if i < len(domains):
                time.sleep(random.uniform(3.0, 6.0))

    # ── 4. Summary ────────────────────────────────────────────
    target_table = "reviews_raw_fr" if args.source == "fr" else "reviews_raw"

    log.info(f"\n[4/4] Done!")
    log.info(f"  Source         : {source_label}")
    log.info(f"  Target table   : reviews.{target_table}")
    log.info(f"  Total reviews  : {total_reviews:,}")
    log.info(f"  Domains found  : {sum(1 for v in stats.values() if v > 0)}/{len(stats)}")
    log.info(f"  Domains failed : {len(failed_domains)}")

    if failed_domains:
        log.warning(f"  Failed: {failed_domains}")

    log.info(f"\n  Top domains by review count:")
    for domain, count in sorted(stats.items(), key=lambda x: -x[1])[:15]:
        bar    = "█" * min(count // 5, 30)
        status = "✅" if count > 0 else "⚪ not found"
        log.info(f"    {domain:<45} {count:>4}  {bar} {status}")

    if args.source == "fr":
        log.info("\n  Next step → dbt build --select stg_reviews_fr")
    else:
        log.info("\n  Next step → python -m ai_enrichment.main")


if __name__ == "__main__":
    main()