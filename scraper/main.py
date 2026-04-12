import argparse
import logging
import random
import sys
import time

import httpx
from tqdm import tqdm

from .bq_client import (
    get_client,
    ensure_reviews_table,
    load_domains,
    get_last_scraped_dates,
    upload_reviews,
)
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
    "default": "Target leads            → reviews.reviews_raw",
    "fr":      "BuiltWith FR eCommerce   → reviews.reviews_raw_fr",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Trustpilot scraper — MerchantRadar")
    parser.add_argument(
        "--source",
        choices=["default", "fr"],
        default="default",
    )
    parser.add_argument("--limit",       type=int,  default=None)
    parser.add_argument("--start-from",  type=str,  default=None)
    parser.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help=(
            "Only scrape reviews newer than the last ingestion date per domain. "
            "Used by Airflow for weekly runs. "
            "New domains always get a full scrape."
        ),
    )
    args = parser.parse_args()

    label = SOURCE_LABELS[args.source]
    mode  = "INCREMENTAL" if args.incremental else "FULL"

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  Trustpilot Scraper — MerchantRadar                  ║")
    log.info("╠══════════════════════════════════════════════════════╣")
    log.info(f"║  Source : {label:<43}║")
    log.info(f"║  Mode   : {mode:<43}║")
    log.info(f"║  Limit  : {str(args.limit or 'none'):<43}║")
    log.info(f"║  Resume : {str(args.start_from or 'from beginning'):<43}║")
    log.info("╚══════════════════════════════════════════════════════╝")

    # ── 1. BigQuery ───────────────────────────────────────────
    bq = get_client()
    ensure_reviews_table(bq, source=args.source)

    # ── 2. Load domains ───────────────────────────────────────
    domains = load_domains(
        bq,
        source=args.source,
        limit=args.limit,
        start_from=args.start_from,
    )
    if not domains:
        log.info("No domains to scrape.")
        return

    # ── 3. Load last scraped dates (incremental mode) ─────────
    last_scraped: dict[str, str] = {}
    if args.incremental:
        last_scraped = get_last_scraped_dates(bq, source=args.source)
        new_domains  = sum(1 for d in domains if d not in last_scraped)
        log.info(
            f"  Incremental mode: {len(last_scraped)} domains with existing data, "
            f"{new_domains} new domains (full scrape)"
        )

    # ── 4. Scrape ─────────────────────────────────────────────
    log.info(f"\n[3/4] Scraping {len(domains)} domains...")

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
                # Pass last_scraped_date for incremental — None means full scrape
                last_date = last_scraped.get(domain) if args.incremental else None

                reviews       = scrape_domain(http_client, domain, last_scraped_date=last_date)
                stats[domain] = len(reviews)

                if reviews:
                    upload_reviews(bq, reviews, source=args.source)
                    total_reviews += len(reviews)

            except KeyboardInterrupt:
                log.warning(
                    f"\n⚠️  Interrupted — resume with: "
                    f"--source {args.source} --start-from {domain}"
                )
                break
            except Exception as e:
                log.error(f"  ❌ {domain}: {e}")
                failed_domains.append(domain)
                continue

            if i < len(domains):
                time.sleep(random.uniform(3.0, 6.0))

    # ── 5. Summary ────────────────────────────────────────────
    target = "reviews_raw_fr" if args.source == "fr" else "reviews_raw"
    log.info("\n[4/4] Done!")
    log.info(f"  Mode           : {mode}")
    log.info(f"  Target table   : reviews.{target}")
    log.info(f"  New reviews    : {total_reviews:,}")
    log.info(f"  Domains found  : {sum(1 for v in stats.values() if v > 0)}/{len(stats)}")
    log.info(f"  Domains failed : {len(failed_domains)}")

    if failed_domains:
        log.warning(f"  Failed: {failed_domains[:10]}")

    log.info("\n  Top domains by new reviews:")
    for domain, count in sorted(stats.items(), key=lambda x: -x[1])[:10]:
        bar = "█" * min(count // 2, 30)
        log.info(f"    {domain:<45} +{count:>3}  {bar}")


if __name__ == "__main__":
    main()