import argparse
import logging
import sys

import httpx

from .scraper import scrape_builtwith_france
from .exporter import save_csv, save_seed_csv
from .config import OUTPUT_CSV, OUTPUT_SEED_CSV, MAX_PAGES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("builtwith_collector.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape French Top eCommerce domains from BuiltWith"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=MAX_PAGES,
        help=f"Number of pages to scrape (default: {MAX_PAGES}, ~50 domains/page)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=OUTPUT_CSV,
        help=f"Output CSV path (default: {OUTPUT_CSV})",
    )
    parser.add_argument(
        "--seed",
        type=str,
        default=OUTPUT_SEED_CSV,
        help=f"dbt seed CSV path (default: {OUTPUT_SEED_CSV})",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip saving the dbt seed CSV",
    )
    args = parser.parse_args()

    log.info("╔══════════════════════════════════════════╗")
    log.info("║  BuiltWith Collector — French eCommerce  ║")
    log.info("╚══════════════════════════════════════════╝")
    log.info(f"  Source : builtwith.com/top-sites/France/eCommerce")
    log.info(f"  Pages  : up to {args.pages}")

    # ── Scrape ────────────────────────────────────────────────
    with httpx.Client(
        timeout=httpx.Timeout(20.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
    ) as client:
        records = scrape_builtwith_france(client, max_pages=args.pages)

    if not records:
        log.error("No domains scraped. Check your network connection or try again.")
        sys.exit(1)

    # ── Save CSV ──────────────────────────────────────────────
    df = save_csv(records, args.output)

    # ── Save dbt seed ─────────────────────────────────────────
    if not args.no_seed:
        save_seed_csv(records, args.seed)

    # ── Summary ───────────────────────────────────────────────
    log.info("\n" + "=" * 50)
    log.info("✅ Done!")
    log.info(f"   Domains scraped : {len(df)}")
    log.info(f"   Output CSV      : {args.output}")
    if not args.no_seed:
        log.info(f"   dbt seed        : {args.seed}")
    log.info("\nTop 10 domains by rank:")
    for _, row in df.head(10).iterrows():
        log.info(
            f"   #{row['rank']:>3}  {row['domain']:<35}"
            f"  revenue={row['sales_revenue'] or '—':>8}"
            f"  traffic={row['traffic_tier'] or '—'}"
        )
    log.info("\nNext steps:")
    log.info("  1. Run dbt seed : cd dbt_transformation && dbt seed")
    log.info("  2. Run dbt build: dbt build --no-partial-parse")


if __name__ == "__main__":
    main()
