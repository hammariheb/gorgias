import argparse
import logging
import sys
from pathlib import Path

import httpx
import pandas as pd

from .config import MAX_PAGES, OUTPUT_CSV, OUTPUT_SEED_CSV, SEED_COLUMNS
from .scraper import scrape_builtwith_france

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


def _save(records: list[dict], path: str, encoding: str = "utf-8") -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in SEED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = (
        df[SEED_COLUMNS]
        .sort_values("rank", na_position="last")
        .drop_duplicates(subset="domain")
        .reset_index(drop=True)
    )
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding=encoding)
    log.info(f"  Saved {len(df)} domains → {path}")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape French top eCommerce domains from BuiltWith")
    parser.add_argument("--pages",   type=int, default=MAX_PAGES,       help="Pages to scrape")
    parser.add_argument("--output",  type=str, default=OUTPUT_CSV,      help="Output CSV path")
    parser.add_argument("--seed",    type=str, default=OUTPUT_SEED_CSV, help="dbt seed CSV path")
    parser.add_argument("--no-seed", action="store_true",               help="Skip dbt seed CSV")
    args = parser.parse_args()

    log.info("╔══════════════════════════════════════════╗")
    log.info("║  BuiltWith Collector — French eCommerce  ║")
    log.info("╚══════════════════════════════════════════╝")
    log.info(f"  Pages : up to {args.pages}")

    with httpx.Client(
        timeout=httpx.Timeout(20.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
    ) as client:
        records = scrape_builtwith_france(client, max_pages=args.pages)

    if not records:
        log.error("No domains scraped.")
        sys.exit(1)

    df = _save(records, args.output, encoding="utf-8-sig")

    if not args.no_seed:
        _save(records, args.seed)

    log.info(f"\n  Done — {len(df)} domains scraped")
    log.info(f"  CSV  : {args.output}")
    if not args.no_seed:
        log.info(f"  Seed : {args.seed}")
    log.info("\n  Top 10:")
    for _, row in df.head(10).iterrows():
        log.info(f"    #{row['rank']:>3}  {row['domain']:<35}  {row['traffic_tier'] or '—'}")


if __name__ == "__main__":
    main()
