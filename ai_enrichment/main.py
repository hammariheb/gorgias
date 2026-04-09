import argparse
import logging
import sys

from openai import OpenAI

from .bigquery_client import get_client, ensure_enriched_table, load_unenriched_reviews
from .enricher import enrich_batch
from .config import OPENAI_API_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ai_enrichment.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SOURCE_LABELS = {
    "default": "reviews_raw        → reviews_enriched",
    "fr":      "reviews_raw_fr     → reviews_enriched_fr",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="AI enrichment pipeline")
    parser.add_argument(
        "--source",
        choices=["default", "fr"],
        default="default",
        help="Which reviews table to enrich",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max reviews to enrich")
    args = parser.parse_args()

    label = SOURCE_LABELS[args.source]

    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║  AI Enrichment Pipeline                          ║")
    log.info("╠══════════════════════════════════════════════════╣")
    log.info(f"║  Source : {label:<41}║")
    log.info(f"║  Model  : gpt-4o-mini                            ║")
    log.info(f"║  Limit  : {str(args.limit or 'none'):<41}║")
    log.info("╚══════════════════════════════════════════════════╝")

    # ── 1. Init clients ───────────────────────────────────────
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    bq_client     = get_client()

    # ── 2. Ensure target table exists ─────────────────────────
    ensure_enriched_table(bq_client, source=args.source)

    # ── 3. Load unenriched reviews ────────────────────────────
    reviews = load_unenriched_reviews(bq_client, source=args.source, limit=args.limit)

    if not reviews:
        log.info("✅ Everything already enriched. Nothing to do.")
        return

    log.info(f"  {len(reviews)} reviews to enrich")
    log.info(f"  Estimated API calls : {len(reviews) // 10 + 1}")

    # ── 4. Enrich ─────────────────────────────────────────────
    enrich_batch(openai_client, bq_client, reviews, source=args.source)

    log.info("\n✅ Enrichment complete")

    if args.source == "fr":
        log.info("\n  Next step → dbt build --select stg_reviews_enriched_fr+")
    else:
        log.info("\n  Next step → dbt build --select stg_reviews_enriched+")


if __name__ == "__main__":
    main()