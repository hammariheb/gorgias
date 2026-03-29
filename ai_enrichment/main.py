import argparse
import logging
import sys
import time

from openai import OpenAI
from tqdm import tqdm

from ai_enrichment import config
from ai_enrichment.bigquery_client import (
    get_client,
    ensure_enriched_table,
    load_reviews_to_enrich,
    upload_enriched_rows,
)
from ai_enrichment.enricher import call_llm_batch, merge_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrichment.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 3 — AI Enrichment | Gorgias")
    parser.add_argument("--limit",  type=int, default=None, help="Limiter à N reviews (test)")
    parser.add_argument("--domain", default=None,           help="Enrichir un seul domaine")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    log.info("╔═══════════════════════════════════════════════╗")
    log.info("║                AI Enrichment                  ║")
    log.info(f"║   Modèle : {config.MODEL:<36}║")
    log.info(f"║   Batch  : {config.BATCH_SIZE} reviews / appel API              ║")
    log.info("╚═══════════════════════════════════════════════╝")

    # 1. Connexion BQ + création dataset/table si absents
    bq_client = get_client()
    ensure_enriched_table(bq_client)   # ← crée dataset ET table

    # 2. Charger les reviews non encore enrichies
    log.info("\n[1/3] Chargement des reviews à enrichir...")
    reviews = load_reviews_to_enrich(
        bq_client,
        limit=args.limit,
        domain=args.domain,
    )

    if not reviews:
        log.info("  Toutes les reviews sont déjà enrichies.")
        return

    # 3. Enrichissement par batch + upload immédiat
    log.info(f"\n[2/3] Enrichissement de {len(reviews)} reviews "
             f"en batches de {config.BATCH_SIZE}...")

    openai_client = OpenAI(api_key=config.OPENAI_API_KEY)
    batches       = [
        reviews[i : i + config.BATCH_SIZE]
        for i in range(0, len(reviews), config.BATCH_SIZE)
    ]
    total_done = 0

    for batch in tqdm(batches, desc="Batches", unit="batch"):
        enrichments = call_llm_batch(openai_client, batch)
        merged      = merge_results(batch, enrichments)
        upload_enriched_rows(bq_client, merged)
        total_done += len(merged)
        time.sleep(config.DELAY_BETWEEN_BATCHES)

    # 4. Résumé
    log.info(f"\n[3/3] Terminé !")
    log.info(f"  Reviews enrichies : {total_done:,}")
    log.info(f"  Table BQ          : {config.BQ_PROJECT}.{config.REVIEWS_DATASET}.{config.ENRICHED_TABLE}")


if __name__ == "__main__":
    main()