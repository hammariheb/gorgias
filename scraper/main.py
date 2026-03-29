# scraper/main.py
# =================
# Point d'entrée unique — orchestration pure.

import argparse
import logging
import random
import sys
import time

import httpx
from tqdm import tqdm

from scraper import config
from scraper.bq_client import (
    get_client,
    ensure_reviews_table,
    load_domains,
    upload_reviews,
)
from scraper.scraper import scrape_domain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trustpilot → BigQuery | Gorgias")
    parser.add_argument("--limit",      type=int, default=None)
    parser.add_argument("--start-from", default=None)
    parser.add_argument("--full",       action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.full:
        config.MAX_PAGES_PER_DOMAIN = config.MAX_PAGES_PER_DOMAIN_FULL
        mode_label = f"FULL ({config.MAX_PAGES_PER_DOMAIN_FULL} pages max)"
    else:
        mode_label = f"STANDARD ({config.MAX_PAGES_PER_DOMAIN} pages max)"

    log.info("╔═══════════════════════════════════════════════╗")
    log.info("║             Trustpilot Scraper                ║")
    log.info(f"║   Mode   : {mode_label:<36}║")
    log.info(f"║   Source : {config.LEADS_DATASET}.{config.LEADS_TABLE:<33}║")
    log.info(f"║   Dest   : {config.REVIEWS_DATASET}.{config.REVIEWS_TABLE:<33}║")
    log.info("╚═══════════════════════════════════════════════╝")

    bq = get_client()
    ensure_reviews_table(bq)

    log.info(f"\nChargement des domaines...")
    domains = load_domains(bq, limit=args.limit, start_from=args.start_from)
    if not domains:
        log.error("Aucun domaine trouvé")
        sys.exit(1)

    log.info(f"\nScraping {len(domains)} domaines...\n")
    total  = 0
    failed = []
    stats  = {}

    with httpx.Client(
        timeout=httpx.Timeout(20.0),
        follow_redirects=True,         
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
    ) as http_client:

        for i, domain in enumerate(tqdm(domains, desc="Domaines", unit="domain"), 1):
            try:
                reviews       = scrape_domain(http_client, domain)
                stats[domain] = len(reviews)
                if reviews:
                    upload_reviews(bq, reviews)
                    total += len(reviews)
            except KeyboardInterrupt:
                log.warning(f"\n⚠️  Interruption — reprendre avec : --start-from {domain}")
                break
            except Exception as e:
                log.error(f"❌ {domain}: {e}")
                failed.append(domain)
                continue

            if i < len(domains):
                time.sleep(random.uniform(*config.DELAY_BETWEEN_DOMAINS))

    found     = sum(1 for c in stats.values() if c > 0)
    not_found = sum(1 for c in stats.values() if c == 0)

    log.info("\n" + "═" * 50)
    log.info(f"  Reviews extraites  : {total:,}")
    log.info(f"  Trouvés Trustpilot : {found}/{len(domains)}")
    log.info(f"  Non trouvés        : {not_found}")
    log.info(f"  Erreurs            : {len(failed)}")
    log.info("═" * 50)

    for domain, count in sorted(stats.items(), key=lambda x: -x[1]):
        status = "✅" if count > 0 else "⚪ non trouvé"
        log.info(f"  {domain:<45} {count:>4}  {status}")


if __name__ == "__main__":
    main()