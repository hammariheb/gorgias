# scraper/scraper.py
# ===================
# Logique de scraping par domaine — boucle unique propre.

import logging
import random
import time

import httpx

from scraper.config import MAX_PAGES_PER_DOMAIN, DELAY_BETWEEN_PAGES
from scraper.http_client import fetch_next_data, search_trustpilot
from scraper.parser import parse_review, extract_reviews_and_pagination

log = logging.getLogger(__name__)


def _resolve_slug(http_client: httpx.Client, domain: str) -> tuple[str, dict | None]:
    """
    Résout le slug Trustpilot réel pour un domaine.

    Retourne (trustpilot_slug, first_page_data).
    first_page_data est None si introuvable.

    Stratégie :
      1. fetch_next_data gère le 308 automatiquement et retourne le bon slug
      2. Si toujours None → fallback search Trustpilot
    """
    # fetch_next_data retourne (data, slug_utilisé)
    data, resolved_slug = fetch_next_data(http_client, domain, page=1)

    if data is not None:
        # Succès direct ou via 308 auto-résolu
        if resolved_slug != domain:
            log.info(f"  [{domain}] Slug résolu : {resolved_slug}")
        return resolved_slug, data

    # Fallback : recherche Trustpilot
    log.info(f"  [{domain}] Fallback search Trustpilot...")
    found_slug = search_trustpilot(http_client, domain)

    if found_slug:
        data, final_slug = fetch_next_data(http_client, found_slug, page=1)
        if data is not None:
            return final_slug, data

    log.info(f"  [{domain}] Introuvable sur Trustpilot — skip")
    return domain, None


def scrape_domain(
    http_client:       httpx.Client,
    domain:            str,
    last_scraped_date: str | None = None,
) -> list[dict]:
    """
    Scrape toutes les reviews d'un domaine en une seule boucle propre.
    Conserve le domain original pour le JOIN BQ avec leads_table.
    """
    mode = f"incrémental depuis {last_scraped_date}" if last_scraped_date else "full"
    log.info(f"🔍 {domain}  [{mode}]")

    # ── 1. Résoudre le slug Trustpilot ───────────────────────
    trustpilot_slug, first_page = _resolve_slug(http_client, domain)
    if first_page is None:
        return []

    # ── 2. Lire le total de pages depuis page 1 ──────────────
    _, total_pages  = extract_reviews_and_pagination(first_page)
    pages_to_scrape = min(total_pages, MAX_PAGES_PER_DOMAIN)

    if trustpilot_slug != domain:
        log.info(f"  [{domain}] Scraping sous slug '{trustpilot_slug}' — {pages_to_scrape}/{total_pages} pages")
    else:
        log.info(f"  [{domain}] {pages_to_scrape}/{total_pages} pages à scraper")

    # ── 3. Boucle unique sur toutes les pages ─────────────────
    all_reviews = []
    seen_ids    = set()

    for page in range(1, pages_to_scrape + 1):

        # Réutiliser page 1 déjà fetchée — pas de requête redondante
        if page == 1:
            data = first_page
        else:
            data, _ = fetch_next_data(http_client, trustpilot_slug, page)

        if data is None:
            log.warning(f"  [{domain}] Page {page} inaccessible — arrêt")
            break

        raw_reviews, _ = extract_reviews_and_pagination(data)

        if not raw_reviews:
            log.info(f"  [{domain}] Page {page}: vide — fin pagination")
            break

        stop_early  = False
        parsed_page = []

        for raw in raw_reviews:
            parsed = parse_review(raw, domain, trustpilot_slug)

            if not parsed or parsed["review_id"] in seen_ids:
                continue

            # Mode incrémental
            if last_scraped_date and parsed.get("date_published"):
                if parsed["date_published"] <= last_scraped_date:
                    log.info(f"  [{domain}] Date ≤ {last_scraped_date} — arrêt incrémental")
                    stop_early = True
                    break

            seen_ids.add(parsed["review_id"])
            parsed_page.append(parsed)

        all_reviews.extend(parsed_page)
        log.info(f"  [{domain}] Page {page}/{pages_to_scrape}: +{len(parsed_page)} (total: {len(all_reviews)})")

        if stop_early:
            break

        if page < pages_to_scrape:
            time.sleep(random.uniform(*DELAY_BETWEEN_PAGES))

    slug_info = f" via '{trustpilot_slug}'" if trustpilot_slug != domain else ""
    log.info(f"✅ {domain}{slug_info}: {len(all_reviews)} reviews")
    return all_reviews