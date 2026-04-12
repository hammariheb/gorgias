import logging
import random
import re
import time

import httpx
from bs4 import BeautifulSoup

from .config import BUILTWITH_BASE_URL, DELAY_BETWEEN_PAGES, MAX_PAGES, USER_AGENTS

log = logging.getLogger(__name__)


# ── HTTP ──────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer":                   "https://builtwith.com/top-sites",
        "Cache-Control":             "max-age=0",
    }


def _fetch_page(client: httpx.Client, page: int, retries: int = 3) -> BeautifulSoup | None:
    url = BUILTWITH_BASE_URL if page == 1 else f"{BUILTWITH_BASE_URL}?p={page}"

    for attempt in range(1, retries + 1):
        try:
            resp = client.get(url, headers=_headers(), timeout=20)

            if resp.status_code == 404:
                return None

            if resp.status_code in (429, 403):
                wait = 2 ** attempt * 15
                log.warning(f"  Page {page}: {resp.status_code} — waiting {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                return None

            return BeautifulSoup(resp.text, "html.parser")

        except httpx.TimeoutException:
            log.warning(f"  Page {page}: timeout (attempt {attempt}/{retries})")
            time.sleep(5 * attempt)
        except Exception as e:
            log.error(f"  Page {page}: {e}")
            if attempt == retries:
                return None
            time.sleep(3)

    return None


# ── Parse ─────────────────────────────────────────────────────

def _parse_page(soup: BeautifulSoup) -> list[dict]:
    records = []
    table   = soup.find("table")
    if not table:
        return records

    tbody = table.find("tbody")
    if not tbody:
        return records

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 9:
            continue
        try:
            domain_raw = cells[2].get_text(strip=True).strip().lower().replace("www.", "").split("/")[0]
            if not re.match(r"^[a-z0-9\-\.]+\.[a-z]{2,}$", domain_raw):
                continue

            rank_raw = cells[0].get_text(strip=True)
            rank     = int(rank_raw) if rank_raw.isdigit() else None

            def clean(val: str) -> str | None:
                v = val.strip()
                return v if v and v != "-" else None

            records.append({
                "rank":             rank,
                "domain":           domain_raw,
                "sales_revenue":    clean(cells[4].get_text(strip=True)),
                "tech_spend":       clean(cells[5].get_text(strip=True)),
                "social_followers": clean(cells[6].get_text(strip=True)),
                "traffic_tier":     clean(cells[8].get_text(strip=True)),
                "country":          "FR",
                "source":           "builtwith_top_ecommerce_fr",
            })
        except Exception:
            continue

    return records


def _is_last_page(soup: BeautifulSoup, page: int) -> bool:
    for link in soup.find_all("a", href=True):
        if f"p={page + 1}" in link.get("href", ""):
            return False
    return True


# ── Orchestrate ───────────────────────────────────────────────

def scrape_builtwith_france(client: httpx.Client, max_pages: int = MAX_PAGES) -> list[dict]:
    """
    Scrapes all pages of the BuiltWith French eCommerce top-sites list.
    Returns a deduplicated list of domain records sorted by rank.
    """
    log.info("BuiltWith scrape — French Top eCommerce")
    log.info(f"  Pages : up to {max_pages}")

    all_records:  list[dict] = []
    seen_domains: set[str]   = set()

    for page in range(1, max_pages + 1):
        soup = _fetch_page(client, page)

        if soup is None:
            log.info(f"  Page {page}: no response — stopping")
            break

        records = _parse_page(soup)

        if not records:
            log.info(f"  Page {page}: empty — stopping")
            break

        new = [r for r in records if r["domain"] not in seen_domains]
        seen_domains.update(r["domain"] for r in new)
        all_records.extend(new)
        log.info(f"  Page {page}: +{len(new)} domains (total: {len(all_records)})")

        if _is_last_page(soup, page):
            break

        time.sleep(random.uniform(*DELAY_BETWEEN_PAGES))

    log.info(f"  Total: {len(all_records)} domains")
    return all_records
