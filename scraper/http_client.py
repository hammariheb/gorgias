import json
import logging
import random
import re
import time

import httpx
from bs4 import BeautifulSoup

from scraper.config import USER_AGENTS

log = logging.getLogger(__name__)

TRUSTPILOT_BASE = "https://www.trustpilot.com"


def build_headers() -> dict:
    return {
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Cache-Control":             "max-age=0",
    }


def _extract_slug_from_url(url: str) -> str:
    """
    Extracts the Trustpilot slug from a full URL string.
    "https://www.trustpilot.com/review/sezane.com?page=2" → "sezane.com"
    """
    path = url.split("trustpilot.com/review/")[-1]
    return path.split("?")[0].strip("/")


def _parse_location(location: str, current_slug: str) -> tuple[str, str]:
    """
    Parse le header Location d'un 308 Trustpilot.
    Retourne (base_url, slug).

    Exemples :
      'https://fr.trustpilot.com/review/sezane.com'  → ('https://www.trustpilot.com', 'sezane.com')
      '/review/sezane.com'                            → ('https://www.trustpilot.com', 'sezane.com')
    """
    match = re.search(r"/review/([^/?#\s]+)", location)
    slug  = match.group(1) if match else current_slug
    # Always normalize to www.trustpilot.com even if redirect points to fr./de./etc.
    return TRUSTPILOT_BASE, slug


def fetch_next_data(
    client:  httpx.Client,
    domain:  str,
    page:    int,
    retries: int = 3,
) -> tuple[dict | None, str]:
    """
    Fetches a Trustpilot review page and returns (__NEXT_DATA__, slug_used).

    Handles:
    - 308 redirects (slug normalization by Trustpilot, language redirects)
    - 403/429 rate limiting with exponential backoff
    - 404 domain not found
    """
    active_slug   = domain
    base_url      = TRUSTPILOT_BASE
    max_redirects = 3
    redirects     = 0

    for attempt in range(1, retries + 1):
        url = f"{base_url}/review/{active_slug}?page={page}&sort=recency"

        try:
            resp = client.get(url, headers=build_headers(), timeout=20)

            # ── 308 : follow manually ─────────────────────────────
            if resp.status_code == 308:
                if redirects >= max_redirects:
                    log.warning(f"  [{domain}] Too many redirects — aborting")
                    return None, active_slug

                location           = resp.headers.get("location", "")
                new_base, new_slug = _parse_location(location, active_slug)

                log.info(f"  [{domain}] 308 → {location}")
                base_url    = new_base
                active_slug = new_slug
                redirects  += 1
                continue

            # ── 404 : not on Trustpilot ───────────────────────────
            if resp.status_code == 404:
                log.info(f"  [{domain}] 404 — not listed on Trustpilot (slug: {active_slug})")
                return None, active_slug

            # ── 403/429 : rate limiting ───────────────────────────
            if resp.status_code in (403, 429):
                wait = 2 ** attempt * 10   # 20s → 40s → 80s
                log.warning(f"  [{domain}] {resp.status_code} — waiting {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            # ── 200 : parse __NEXT_DATA__ ─────────────────────────
            soup = BeautifulSoup(resp.text, "html.parser")
            tag  = soup.find("script", {"id": "__NEXT_DATA__"})

            if not tag:
                log.warning(f"  [{domain}] __NEXT_DATA__ not found on page {page}")
                return None, active_slug

            # FIX 1 — resp.url is httpx.URL not str → cast to str
            # FIX 2 — tag.string is str | None → guard before json.loads
            content = tag.string
            if not content:
                log.warning(f"  [{domain}] __NEXT_DATA__ is empty on page {page}")
                return None, active_slug

            return json.loads(content), active_slug

        except httpx.TimeoutException:
            log.warning(f"  [{domain}] Timeout page {page} (attempt {attempt}/{retries})")
            time.sleep(5 * attempt)
            redirects = 0
        except json.JSONDecodeError:
            log.error(f"  [{domain}] Invalid JSON page {page}")
            return None, active_slug
        except Exception as e:
            log.error(f"  [{domain}] Error page {page}: {e}")
            if attempt == retries:
                return None, active_slug
            time.sleep(3)

    return None, active_slug


def search_trustpilot(
    client: httpx.Client,
    domain: str,
) -> str | None:
    """
    Fallback: searches Trustpilot for a domain when fetch_next_data returns None.
    Returns the real Trustpilot slug or None.
    """
    search_url = f"{TRUSTPILOT_BASE}/search?query={domain}"

    try:
        resp = client.get(search_url, headers=build_headers(), timeout=15)

        # Handle 308 on search page too
        if resp.status_code == 308:
            location      = resp.headers.get("location", "")
            _, found_slug = _parse_location(location, domain)
            if found_slug and found_slug != domain:
                log.info(f"  [{domain}] Search 308 → slug: {found_slug}")
                return found_slug
            return None

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        tag  = soup.find("script", {"id": "__NEXT_DATA__"})
        if not tag:
            return None

        # FIX 3 — tag.string is str | None → guard before json.loads
        content = tag.string
        if not content:
            return None

        data    = json.loads(content)
        props   = data.get("props", {}).get("pageProps", {})
        results = props.get("businesses", [])

        if not results:
            return None

        first       = results[0]
        tp_url      = first.get("websiteUrl", "")
        tp_name     = first.get("displayName", "")
        domain_base = domain.replace("www.", "").split(".")[0]

        if domain_base.lower() in tp_url.lower() or domain_base.lower() in tp_name.lower():
            profile_url = first.get("links", {}).get("profileUrl", "")
            tp_slug     = profile_url.strip("/").replace("review/", "")
            log.info(f"  [{domain}] Found via search → slug: {tp_slug}")
            return tp_slug

        return None

    except Exception as e:
        log.debug(f"  [{domain}] Search error: {e}")
        return None