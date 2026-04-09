import random
import logging
import time

import httpx
from bs4 import BeautifulSoup

from .config import USER_AGENTS, BUILTWITH_BASE_URL, DELAY_BETWEEN_PAGES

log = logging.getLogger(__name__)


def build_headers() -> dict:
    """
    Browser-like headers to avoid being blocked by BuiltWith.
    Rotates User-Agent on every request.
    """
    return {
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer":                   "https://builtwith.com/top-sites",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "same-origin",
        "Cache-Control":             "max-age=0",
    }


def fetch_page(
    client:  httpx.Client,
    page:    int,
    retries: int = 3,
) -> BeautifulSoup | None:
    """
    Fetches a single BuiltWith top-sites page and returns its parsed HTML.

    URL pattern:
        Page 1: https://builtwith.com/top-sites/France/eCommerce
        Page 2: https://builtwith.com/top-sites/France/eCommerce?p=2
        Page 3: https://builtwith.com/top-sites/France/eCommerce?p=3

    Returns a BeautifulSoup object, or None if the page fails or is empty.
    """
    url = BUILTWITH_BASE_URL if page == 1 else f"{BUILTWITH_BASE_URL}?p={page}"

    for attempt in range(1, retries + 1):
        try:
            resp = client.get(url, headers=build_headers(), timeout=20)

            if resp.status_code == 404:
                log.info(f"  Page {page}: 404 — no more pages")
                return None

            if resp.status_code in (429, 403):
                wait = 2 ** attempt * 15   # 30s → 60s → 120s
                log.warning(f"  Page {page}: {resp.status_code} — waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                log.warning(f"  Page {page}: HTTP {resp.status_code}")
                return None

            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")

        except httpx.TimeoutException:
            log.warning(f"  Page {page}: timeout (attempt {attempt}/{retries})")
            time.sleep(5 * attempt)
        except Exception as e:
            log.error(f"  Page {page}: unexpected error — {e}")
            if attempt == retries:
                return None
            time.sleep(3)

    return None
