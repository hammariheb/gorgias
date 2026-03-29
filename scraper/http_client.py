# scraper/http_client.py
# =======================
# Construction des headers HTTP et fetch des pages Trustpilot.
#
# Stratégie finale :
#   - follow_redirects=True → httpx suit les 308 automatiquement
#   - On lit l'URL finale pour extraire le slug réel si Trustpilot a redirigé
#   - 404 sur l'URL finale = domaine vraiment introuvable

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


def _extract_slug_from_url(url: str) -> str | None:
    """Extrait le slug depuis une URL Trustpilot finale."""
    match = re.search(r"trustpilot\.com/review/([^/?#\s]+)", str(url))
    return match.group(1) if match else None


def fetch_next_data(
    client:  httpx.Client,
    domain:  str,
    page:    int,
    retries: int = 3,
) -> tuple[dict | None, str]:
    """
    Fetche une page Trustpilot avec follow_redirects=True.
    Retourne (__NEXT_DATA__, slug_final).

    httpx suit automatiquement les 308.
    On lit resp.url (URL finale après redirections) pour extraire le vrai slug.
    """
    url = f"{TRUSTPILOT_BASE}/review/{domain}?page={page}&sort=recency"

    for attempt in range(1, retries + 1):
        try:
            resp = client.get(url, headers=build_headers(), timeout=20)

            # Extraire le slug depuis l'URL finale (après redirections)
            final_slug = _extract_slug_from_url(resp.url) or domain
            if final_slug != domain:
                log.info(f"  [{domain}] Redirigé → slug réel : {final_slug}")

            if resp.status_code == 404:
                log.info(f"  [{domain}] 404 — non référencé sur Trustpilot")
                return None, final_slug

            if resp.status_code in (403, 429):
                wait = 2 ** attempt * 10
                log.warning(f"  [{domain}] {resp.status_code} — pause {wait}s (essai {attempt}/{retries})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            tag  = soup.find("script", {"id": "__NEXT_DATA__"})

            if not tag:
                log.warning(f"  [{domain}] __NEXT_DATA__ absent page {page}")
                return None, final_slug

            return json.loads(tag.string), final_slug

        except httpx.TimeoutException:
            log.warning(f"  [{domain}] Timeout page {page} (essai {attempt}/{retries})")
            time.sleep(5 * attempt)
        except json.JSONDecodeError:
            log.error(f"  [{domain}] JSON invalide page {page}")
            return None, domain
        except Exception as e:
            log.error(f"  [{domain}] Erreur page {page}: {e}")
            if attempt == retries:
                return None, domain
            time.sleep(3)

    return None, domain


def search_trustpilot(
    client: httpx.Client,
    domain: str,
) -> str | None:
    """
    Fallback : cherche le marchand via la page de recherche Trustpilot.
    """
    search_url = f"{TRUSTPILOT_BASE}/search?query={domain}"

    try:
        resp = client.get(search_url, headers=build_headers(), timeout=15)

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        tag  = soup.find("script", {"id": "__NEXT_DATA__"})
        if not tag:
            return None

        data    = json.loads(tag.string)
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
            log.info(f"  [{domain}] Trouvé via search → slug: {tp_slug}")
            return tp_slug

        return None

    except Exception as e:
        log.debug(f"  [{domain}] Search error: {e}")
        return None