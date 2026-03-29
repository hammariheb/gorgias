import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def safe_str(val, max_len: int = 10_000) -> str | None:
    """Strip, supprime les null bytes, tronque."""
    if val is None:
        return None
    s = str(val).strip().replace("\x00", "")
    return s[:max_len] if s else None


def detect_language(text: str) -> str:
    """
    Détecte la langue ISO 639-1 du texte.
    Utile pour filtrer les reviews non-anglaises en step 2 (dbt).
    """
    if not text or len(text.strip()) < 15:
        return "unknown"
    try:
        from langdetect import detect
        return detect(text)
    except Exception:
        return "unknown"


def parse_review(
    raw:             dict,
    domain:          str,
    trustpilot_slug: str | None = None,
) -> dict | None:
    """
    Transforme un bloc review brut (JSON Trustpilot) en dict BQ-ready.

    Args:
        raw             : bloc review brut depuis __NEXT_DATA__
        domain          : domaine original du lead (pour le JOIN BQ avec leads_table)
        trustpilot_slug : slug réellement utilisé sur Trustpilot (peut différer de domain)

    Structure Trustpilot :
    {
      "id":       "64abc...",
      "text":     "Great product...",
      "title":    "Love it!",
      "rating":   5,
      "dates":    { "publishedDate": "2024-03-01T10:00:00.000Z" },
      "consumer": { "displayName": "John D." },
      "reply":    { "message": "Thank you!" }
    }
    """
    try:
        review_text  = safe_str(raw.get("text"))
        review_title = safe_str(raw.get("title"))
        star_rating  = raw.get("rating")

        # Rating requis et dans [1, 5]
        if star_rating is None:
            return None
        star_rating = int(star_rating)
        if star_rating not in range(1, 6):
            return None

        # Date → YYYY-MM-DD
        dates          = raw.get("dates") or {}
        raw_date       = dates.get("publishedDate") or dates.get("updatedDate") or ""
        date_published = raw_date[:10] if raw_date else None

        # Reviewer
        consumer      = raw.get("consumer") or {}
        reviewer_name = safe_str(consumer.get("displayName")) or "Anonymous"

        # Réponse marchand
        reply           = raw.get("reply")
        company_replied = bool(reply and reply.get("message"))

        # Langue
        language = detect_language(review_text or review_title or "")

        return {
            "domain":           domain,                              # domaine lead → JOIN BQ
            "trustpilot_slug":  trustpilot_slug or domain,          # slug réel Trustpilot
            "review_id":        safe_str(raw.get("id"), max_len=100),
            "review_text":      review_text,
            "review_title":     review_title,
            "star_rating":      star_rating,
            "date_published":   date_published,
            "reviewer_name":    reviewer_name,
            "company_replied":  company_replied,
            "language":         language,
            "ingested_at":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    except Exception as e:
        log.debug(f"  parse_review error: {e}")
        return None


def extract_reviews_and_pagination(data: dict) -> tuple[list, int]:
    """
    Extrait les reviews brutes + nombre total de pages depuis __NEXT_DATA__.
    Teste plusieurs chemins car Trustpilot change parfois sa structure.
    """
    page_props = data.get("props", {}).get("pageProps", {})

    raw_reviews = (
        page_props.get("reviews") or
        page_props.get("businessUnit", {}).get("reviews") or
        []
    )

    pagination  = page_props.get("pagination") or {}
    total_pages = int(
        pagination.get("totalPages") or
        pagination.get("lastPage") or 1
    )

    return raw_reviews, total_pages