import json
import logging
import time
from datetime import datetime, timezone

from openai import OpenAI
from google.cloud import bigquery

from .bigquery_client import upload_enriched_rows
from .prompts import SYSTEM_PROMPT, build_user_prompt
from .config import OPENAI_MODEL, BATCH_SIZE, DELAY_BETWEEN_BATCHES

log = logging.getLogger(__name__)

VALID_SENTIMENTS = {"positive", "neutral", "negative"}
VALID_CATEGORIES = {
    "customer_support", "shipping", "product_quality", "pricing",
    "ux", "returns", "packaging", "communication", "stock", "loyalty", "other",
}


def _call_openai(client: OpenAI, reviews_batch: list[dict]) -> str:
    """Calls OpenAI with a batch of reviews. Returns the raw JSON string."""
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": build_user_prompt(reviews_batch)},
        ],
        response_format={"type": "json_object"},
        temperature=0,
        max_tokens=1000,
    )
    return response.choices[0].message.content or ""


def _validate(result: dict) -> dict:
    """Normalizes OpenAI output — ensures no invalid values reach BigQuery."""
    if result.get("sentiment") not in VALID_SENTIMENTS:
        result["sentiment"] = "neutral"
    if result.get("category") not in VALID_CATEGORIES:
        result["category"] = "other"
    if not result.get("actionable_insight"):
        result["actionable_insight"] = "Review customer feedback manually."
    return result


def _fallback(review: dict) -> dict:
    """Star-rating based fallback when OpenAI fails completely."""
    star = int(review.get("star_rating") or 3)
    return {
        "review_id":          review["review_id"],
        "domain":             review.get("domain", ""),
        "sentiment":          "positive" if star >= 4 else "negative" if star <= 2 else "neutral",
        "category":           "other",
        "pain_point":         None if star >= 4 else "Unable to enrich — fallback",
        "actionable_insight": "Review this feedback manually.",
        "model_used":         "fallback",
        "enriched_at":        datetime.now(timezone.utc).isoformat(),
    }


def _parse_batch(raw_content: str, reviews_batch: list[dict]) -> list[dict]:
    """Parses OpenAI JSON response and maps results back to review_ids."""
    enriched = []
    now      = datetime.now(timezone.utc).isoformat()

    try:
        data    = json.loads(raw_content)
        results = data.get("results", [])
    except (json.JSONDecodeError, AttributeError):
        log.error("  Failed to parse OpenAI response — using fallback for entire batch")
        return [_fallback(r) for r in reviews_batch]

    results_by_id = {r["review_id"]: r for r in results if "review_id" in r}

    for review in reviews_batch:
        rid    = review["review_id"]
        result = results_by_id.get(rid)

        if result is None:
            log.warning(f"  Missing result for review_id={rid} — using fallback")
            enriched.append(_fallback(review))
            continue

        validated = _validate(result)
        enriched.append({
            "review_id":          rid,
            "domain":             review.get("domain", ""),
            "sentiment":          validated["sentiment"],
            "category":           validated["category"],
            "pain_point":         validated.get("pain_point"),
            "actionable_insight": validated["actionable_insight"],
            "model_used":         OPENAI_MODEL,
            "enriched_at":        now,
        })

    return enriched


def enrich_batch(
    openai_client: OpenAI,
    bq_client:     bigquery.Client,
    reviews:       list[dict],
    source:        str = "default",
) -> None:
    """
    Processes all reviews in batches of BATCH_SIZE.
    Uploads immediately after each batch — resilient to crashes.
    source is passed to upload_enriched_rows to target the correct table.
    """
    total   = len(reviews)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    log.info(f"  Processing {total} reviews in {batches} batches of {BATCH_SIZE}")

    for i in range(0, total, BATCH_SIZE):
        batch      = reviews[i:i + BATCH_SIZE]
        batch_num  = i // BATCH_SIZE + 1

        log.info(f"  Batch {batch_num}/{batches} ({len(batch)} reviews)")

        try:
            raw     = _call_openai(openai_client, batch)
            results = _parse_batch(raw, batch)
        except Exception as e:
            log.error(f"  Batch {batch_num} failed: {e} — using fallback")
            results = [_fallback(r) for r in batch]

        # Upload immediately — already processed reviews are safe even if script crashes
        upload_enriched_rows(bq_client, results, source=source)

        if i + BATCH_SIZE < total:
            time.sleep(DELAY_BETWEEN_BATCHES)