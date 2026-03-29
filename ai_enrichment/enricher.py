import json
import logging
import time
from datetime import datetime, timezone

from openai import OpenAI

from ai_enrichment.config import (
    MODEL,
    TEMPERATURE,
    MAX_TOKENS,
    MAX_RETRIES,
    VALID_SENTIMENTS,
    VALID_CATEGORIES,
)
from ai_enrichment.prompts import SYSTEM_PROMPT, build_user_prompt, fallback_enrichment

log = logging.getLogger(__name__)


def _parse_llm_response(raw_content: str) -> list[dict]:
    """Parse la réponse JSON — gère {"results": [...]} et [...] directement."""
    parsed = json.loads(raw_content)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("results", "reviews", "data"):
            if isinstance(parsed.get(key), list):
                return parsed[key]
        for value in parsed.values():
            if isinstance(value, list):
                return value
    raise ValueError(f"Format inattendu : {type(parsed)}")


def _validate_enrichment(e: dict) -> dict:
    """
    Normalise un enrichissement LLM.
    - pain_point      : null autorisé pour les positifs sans plainte
    - actionable_insight : jamais null
    """
    sentiment = (e.get("sentiment") or "neutral").lower()
    category  = (e.get("category")  or "other").lower()

    pain_point = e.get("pain_point")
    if pain_point is not None and not str(pain_point).strip():
        pain_point = None

    actionable_insight = e.get("actionable_insight")
    if not actionable_insight or not str(actionable_insight).strip():
        actionable_insight = "Maintain current service quality and monitor customer feedback"

    return {
        "review_id":          e.get("review_id"),
        "sentiment":          sentiment if sentiment in VALID_SENTIMENTS else "neutral",
        "category":           category  if category  in VALID_CATEGORIES else "other",
        "pain_point":         pain_point,
        "actionable_insight": actionable_insight,
    }


def call_llm_batch(client: OpenAI, reviews: list[dict]) -> list[dict]:
    """
    Envoie un batch de reviews à OpenAI.
    Retry avec backoff exponentiel (2s, 4s, 8s).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                response_format={"type": "json_object"},
                temperature=TEMPERATURE,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": build_user_prompt(reviews)},
                ],
            )
            raw       = response.choices[0].message.content
            results   = _parse_llm_response(raw)
            validated = [_validate_enrichment(r) for r in results]
            log.debug(f"  Batch OK — {len(validated)} enrichissements")
            return validated

        except json.JSONDecodeError as e:
            log.warning(f"  JSON invalide (essai {attempt}/{MAX_RETRIES}): {e}")
        except Exception as e:
            log.warning(f"  Erreur API (essai {attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)

    log.error(f"  Batch échoué après {MAX_RETRIES} essais — fallback")
    return []


def merge_results(
    reviews:     list[dict],
    enrichments: list[dict],
    model_used:  str = MODEL,
) -> list[dict]:
    """
    Merge enrichissements LLM + métadonnées originales.
    Fallback sur star_rating si review_id absent de la réponse LLM.
    """
    now        = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    enrich_map = {e["review_id"]: e for e in enrichments if e.get("review_id")}

    return [
        {
            "review_id":          r["review_id"],
            "domain":             r["domain"],
            "sentiment":          (enrich_map.get(r["review_id"]) or fallback_enrichment(r)).get("sentiment", "neutral"),
            "category":           (enrich_map.get(r["review_id"]) or fallback_enrichment(r)).get("category",  "other"),
            "pain_point":         (enrich_map.get(r["review_id"]) or fallback_enrichment(r)).get("pain_point"),
            "actionable_insight": (enrich_map.get(r["review_id"]) or fallback_enrichment(r)).get("actionable_insight"),
            "model_used":         model_used,
            "enriched_at":        now,
        }
        for r in reviews
    ]