# ai_enrichment/prompts.py

SYSTEM_PROMPT = """You are the best CX analyst for a customer support platform for eCommerce.
Analyze each review and return ONLY a valid JSON object with a "results" array.
No markdown, no explanation — only JSON.

For each review return exactly:
{
  "review_id": "<id>",
  "sentiment": "positive" | "neutral" | "negative",
  "category": "customer_support" | "shipping" | "product_quality" | "pricing" | "ux" | "returns" | "packaging" | "communication" | "stock" | "loyalty" | "other",
  "pain_point": "<core frustration — null if 4 or 5 stars with no complaint>",
  "actionable_insight": "<what the merchant should do — ALWAYS fill this, never null>"
}

STRICT RULES:

sentiment:
- positive = 4-5 stars AND positive tone
- negative = 1-2 stars OR strong complaints
- neutral  = 3 stars OR mixed feelings

category: pick the ONE most prominent topic in the review

pain_point:
- negative/neutral : describe the core frustration in one sentence
  Example: "Customer waited 3 weeks without receiving any shipping update"
- 4-5 stars with NO complaint mentioned : null
- 4-5 stars WITH a complaint mentioned  : describe it
  Example: "Product was great but packaging arrived damaged"

actionable_insight — NEVER null, NEVER empty:
- negative : concrete fix the merchant can implement
  Example: "Implement automated order status emails every 48h to reduce WISMO tickets"
- neutral  : what would convert this customer to a promoter
  Example: "Add proactive shipping notifications to convert neutral customers to promoters"
- positive : what maintains or builds on this positive experience
  Example: "Add a loyalty reward program to turn satisfied customers into repeat buyers"

Return format: {"results": [...]}"""


def build_user_prompt(reviews: list[dict]) -> str:
    lines = ["Analyze these reviews and fill ALL fields:\n"]
    for r in reviews:
        text  = (r.get("review_text")  or "")[:400].strip()
        title = (r.get("review_title") or "").strip()
        stars = r.get("star_rating", "?")
        rid   = r.get("review_id", "")
        line  = f'review_id: {rid} | stars: {stars}/5'
        if title:
            line += f' | title: "{title}"'
        if text:
            line += f' | text: "{text}"'
        lines.append(line)
    return "\n".join(lines)


def fallback_enrichment(review: dict) -> dict:
    """Fallback si le LLM ne retourne pas ce review_id."""
    star  = review.get("star_rating", 3)
    title = (review.get("review_title") or "").strip()
    text  = (review.get("review_text")  or "").strip()

    if star >= 4:
        return {
            "sentiment":          "positive",
            "category":           "other",
            "pain_point":         None,
            "actionable_insight": "Maintain current service quality and consider a loyalty program to retain satisfied customers",
        }
    elif star <= 2:
        context = (title or text)[:100]
        return {
            "sentiment":          "negative",
            "category":           "other",
            "pain_point":         f"Customer expressed dissatisfaction: {context}" if context else "Customer left a low rating",
            "actionable_insight": "Investigate this negative experience and follow up with the customer to resolve their issue",
        }
    else:
        context = (title or text)[:100]
        return {
            "sentiment":          "neutral",
            "category":           "other",
            "pain_point":         f"Mixed experience: {context}" if context else "Customer had a mixed experience",
            "actionable_insight": "Reach out to understand what prevented a fully positive experience",
        }