with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
),

domains as (
    select
        domain_id,
        domain,
        domain_source,
        -- Gorgias lead context
        ecommerce_platform,
        estimated_gmv_band,
        helpdesk,
        tech_maturity,
        outreach_signal,
        -- FR brand context
        builtwith_rank,
        traffic_tier,
        sales_revenue,
        -- Both sources
        cx_quality_tier,
        top_pain_category
    from {{ ref('mart_domain_insights') }}
)

select
    -- ── Identifiers ───────────────────────────────────────────
    e.review_id,
    e.domain_id,
    e.domain,
    e.domain_source,

    -- ── Domain context (Gorgias leads) ────────────────────────
    d.ecommerce_platform,
    d.estimated_gmv_band,
    d.helpdesk,
    d.tech_maturity,
    d.outreach_signal,

    -- ── Domain context (FR brands) ────────────────────────────
    d.builtwith_rank,
    d.traffic_tier,
    d.sales_revenue,

    -- ── Domain context (both sources) ─────────────────────────
    d.cx_quality_tier,
    d.top_pain_category,

    -- ── Review content ────────────────────────────────────────
    e.review_title,
    e.review_text,
    e.star_rating,
    e.review_date,
    e.reviewer_name,
    e.company_replied,
    e.language,

    -- ── AI enrichment ─────────────────────────────────────────
    e.sentiment_final               as sentiment,
    e.category,
    e.pain_point,
    e.actionable_insight,
    e.model_used,
    e.enriched_at,
    e.model_used is not null        as is_enriched

from enriched e
left join domains d on e.domain_id    = d.domain_id
                   and e.domain_source = d.domain_source