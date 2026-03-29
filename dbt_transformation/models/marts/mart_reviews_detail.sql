with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
),

leads as (
    select
        domain,
        domain_id,
        ecommerce_platform,
        technologies_app_partners,
        estimated_gmv_band
    from {{ ref('stg_leads') }}
)

select
    -- ── Identifiants ─────────────────────────────────────────
    e.review_id,
    e.domain_id,
    e.domain,

    -- ── Contexte lead ─────────────────────────────────────────
    l.ecommerce_platform,
    l.estimated_gmv_band,
    l.technologies_app_partners,

    -- ── Contenu review ────────────────────────────────────────
    e.review_title,
    e.review_text,
    e.star_rating,
    e.review_date,
    e.reviewer_name,
    e.company_replied,
    e.language,

    -- ── Enrichissement AI ─────────────────────────────────────
    e.sentiment,
    e.category,
    e.pain_point,
    e.actionable_insight,
    e.is_enriched,
    e.model_used,
    e.enriched_at

from enriched e
left join leads l on e.domain = l.domain