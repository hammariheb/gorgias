with reviews as (
    select * from {{ ref("stg_reviews") }}
),

enriched as (
    select * from {{ source('reviews', 'reviews_enriched') }}
),

enriched_fr as (
    select * from {{ source('reviews', 'reviews_enriched_fr') }}
),

joined as (

    select
        -- ── Identifiers ───────────────────────────────────────
        r.review_id,
        r.domain_id,
        r.domain,
        r.source                                           as domain_source,

        -- ── Review content ────────────────────────────────────
        r.review_text,
        r.review_title,
        r.star_rating,
        r.review_date,
        r.reviewer_name,
        r.company_replied,
        r.language,
        r.ingested_at,

        -- ── AI enrichment ─────────────────────────────────────
        case
            when r.source = 'builtwith_top_ecommerce_fr'
                then e_fr.sentiment
            else e.sentiment
        end as sentiment,

        case
            when r.source = 'builtwith_top_ecommerce_fr'
                then e_fr.category
            else e.category
        end as category,

        case
            when r.source = 'builtwith_top_ecommerce_fr'
                then e_fr.pain_point
            else e.pain_point
        end as pain_point,

        case
            when r.source = 'builtwith_top_ecommerce_fr'
                then e_fr.actionable_insight
            else e.actionable_insight
        end as actionable_insight,

        case
            when r.source = 'builtwith_top_ecommerce_fr'
                then e_fr.model_used
            else e.model_used
        end as model_used,

        case
            when r.source = 'builtwith_top_ecommerce_fr'
                then e_fr.enriched_at
            else e.enriched_at
        end as enriched_at,

        -- ── Sentiment fallback ────────────────────────────────
        case
            when r.source = 'builtwith_top_ecommerce_fr'
             and e_fr.sentiment is not null
                then e_fr.sentiment
            when r.source = 'target_leads_raw'
             and e.sentiment is not null
                then e.sentiment
            when r.star_rating >= 4 then 'positive'
            when r.star_rating =  3 then 'neutral'
            else                         'negative'
        end                                                      as sentiment_final

    from reviews r

    left join enriched    e    on r.review_id = e.review_id
        and r.source = 'target_leads_raw'

    left join enriched_fr e_fr on r.review_id = e_fr.review_id
        and r.source = 'builtwith_top_ecommerce_fr'

)

select * from joined