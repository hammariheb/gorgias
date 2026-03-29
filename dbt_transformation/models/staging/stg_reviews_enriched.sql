with reviews as (
    select * from {{ ref('stg_reviews') }}
),

enriched as (
    select * from {{ source('reviews', 'reviews_enriched') }}
)

select
    r.review_id,
    r.domain_id,
    r.domain,
    r.review_text,
    r.review_title,
    r.star_rating,
    r.review_date,
    r.reviewer_name,
    r.company_replied,
    r.language,

    e.sentiment,
    e.category,
    e.pain_point,
    e.actionable_insight,
    e.model_used,
    e.enriched_at,

    case
        when e.review_id is not null then true
        else false
    end as is_enriched

from reviews r
left join enriched e on r.review_id = e.review_id