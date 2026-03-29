with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
    where is_enriched = true
      and sentiment = 'negative'
),

pain_point_agg as (
    select
        domain_id,
        domain,
        category,
        count(*)                    as negative_count,
        round(avg(star_rating), 2)  as avg_rating,

        -- Concaténer les pain points pour donner un aperçu au sales rep
        -- (les 3 premiers suffisent pour le contexte)
        string_agg(pain_point, ' | ' order by review_date desc limit 3) as sample_pain_points,

        -- Insights actionnables pour le marchand
        string_agg(
            distinct actionable_insight, ' | '
            limit 2
        ) as sample_insights,

        max(review_date)            as latest_negative_review

    from enriched
    group by domain_id, domain, category
)

select
    domain_id,
    domain,
    category,
    negative_count,
    avg_rating,
    sample_pain_points,
    sample_insights,
    latest_negative_review,

    -- Rang : catégorie avec le plus de pain points en premier
    row_number() over (
        partition by domain
        order by negative_count desc
    ) as pain_rank

from pain_point_agg