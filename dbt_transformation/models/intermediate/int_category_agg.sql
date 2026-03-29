with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
    where is_enriched = true
)

select
    domain_id,
    domain,
    category,

    count(*)                                                          as review_count,
    round(count(*) * 100.0 / sum(count(*)) over (partition by domain), 1) as pct_of_domain,

    countif(sentiment = 'positive')                                   as positive_count,
    countif(sentiment = 'neutral')                                    as neutral_count,
    countif(sentiment = 'negative')                                   as negative_count,

    round(avg(star_rating), 2)                                        as avg_rating

from enriched
group by domain_id, domain, category