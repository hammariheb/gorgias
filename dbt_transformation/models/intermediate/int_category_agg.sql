with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
    where category is not null
)

select
    domain,
    domain_id,
    domain_source,
    category,

    count(*)                                                                    as review_count,
    round(count(*) * 100.0 / sum(count(*)) over (partition by domain), 1)      as pct_of_domain,

    countif(sentiment_final = 'positive')                                       as positive_count,
    countif(sentiment_final = 'neutral')                                        as neutral_count,
    countif(sentiment_final = 'negative')                                       as negative_count,

    round(avg(star_rating), 2)                                                  as avg_rating

from enriched
group by domain, domain_id, domain_source, category