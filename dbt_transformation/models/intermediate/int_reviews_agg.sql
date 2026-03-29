with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
)

select
    domain_id,
    domain,
    count(*)                                                as review_count,
    round(avg(star_rating), 2)                              as avg_rating,
    countif(sentiment = 'positive')                         as positive_count,
    countif(sentiment = 'neutral')                          as neutral_count,
    countif(sentiment = 'negative')                         as negative_count,

    round(countif(sentiment = 'positive') * 100.0 / count(*), 1) as pct_positive,
    round(countif(sentiment = 'neutral')  * 100.0 / count(*), 1) as pct_neutral,
    round(countif(sentiment = 'negative') * 100.0 / count(*), 1) as pct_negative,
    round(countif(company_replied = true) * 100.0 / count(*), 1) as reply_rate,
    min(review_date)                                        as first_review_date,
    max(review_date)                                        as last_review_date,

    round(countif(is_enriched = true) * 100.0 / count(*), 1) as enrichment_rate

from enriched
group by domain_id, domain