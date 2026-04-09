with enriched as (
    select * from {{ ref('stg_reviews_enriched') }}
)

select
    domain,
    domain_id,
    domain_source,

    count(*)                                                              as review_count,
    round(avg(star_rating), 2)                                            as avg_rating,

    countif(sentiment_final = 'positive')                                 as positive_count,
    countif(sentiment_final = 'neutral')                                  as neutral_count,
    countif(sentiment_final = 'negative')                                 as negative_count,

    round(countif(sentiment_final = 'positive') * 100.0 / count(*), 1)   as pct_positive,
    round(countif(sentiment_final = 'neutral')  * 100.0 / count(*), 1)   as pct_neutral,
    round(countif(sentiment_final = 'negative') * 100.0 / count(*), 1)   as pct_negative,

    round(countif(company_replied = true) * 100.0 / count(*), 1)         as reply_rate,

    min(review_date)                                                      as first_review_date,
    max(review_date)                                                      as last_review_date

from enriched
group by domain, domain_id, domain_source