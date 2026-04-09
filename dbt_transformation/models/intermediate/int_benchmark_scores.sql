with fr_medians as (

    select
        percentile_cont(avg_rating,   0.5) over () as fr_median_rating,
        percentile_cont(pct_negative, 0.5) over () as fr_median_pct_negative,
        percentile_cont(reply_rate,   0.5) over () as fr_median_reply_rate
    from {{ ref('int_reviews_agg') }}
    where domain_source = 'builtwith_top_ecommerce_fr'
      and avg_rating is not null
    limit 1

),

-- Target leads aggregates
leads_agg as (
    select * from {{ ref('int_reviews_agg') }}
    where domain_source = 'target_leads_raw'
)

select
    la.domain,
    la.domain_id,

    la.avg_rating,
    la.pct_negative,
    la.reply_rate,

    m.fr_median_rating,
    m.fr_median_pct_negative,
    m.fr_median_reply_rate,

    round(la.avg_rating   - m.fr_median_rating,       2) as rating_gap,
    round(la.pct_negative - m.fr_median_pct_negative, 1) as neg_gap,
    round(la.reply_rate   - m.fr_median_reply_rate,   1) as reply_gap,

    round(
          (la.avg_rating   - m.fr_median_rating)       * 20
        - (la.pct_negative - m.fr_median_pct_negative) * 0.6
        + (la.reply_rate   - m.fr_median_reply_rate)   * 0.2
    , 1)                                                  as benchmark_score,

    case
        when (la.avg_rating - m.fr_median_rating) < -1.0 then 'Far below French standard'
        when (la.avg_rating - m.fr_median_rating) < -0.3 then 'Below French standard'
        when (la.avg_rating - m.fr_median_rating) <  0.3 then 'On par with French standard'
        else                                                   'Above French standard'
    end                                                   as benchmark_label

from leads_agg la
cross join fr_medians m