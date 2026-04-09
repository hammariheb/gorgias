with domains as (
    select * from {{ ref('stg_domains') }}
),

reviews_agg as (
    select * from {{ ref('int_reviews_agg') }}
),

benchmark as (
    select * from {{ ref('int_benchmark_scores') }}
),

top_pain as (

    select
        domain_id,
        domain,
        category as top_pain_category
    from (
        select
            domain_id,
            domain,
            category,
            row_number() over (
                partition by domain_id
                order by count(*) desc
            ) as rn
        from {{ ref('stg_reviews_enriched') }}
        where sentiment_final = 'negative'
          and category        is not null
        group by domain_id, domain, category
    )
    where rn = 1

)

select
    -- ── Identifiers ───────────────────────────────────────────
    d.domain_id,
    d.domain,
    d.source                                                as domain_source,

    -- ── BuiltWith metadata ───────
    d.rank                                                  as builtwith_rank,
    d.traffic_tier,
    d.sales_revenue,

    -- ── Lead metadata (targeted domains) ────────────
    d.ecommerce_platform,
    d.helpdesk,
    d.technologies_app_partners,
    d.estimated_gmv_band,

    -- ── Trustpilot presence ───────────────────────────────────
    case
        when r.domain_id is not null then 'found'
        else                              'not_found'
    end                                                     as trustpilot_status,

    -- ── Review metrics (both sources) ─────────────────────────
    coalesce(r.review_count, 0)                             as review_count,
    r.avg_rating,
    r.pct_positive,
    r.pct_neutral,
    r.pct_negative,
    r.reply_rate,
    r.first_review_date,
    r.last_review_date,

    -- ── Top pain category (both sources) ─────────────────────
    p.top_pain_category,

    -- ── CX quality tier (both sources) ───────────────────────
    case
        when r.avg_rating >= 4.0      then 'strong'
        when r.avg_rating >= 3.0      then 'moderate'
        when r.avg_rating is not null then 'weak'
        else                               'no_data'
    end                                                     as cx_quality_tier,

    -- ── FR benchmark scores (Target leads only) ─
    b.benchmark_score,
    b.benchmark_label,
    b.rating_gap,
    b.neg_gap,
    b.reply_gap,
    b.fr_median_rating,
    b.fr_median_pct_negative,
    b.fr_median_reply_rate,

    -- ── Outreach signal (targetted leads only) ─────
    case
        when d.source != 'target_leads_raw'                   then null
        when r.domain_id is not null and r.avg_rating < 3.0   then 'priority_lead'
        when r.domain_id is not null and r.avg_rating < 4.0   then 'warm_lead'
        when r.domain_id is not null                          then 'low_priority'
        when r.domain_id is null and d.helpdesk is null       then 'no_stack_prospect'
        when r.domain_id is null
         and lower(d.helpdesk) like '%shopify inbox%'         then 'inbox_upgrade_prospect'
        when r.domain_id is null
         and (lower(d.helpdesk) like '%zendesk%'
           or lower(d.helpdesk) like '%intercom%'
           or lower(d.helpdesk) like '%help_scout%')          then 'competitor_prospect'
        when r.domain_id is null
         and (lower(d.helpdesk) like '%tidio%'
           or lower(d.helpdesk) like '%chatra%'
           or lower(d.helpdesk) like '%olark%'
           or lower(d.helpdesk) in ('helpcenter','helpdesk')) then 'lightweight_prospect'
        else 'research_needed'
    end                                                     as outreach_signal,

    -- ── Tech maturity (Target leads only) ───────
    case
        when d.source != 'target_leads_raw'               then null
        when lower(d.helpdesk) like '%zendesk%'
          or lower(d.helpdesk) like '%intercom%'
          or lower(d.helpdesk) like '%help_scout%'         then 'high'
        when d.helpdesk is not null                        then 'medium'
        else                                                    'low'
    end                                                     as tech_maturity,

    current_timestamp()                                     as _loaded_at

from domains d
left join reviews_agg r  on d.domain_id = r.domain_id
left join benchmark   b  on d.domain_id = b.domain_id
left join top_pain    p  on d.domain_id = p.domain_id