with leads as (
    select * from {{ ref('stg_leads') }}
),

reviews_agg as (
    select * from {{ ref('int_reviews_agg') }}
)

select
    -- ── Identifiants ─────────────────────────────────────────
    l.domain_id,
    l.domain,

    -- ── Stack tech ────────────────────────────────────────────
    l.ecommerce_platform,
    l.helpdesk,
    l.technologies_app_partners,
    l.estimated_gmv_band,

    -- ── Présence Trustpilot ───────────────────────────────────
    case
        when r.domain is not null then 'found'
        else                           'not_found'
    end as trustpilot_status,

    -- ── Métriques reviews (null si not_found) ─────────────────
    coalesce(r.review_count, 0)                     as review_count,
    r.avg_rating,
    r.pct_positive,
    r.pct_neutral,
    r.pct_negative,
    r.reply_rate,
    r.first_review_date,
    r.last_review_date,

    case
        -- Domaines FOUND : priorité basée sur avg_rating
        when r.domain is not null and r.avg_rating < 3.0
            then 'priority_lead'

        when r.domain is not null and r.avg_rating between 3.0 and 3.9
            then 'warm_lead'

        when r.domain is not null and r.avg_rating >= 4.0
            then 'low_priority'

        -- Domaines NOT FOUND : opportunité basée sur helpdesk
        when r.domain is null and l.helpdesk is null
            then 'no_stack_prospect'

        when r.domain is null
         and lower(l.helpdesk) like '%shopify inbox%'
            then 'inbox_upgrade_prospect'

        when r.domain is null
         and (lower(l.helpdesk) like '%zendesk%'
           or lower(l.helpdesk) like '%intercom%'
           or lower(l.helpdesk) like '%help_scout%')
            then 'competitor_prospect'

        when r.domain is null
         and (lower(l.helpdesk) like '%tidio%'
           or lower(l.helpdesk) like '%chatra%'
           or lower(l.helpdesk) like '%olark%'
           or lower(l.helpdesk) in ('helpcenter', 'helpdesk'))
            then 'lightweight_prospect'

        else 'research_needed'
    end as outreach_signal,

    case
        when lower(l.helpdesk) like '%zendesk%'
          or lower(l.helpdesk) like '%intercom%'
          or lower(l.helpdesk) like '%help_scout%'
            then 'high'

        when l.helpdesk is not null
            then 'medium'

        else 'low'
    end as tech_maturity

from leads l
left join reviews_agg r on l.domain = r.domain