with leads_source as (
    select * from {{ source('leads', 'leads_raw') }}
),

builtwith_fr_source as (
    select * from {{ ref('leads_builtwith_fr') }}
),

cleaned as (

    -- ── Source 1 : target leads ────────────────────
    select
        {{ dbt_utils.generate_surrogate_key([
           'domain',
           'ecommerce_platform',
           'helpdesk',
           'technologies_app_partners',
           'estimated_gmv_band'
       ]) }}                               as domain_id,

        cast(null as int64)                 as rank,
        lower(trim(domain))                 as domain,
        cast(null as string)                as sales_revenue,
        cast(null as string)                as tech_spend,
        cast(null as string)                as social_followers,
        cast(null as string)                as traffic_tier,
        lower(trim(ecommerce_platform))     as ecommerce_platform,
        lower(trim(helpdesk))               as helpdesk,
        technologies_app_partners,
        estimated_gmv_band,
        'target_leads_raw'                  as source,
        current_timestamp()                 as _loaded_at

    from leads_source
    where domain is not null
      and trim(domain) != ''

    union all

    -- ── Source 2 : BuiltWith French top eCommerce ─────────────
    select
       {{ dbt_utils.generate_surrogate_key([
            'domain',
            'rank',
            'tech_spend',
          'social_followers'
       ]) }}                               as domain_id,

        rank,
        lower(trim(domain))                 as domain,
        sales_revenue,
        tech_spend,
        social_followers,

        case
            when lower(trim(traffic_tier)) = 'very high' then 'very_high'
            when lower(trim(traffic_tier)) = 'high'      then 'high'
            when lower(trim(traffic_tier)) = 'medium'    then 'medium'
            when lower(trim(traffic_tier)) = 'low'       then 'low'
            else null
        end                                 as traffic_tier,

        cast(null as string)                as ecommerce_platform,
        cast(null as string)                as helpdesk,
        cast(null as string)                as technologies_app_partners,

        case
            when sales_revenue like '%100m%'  then 'Enterprise (>$100M)'
            when sales_revenue like '%m+'     then 'Mid-Market ($3M-$100M)'
            when sales_revenue like '%k+'     then 'SMB 2 ($150K-3M)'
            else                                   'No Data'
        end                                 as estimated_gmv_band,

        source,
        current_timestamp()                 as _loaded_at

    from builtwith_fr_source
    where domain is not null
      and trim(domain) != ''

),

deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by domain
                order by
                    case when source = 'target_leads_raw' then 0 else 1 end asc,
                    rank asc nulls last
            ) as row_num
        from cleaned
    )
    where row_num = 1

)

select
    domain_id,
    rank,
    domain,
    sales_revenue,
    tech_spend,
    social_followers,
    traffic_tier,
    ecommerce_platform,
    helpdesk,
    technologies_app_partners,
    estimated_gmv_band,
    source,
    _loaded_at

from deduplicated