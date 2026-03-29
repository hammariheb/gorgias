with source as (
    select * from {{ source('leads', 'leads_raw') }}
),

dedup as (
    select *,
    row_number() over (partition by lower(trim(domain)) order by lower(trim(domain) )) as rn
    from source
)

select
    {{dbt_utils.generate_surrogate_key(['domain', 'ecommerce_platform', 'helpdesk', 'technologies_app_partners', 'estimated_gmv_band'])}} as domain_id,
    lower(trim(domain))             as domain,
    lower(trim(ecommerce_platform)) as ecommerce_platform,
    lower(trim(helpdesk))           as helpdesk,
    technologies_app_partners,
    estimated_gmv_band
from dedup
where rn = 1