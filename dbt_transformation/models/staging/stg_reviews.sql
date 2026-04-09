with leads_reviews as (
    select * from {{ source('reviews', 'reviews_raw') }}
),

builtwith_reviews as (
    select * from {{ source('reviews', 'reviews_raw_fr') }}
),

cleaned as (

    select
        review_id,
        lower(trim(domain))         as domain,
        review_text,
        review_title,
        cast(star_rating as int64)  as star_rating,
        date_published              as review_date,
        reviewer_name,
        company_replied,
        language,
        ingested_at,
        'target_leads_raw'          as source
    from leads_reviews
    where review_id is not null

    union all

    select
        review_id,
        lower(trim(domain))         as domain,
        review_text,
        review_title,
        cast(star_rating as int64)  as star_rating,
        date_published              as review_date,
        reviewer_name,
        company_replied,
        language,
        ingested_at,
        -- Tag source directly — guaranteed correct regardless of domain join
        'builtwith_top_ecommerce_fr' as source
    from builtwith_reviews
    where review_id is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by review_id
            order by ingested_at desc
        ) as row_num
    from cleaned

),

domains as (

    select domain, domain_id
    from {{ ref('stg_domains') }}

)

select
    r.review_id,
    d.domain_id,
    r.domain,
    r.source,
    r.review_text,
    r.review_title,
    r.star_rating,
    r.review_date,
    r.reviewer_name,
    r.company_replied,
    r.language,
    r.ingested_at
from deduped r
left join domains d on r.domain = d.domain
where r.row_num = 1