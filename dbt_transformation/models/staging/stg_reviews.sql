with source as (
    select * from {{ source('reviews', 'reviews_raw') }}
),

cleaned as (
    select
        review_id,
        lower(trim(domain)) as domain,
        review_text,
        review_title,
        cast(star_rating as int64) as star_rating,
        date_published as review_date,
        reviewer_name,
        company_replied,
        language,
        ingested_at
    from source

),

deduped as (
    select *,
        row_number() over (
            partition by review_id
            order by ingested_at desc 
        ) as row_num
    from cleaned
),

leads as (
    select domain, domain_id
    from {{ ref('stg_leads') }}
)

select
    r.review_id,
    l.domain_id,
    r.domain,
    r.review_text,
    r.review_title,
    r.star_rating,
    r.review_date,
    r.reviewer_name,
    r.company_replied,
    r.language,
    r.ingested_at
from deduped r
left join leads l on r.domain = l.domain
where r.row_num = 1 