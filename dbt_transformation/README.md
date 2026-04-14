# dbt_transformation/

Transforms raw BigQuery data into analytics-ready marts using dbt.
Handles both data sources (Target leads + BuiltWith FR) in a single unified
pipeline — no separate marts per source.

## Model layers

```
seeds/
└── leads_builtwith_fr.csv         # BuiltWith FR domain list → BigQuery

staging/                           # Clean + standardise raw tables
├── stg_domains.sql                # UNION ALL: leads_raw + builtwith seed, dedup
├── stg_reviews.sql                # UNION ALL: reviews_raw + reviews_raw_fr, tag lead_source
└── stg_reviews_enriched.sql       # JOIN each source to its correct enriched table

intermediate/                      # Aggregate + compute metrics
├── int_reviews_agg.sql            # Ratings, sentiment %, reply rate per domain
├── int_category_agg.sql           # Review category breakdown per domain
├── int_benchmark_scores.sql       # Gap vs FR median (benchmark scores)
└── int_trend_analysis.sql         # 30d / prev 30d / 90d rolling windows + trend_signal

marts/                             # Final analytics tables — one row per domain
├── mart_domain_insights.sql       # All domains, all sources, outreach_signal, tech_maturity
└── mart_reviews_detail.sql        # All reviews with enrichment joined
```

## Key design decisions

### Unified marts
Both Target leads and FR brands live in the same mart tables, distinguished
by the `domain_source` column. This avoids duplicating downstream logic and
makes cross-source comparisons trivial.

### `lead_source` tagged in staging
`lead_source` is set directly in `stg_reviews.sql` based on the source table
(`'target_leads_raw'` vs `'builtwith_top_ecommerce_fr'`). This prevents FR
domains from getting `NULL` domain_source downstream, which would break the
`trustpilot_status` logic.

### Separate enrichment JOINs
`stg_reviews_enriched.sql` joins each review to its correct enriched table
using `AND r.lead_source = '...'` — not a COALESCE between both pipelines.
This prevents cross-contamination between sources.

## Running dbt

```bash
cd dbt_transformation

# Install packages
dbt deps

# Full build (run + test)
dbt build --no-partial-parse

# Run only
dbt run --no-partial-parse

# Specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Single model
dbt run --select mart_domain_insights
```

## Profiles

`profiles.yml` uses `env_var()` for portability across local, Docker, and CI:

```yaml
dbt_transformation:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: service-account
      project: ""{{ env_var('BQ_PROJECT') }}"
      dataset: analytics_prod
      location: EU
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      threads: 4
      timeout_seconds: 300
```

## mart_domain_insights columns

| Column | Description |
|---|---|
| `domain` | Merchant domain |
| `domain_source` | `target_leads_raw` or `builtwith_top_ecommerce_fr` |
| `trustpilot_status` | `found` / `not_found` |
| `avg_rating` | Average star rating (1–5) |
| `review_count` | Total reviews scraped |
| `pct_negative` | % negative reviews |
| `reply_rate` | % reviews with company reply |
| `outreach_signal` | `priority_lead` / `warm_lead` / `monitor` / `not_found` |
| `tech_maturity` | `low` / `medium` / `high` (Target leads only) |
| `trend_signal` | `declining` / `stable` / `improving` |
| `benchmark_gap` | Rating gap vs FR median |
