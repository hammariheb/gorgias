# MerchantRadar

End-to-end analytics pipeline that identifies eCommerce merchants who are struggling with customer experience — and surfaces them as Target leads.


![Alt Full Pipeline](assets/Full_pipeline.png)


## What it does

MerchantRadar scrapes Trustpilot reviews for two domain lists, enriches them with AI-generated sentiment and pain-point analysis, transforms everything with dbt, and serves the results through a Streamlit dashboard.

```
BuiltWith (French top eCommerce) ──┐
                                    ├──► Trustpilot scraper ──► AI enrichment ──► dbt ──► Dashboard
Target leads (target accounts) ───┘
```

## Architecture

```
merchant_radar/
├── builtwith_domain_collector/   # Scrapes domain list from BuiltWith
├── scraper/                      # Scrapes Trustpilot reviews for all domains
├── ai_enrichment/                # Enriches reviews with GPT-4o-mini
├── dbt_transformation/           # Transforms raw data into analytics-ready marts
├── dashboard/                    # Streamlit dashboard
├── airflow/                      # Weekly orchestration DAG
└── .github/workflows/            # CI (lint + dbt compile) and CD (dbt build)
```

## Quickstart — local run

```bash
# 1. Install dependencies
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Set environment variables
cp .env.example .env
# Fill in BQ_PROJECT, OPENAI_API_KEY, GOOGLE_APPLICATION_CREDENTIALS

# 3. Collect FR domain list
python -m builtwith_domain_collector.main --pages 20

# 4. Scrape Trustpilot reviews
python -m scraper.main --source default --incremental
python -m scraper.main --source fr --incremental

# 5. Enrich with AI
python -m ai_enrichment.main --source default
python -m ai_enrichment.main --source fr

# 6. Run dbt
cd dbt_transformation
dbt deps
dbt build --no-partial-parse

# 7. Launch dashboard
cd dashboard
streamlit run app.py
```

## Quickstart — Airflow (Docker)

```bash
# Copy credentials to project root
cp /path/to/gcp-key.json ./gcp_credentials.json

# Start Airflow
docker-compose up airflow-init
docker-compose up -d

# Open http://localhost:8080 (airflow / airflow)
# Trigger: merchantradar_weekly
```

## Environment variables

| Variable | Description |
|---|---|
| `BQ_PROJECT` | Google Cloud project ID |
| `OPENAI_API_KEY` | OpenAI API key for GPT-4o-mini enrichment |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON |

## CI/CD

- **CI** — runs on every PR to `main`: ruff lint + dbt compile
- **CD** — runs on merge to `main` when pipeline code changes: dbt build in production

Both require GitHub secrets: `GCP_SERVICE_ACCOUNT_JSON`, `BQ_PROJECT`.

## Data sources

| Source | Table | Description |
|---|---|---|
| Target leads | `leads.leads_table` | Target accounts — Target prospecting list |
| BuiltWith FR | `analytics.leads_builtwith_fr` | French top eCommerce brands (dbt seed) |
| Trustpilot | `reviews.reviews_raw` | Scraped reviews for Target leads |
| Trustpilot FR | `reviews.reviews_raw_fr` | Scraped reviews for FR brands |
| AI enrichment | `reviews.reviews_enriched` | Sentiment, category, pain points |
| AI enrichment FR | `reviews.reviews_enriched_fr` | Same for FR brands |
