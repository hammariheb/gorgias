# dashboard/

Streamlit dashboard that surfaces MerchantRadar analytics for the Gorgias sales team.
Reads directly from BigQuery marts — no local data files.

## Tabs

| Tab | Purpose |
|---|---|
| **Overview** | KPIs, outreach signal distribution, quick domain table with benchmark + trend badges |
| **Drill-down** | Per-domain deep dive — trend panel, benchmark vs FR, category breakdown, raw reviews |
| **Categories** | Category-level breakdown across all Gorgias leads |
| **Pain Points** | Top 10 priority and warm leads ranked by pain severity |
| **Top eCommerce** | French reference brands — ratings, sentiment, vs-your-leads comparison |

## Running locally

```bash
cd dashboard
pip install streamlit plotly pandas google-cloud-bigquery db-dtypes
streamlit run app.py
```

Requires `GOOGLE_APPLICATION_CREDENTIALS` and `BQ_PROJECT` to be set.

## Deployed

### Live Dashboard at: https://merchantradar.streamlit.app/

Streamlit Cloud reads secrets from the app's Secrets settings
(equivalent to the `.env` file).

## Files

| File | Purpose |
|---|---|
| `app.py` | Entry point — loads data, routes to tab components |
| `bigquery_client.py` | Three query functions: `load_domain_insights()`, `load_reviews_for_domain()`, `load_category_agg()` |
| `components/overview.py` | KPI cards, signal chart, domain quick-view table |
| `components/drilldown.py` | Per-domain trend panel, benchmark, category chart, review list |
| `components/categories.py` | Category breakdown charts (Gorgias leads only) |
| `components/pain_points.py` | Priority/warm lead ranking by pain score |
| `components/top_ecommerce.py` | FR brands KPIs, ranking table, benchmark comparison |

## Data flow

```
mart_domain_insights  ──► Overview, Pain Points, Top eCommerce
mart_reviews_detail   ──► Drill-down (review list)
int_category_agg      ──► Categories, Drill-down (category chart)
```

All data is filtered by `domain_source` where relevant to prevent
Gorgias and FR data from mixing in source-specific views.
