import logging
from google.cloud import bigquery
 
from .config import (
    BQ_PROJECT,
    BQ_LOCATION,
    BQ_BATCH_SIZE,
    LEADS_DATASET,
    LEADS_TABLE,
    FR_LEADS_DATASET,
    FR_LEADS_TABLE,
    REVIEWS_DATASET,
    REVIEWS_TABLE,
    REVIEWS_TABLE_FR,
)
 
log = logging.getLogger(__name__)
 
# ── BQ Schema — identical for both tables ─────────────────────
BQ_SCHEMA = [
    bigquery.SchemaField("domain",          "STRING",    description="Original domain from leads source"),
    bigquery.SchemaField("trustpilot_slug", "STRING",    description="Real Trustpilot slug (may differ from domain)"),
    bigquery.SchemaField("review_id",       "STRING",    description="Unique Trustpilot review ID"),
    bigquery.SchemaField("review_text",     "STRING",    description="Review body"),
    bigquery.SchemaField("review_title",    "STRING",    description="Review title"),
    bigquery.SchemaField("star_rating",     "INTEGER",   description="Rating 1-5"),
    bigquery.SchemaField("date_published",  "DATE",      description="Publication date"),
    bigquery.SchemaField("reviewer_name",   "STRING",    description="Reviewer display name"),
    bigquery.SchemaField("company_replied", "BOOLEAN",   description="Merchant replied?"),
    bigquery.SchemaField("language",        "STRING",    description="Detected language ISO 639-1"),
    bigquery.SchemaField("ingested_at",     "TIMESTAMP", description="Pipeline ingestion timestamp"),
]
 
 
def get_client() -> bigquery.Client:
    """Connects via gcloud ADC — requires: gcloud auth application-default login."""
    client = bigquery.Client(project=BQ_PROJECT)
    log.info(f"✅ BigQuery connected — {BQ_PROJECT}")
    return client
 
 
def _ensure_table(client: bigquery.Client, table_id: str) -> None:
    """
    Creates a reviews table if it doesn't exist.
    Partitioned by ingested_at, clustered by domain.
    Shared by both pipelines — same schema, different table names.
    """
    table                   = bigquery.Table(table_id, schema=BQ_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_at",
    )
    table.clustering_fields = ["domain"]
    client.create_table(table, exists_ok=True)
    log.info(f"✅ Table ready: {table_id}")
 
 
def ensure_reviews_table(client: bigquery.Client, source: str = "default") -> None:
    """
    Ensures the reviews dataset and the correct target table exist.
 
    source="default" → creates reviews.reviews_raw
    source="fr"      → creates reviews.reviews_raw_fr
    """
    # Dataset — shared by both tables
    ds          = bigquery.Dataset(f"{BQ_PROJECT}.{REVIEWS_DATASET}")
    ds.location = BQ_LOCATION
    client.create_dataset(ds, exists_ok=True)
 
    # Target table depends on source
    table_name = REVIEWS_TABLE_FR if source == "fr" else REVIEWS_TABLE
    table_id   = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{table_name}"
    _ensure_table(client, table_id)
 
 
def load_domains(
    client:     bigquery.Client,
    source:     str        = "default",
    limit:      int | None = None,
    start_from: str | None = None,
) -> list[str]:
    """
    Loads domains from the correct leads source table.
 
    source="default" → reads from leads.leads_table
                        (original Gorgias leads)
 
    source="fr"      → reads from analytics.stg_leads_builtwith_fr
                        (BuiltWith French top eCommerce)
 
    Both queries normalize domains with LOWER(TRIM()) and deduplicate.
    start_from enables resuming after a crash alphabetically.
    """
    if source == "fr":
        from_clause = f"`{BQ_PROJECT}.{FR_LEADS_DATASET}.{FR_LEADS_TABLE}`"
        label       = f"{FR_LEADS_DATASET}.{FR_LEADS_TABLE}"
    else:
        from_clause = f"`{BQ_PROJECT}.{LEADS_DATASET}.{LEADS_TABLE}`"
        label       = f"{LEADS_DATASET}.{LEADS_TABLE}"
 
    start_clause = f"AND LOWER(TRIM(domain)) >= '{start_from.lower()}'" if start_from else ""
    limit_clause = f"LIMIT {limit}" if limit else ""
 
    query = f"""
        SELECT DISTINCT LOWER(TRIM(domain)) AS domain
        FROM {from_clause}
        WHERE domain IS NOT NULL
          AND TRIM(domain) != ''
          {start_clause}
        ORDER BY domain
        {limit_clause}
    """
 
    domains = [row["domain"] for row in client.query(query, location=BQ_LOCATION).result()]
    log.info(f"✅ {len(domains)} domains loaded from {label}")
    return domains
 
 
def upload_reviews(
    client: bigquery.Client,
    rows:   list[dict],
    source: str = "default",
) -> None:
    """
    Uploads reviews to the correct target table.
 
    source="default" → reviews.reviews_raw
    source="fr"      → reviews.reviews_raw_fr
    """
    if not rows:
        return
 
    table_name = REVIEWS_TABLE_FR if source == "fr" else REVIEWS_TABLE
    table_id   = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{table_name}"
 
    for start in range(0, len(rows), BQ_BATCH_SIZE):
        batch  = rows[start:start + BQ_BATCH_SIZE]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            log.error(f"❌ BQ insert errors: {errors[:2]}")
        else:
            log.info(f"⬆  {len(batch)} rows → {table_id}")