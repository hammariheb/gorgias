import logging
import time
from datetime import datetime, timezone

from google.cloud import bigquery

from .config import (
    BQ_PROJECT,
    BQ_LOCATION,
    REVIEWS_DATASET,
    SOURCE_TABLE,
    ENRICHED_TABLE,
    SOURCE_TABLE_FR,
    ENRICHED_TABLE_FR,
)

log = logging.getLogger(__name__)

ENRICHED_SCHEMA = [
    bigquery.SchemaField("review_id",          "STRING",    description="FK → reviews_raw.review_id"),
    bigquery.SchemaField("domain",             "STRING",    description="Merchant domain"),
    bigquery.SchemaField("sentiment",          "STRING",    description="positive | neutral | negative"),
    bigquery.SchemaField("category",           "STRING",    description="Main review category"),
    bigquery.SchemaField("pain_point",         "STRING",    description="Pain point (nullable for positives)"),
    bigquery.SchemaField("actionable_insight", "STRING",    description="Actionable insight — never null"),
    bigquery.SchemaField("model_used",         "STRING",    description="LLM model used"),
    bigquery.SchemaField("enriched_at",        "TIMESTAMP", description="Enrichment timestamp"),
]


def get_client() -> bigquery.Client:
    client = bigquery.Client(project=BQ_PROJECT)
    log.info(f"✅ BigQuery connected — {BQ_PROJECT}")
    return client


def _source_and_enriched_tables(source: str) -> tuple[str, str]:
    """Returns (source_table, enriched_table) for the given source."""
    if source == "fr":
        return SOURCE_TABLE_FR, ENRICHED_TABLE_FR
    return SOURCE_TABLE, ENRICHED_TABLE


def ensure_enriched_table(client: bigquery.Client, source: str = "default") -> None:
    """
    Creates reviews dataset + enriched table if they don't exist.
    Waits 5s after creation for BQ streaming insert propagation.
    """
    _, enriched_table = _source_and_enriched_tables(source)

    # Dataset
    dataset_ref          = bigquery.Dataset(f"{BQ_PROJECT}.{REVIEWS_DATASET}")
    dataset_ref.location = BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)

    # Table
    table_id                = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{enriched_table}"
    table                   = bigquery.Table(table_id, schema=ENRICHED_SCHEMA)
    table.clustering_fields = ["domain", "sentiment"]
    client.create_table(table, exists_ok=True)
    log.info(f"✅ Table ready: {table_id}")

    # BQ streaming insert needs a few seconds after table creation
    log.info("  Waiting 5s for BQ propagation...")
    time.sleep(5)


def load_unenriched_reviews(
    client: bigquery.Client,
    source: str       = "default",
    limit:  int | None = None,
) -> list[dict]:
    """
    Loads reviews not yet enriched using LEFT JOIN for idempotence.

    source="default" → reads reviews_raw,    skips reviews already in reviews_enriched
    source="fr"      → reads reviews_raw_fr, skips reviews already in reviews_enriched_fr

    Safe to re-run — will only return reviews not yet processed.
    """
    source_table, enriched_table = _source_and_enriched_tables(source)
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT
            r.review_id,
            r.domain,
            r.review_text,
            r.review_title,
            r.star_rating
        FROM `{BQ_PROJECT}.{REVIEWS_DATASET}.{source_table}` r
        LEFT JOIN `{BQ_PROJECT}.{REVIEWS_DATASET}.{enriched_table}` e
            ON r.review_id = e.review_id
        WHERE e.review_id IS NULL
          AND r.review_text IS NOT NULL
          AND LENGTH(TRIM(r.review_text)) > 10
        ORDER BY r.domain
        {limit_clause}
    """

    rows = [dict(row) for row in client.query(query, location=BQ_LOCATION).result()]
    log.info(f"✅ {len(rows)} reviews to enrich from {source_table}")
    return rows


def upload_enriched_rows(
    client: bigquery.Client,
    rows:   list[dict],
    source: str = "default",
) -> None:
    """Streaming insert of enriched rows into the correct target table."""
    if not rows:
        return

    _, enriched_table = _source_and_enriched_tables(source)
    table_id          = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{enriched_table}"
    errors            = client.insert_rows_json(table_id, rows)

    if errors:
        log.error(f"❌ BQ insert errors: {errors[:2]}")
    else:
        log.info(f"⬆  {len(rows)} rows → {table_id}")