import logging

from google.cloud import bigquery

from ai_enrichment.config import (
    BQ_PROJECT,
    REVIEWS_DATASET,
    SOURCE_TABLE,
    ENRICHED_TABLE,
)

log = logging.getLogger(__name__)

ENRICHED_SCHEMA = [
    bigquery.SchemaField("review_id",          "STRING",    description="FK → reviews_raw.review_id"),
    bigquery.SchemaField("domain",             "STRING",    description="Domaine du marchand"),
    bigquery.SchemaField("sentiment",          "STRING",    description="positive | neutral | negative"),
    bigquery.SchemaField("category",           "STRING",    description="Catégorie principale"),
    bigquery.SchemaField("pain_point",         "STRING",    description="Point de douleur — null si positif sans plainte"),
    bigquery.SchemaField("actionable_insight", "STRING",    description="Insight actionnable — jamais null"),
    bigquery.SchemaField("model_used",         "STRING",    description="Modèle LLM utilisé"),
    bigquery.SchemaField("enriched_at",        "TIMESTAMP", description="Timestamp enrichissement"),
]


def get_client() -> bigquery.Client:
    client = bigquery.Client(project=BQ_PROJECT)
    log.info(f"✅ BigQuery connecté — {BQ_PROJECT}")
    return client


def ensure_enriched_table(client: bigquery.Client) -> None:
    """
    Crée le dataset reviews ET la table reviews_enriched si absents.
    Le dataset doit être créé EN PREMIER — BQ ne le crée pas automatiquement.
    """
    # 1. Dataset
    dataset_ref          = bigquery.Dataset(f"{BQ_PROJECT}.{REVIEWS_DATASET}")
    dataset_ref.location = "EU"
    client.create_dataset(dataset_ref, exists_ok=True)
    log.info(f"✅ Dataset prêt : {BQ_PROJECT}.{REVIEWS_DATASET}")

    # 2. Table
    table_id                = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{ENRICHED_TABLE}"
    table                   = bigquery.Table(table_id, schema=ENRICHED_SCHEMA)
    table.clustering_fields = ["domain", "sentiment"]
    client.create_table(table, exists_ok=True)
    log.info(f"✅ Table prête : {table_id}")


def load_reviews_to_enrich(
    client: bigquery.Client,
    limit:  int | None = None,
    domain: str | None = None,
) -> list[dict]:
    """
    Charge uniquement les reviews PAS encore enrichies (idempotent).
    LEFT JOIN sur reviews_enriched → safe à relancer sans doublons.
    """
    domain_clause = f"AND r.domain = '{domain}'" if domain else ""
    limit_clause  = f"LIMIT {limit}"             if limit  else ""

    query = f"""
        SELECT
            r.review_id,
            r.domain,
            r.review_text,
            r.review_title,
            r.star_rating
        FROM `{BQ_PROJECT}.{REVIEWS_DATASET}.{SOURCE_TABLE}` r
        LEFT JOIN `{BQ_PROJECT}.{REVIEWS_DATASET}.{ENRICHED_TABLE}` e
            ON r.review_id = e.review_id
        WHERE e.review_id IS NULL
          AND r.review_text IS NOT NULL
          AND LENGTH(r.review_text) > 10
          {domain_clause}
        ORDER BY r.domain
        {limit_clause}
    """

    rows = [dict(row) for row in client.query(query).result()]
    log.info(f"✅ {len(rows)} reviews à enrichir")
    return rows


def upload_enriched_rows(client: bigquery.Client, rows: list[dict]) -> None:
    """Streaming insert vers BigQuery."""
    if not rows:
        return
    table_id = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{ENRICHED_TABLE}"
    errors   = client.insert_rows_json(table_id, rows)
    if errors:
        log.error(f"❌ BQ insert errors: {errors[:2]}")
    else:
        log.info(f"⬆️  {len(rows)} rows → {table_id}")