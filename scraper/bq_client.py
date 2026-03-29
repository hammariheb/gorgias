# scraper/bq_client.py
# =====================
# Toute la logique BigQuery : connexion, création table, lecture domaines, upload.

import logging

from google.cloud import bigquery

from scraper.config import (
    BQ_PROJECT,
    BQ_LOCATION,
    BQ_BATCH_SIZE,
    LEADS_DATASET,
    LEADS_TABLE,
    REVIEWS_DATASET,
    REVIEWS_TABLE,
)

log = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────

BQ_SCHEMA = [
    bigquery.SchemaField("domain",           "STRING",    description="Domaine original du lead — clé de JOIN avec leads_table"),
    bigquery.SchemaField("trustpilot_slug",  "STRING",    description="Slug réellement utilisé sur Trustpilot (peut différer de domain)"),
    bigquery.SchemaField("review_id",        "STRING",    description="ID unique Trustpilot"),
    bigquery.SchemaField("review_text",      "STRING",    description="Corps de la review"),
    bigquery.SchemaField("review_title",     "STRING",    description="Titre"),
    bigquery.SchemaField("star_rating",      "INTEGER",   description="Note 1-5"),
    bigquery.SchemaField("date_published",   "DATE",      description="Date de publication"),
    bigquery.SchemaField("reviewer_name",    "STRING",    description="Nom du reviewer"),
    bigquery.SchemaField("company_replied",  "BOOLEAN",   description="Le marchand a-t-il répondu ?"),
    bigquery.SchemaField("language",         "STRING",    description="Langue ISO 639-1 détectée"),
    bigquery.SchemaField("ingested_at",      "TIMESTAMP", description="Timestamp ingestion pipeline"),
]


def get_client() -> bigquery.Client:
    """Connexion via gcloud ADC — nécessite : gcloud auth application-default login."""
    client = bigquery.Client(project=BQ_PROJECT)
    log.info(f"✅ BigQuery connecté — {BQ_PROJECT}")
    return client


def ensure_reviews_table(client: bigquery.Client) -> None:
    """
    Crée le dataset reviews et la table reviews_raw si absents.
    - Partitionnement sur ingested_at → requêtes filtrées rapides
    - Clustering sur domain           → GROUP BY domain efficace
    """
    ds          = bigquery.Dataset(f"{BQ_PROJECT}.{REVIEWS_DATASET}")
    ds.location = BQ_LOCATION
    client.create_dataset(ds, exists_ok=True)

    table_id                = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{REVIEWS_TABLE}"
    table                   = bigquery.Table(table_id, schema=BQ_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_at",
    )
    table.clustering_fields = ["domain"]
    client.create_table(table, exists_ok=True)

    log.info(f"✅ Table prête : {table_id}")


def load_domains(
    client:     bigquery.Client,
    limit:      int | None = None,
    start_from: str | None = None,
) -> list[str]:
    """
    Lit la colonne domain depuis leads.leads_table.
    Normalise en lowercase + strip.
    """
    start_clause = f"AND LOWER(TRIM(domain)) >= '{start_from.lower()}'" if start_from else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT DISTINCT LOWER(TRIM(domain)) AS domain
        FROM `{BQ_PROJECT}.{LEADS_DATASET}.{LEADS_TABLE}`
        WHERE domain IS NOT NULL
          AND TRIM(domain) != ''
          {start_clause}
        ORDER BY domain
        {limit_clause}
    """

    domains = [row["domain"] for row in client.query(query).result()]
    log.info(f"✅ {len(domains)} domaines chargés depuis {LEADS_DATASET}.{LEADS_TABLE}")
    return domains


def upload_reviews(client: bigquery.Client, rows: list[dict]) -> None:
    """Streaming insert — upload immédiat après chaque domaine pour la résilience."""
    if not rows:
        return

    table_id = f"{BQ_PROJECT}.{REVIEWS_DATASET}.{REVIEWS_TABLE}"

    for start in range(0, len(rows), BQ_BATCH_SIZE):
        batch  = rows[start : start + BQ_BATCH_SIZE]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            log.error(f"❌ BQ insert errors: {errors[:2]}")
        else:
            log.info(f"⬆️  {len(batch)} rows → {table_id}")