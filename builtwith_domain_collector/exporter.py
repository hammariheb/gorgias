import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Columns in the exact order expected by the dbt seed schema
SEED_COLUMNS = [
    "rank",
    "domain",
    "sales_revenue",
    "tech_spend",
    "social_followers",
    "traffic_tier",
    "country",
    "source",
]

# ── Default seed path ─────────────────────────────────────────
# Resolves to: <project_root>/dbt_transformation/seeds/leads_builtwith_fr.csv
# __file__ = builtwith_collector/exporter.py
# .parent   = builtwith_collector/
# .parent   = <project_root>/
# then down into dbt_transformation/seeds/
DEFAULT_SEED_PATH = (
    Path(__file__).parent.parent
    / "dbt_transformation"
    / "seeds"
    / "leads_builtwith_fr.csv"
)


def _build_df(records: list[dict]) -> pd.DataFrame:
    """Shared cleaning logic used by both save functions."""
    df = pd.DataFrame(records)

    # Ensure all expected columns exist
    for col in SEED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Keep only expected columns in correct order
    df = df[SEED_COLUMNS]

    # Sort by rank ascending — lowest rank = most important
    df = df.sort_values("rank", na_position="last").reset_index(drop=True)

    # Deduplicate on domain
    df = df.drop_duplicates(subset="domain").reset_index(drop=True)

    return df


def save_csv(records: list[dict], output_path: str) -> pd.DataFrame:
    """
    Saves scraped records to a general-purpose CSV file.
    UTF-8 with BOM for Excel compatibility.
    """
    if not records:
        log.warning("No records to save.")
        return pd.DataFrame(columns=SEED_COLUMNS)

    df = _build_df(records)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    log.info(f"✅ Saved {len(df)} domains to {output_path}")
    return df


def save_seed_csv(
    records:   list[dict],
    seed_path: str | None = None,
) -> pd.DataFrame:
    """
    Saves a dbt-compatible seed CSV to dbt_transformation/seeds/.

    Default destination:
        <project_root>/dbt_transformation/seeds/leads_builtwith_fr.csv

    Why this path?
        dbt looks for seeds in the seeds/ directory at the dbt project root.
        seeds.yml must also be in that same folder.
        Putting the CSV anywhere else (e.g. models/seeds/) causes
        'Did not find matching node' warnings and dbt seed failures.

    dbt seed requirements:
    - No index column (index=False)
    - Column names matching seeds.yml schema exactly
    - Plain UTF-8 encoding (no BOM — dbt doesn't handle BOM well)

    The seeds/ folder is created automatically if it doesn't exist.
    """
    if not records:
        log.warning("No records to save as dbt seed.")
        return pd.DataFrame(columns=SEED_COLUMNS)

    # Use default path if none provided
    path = Path(seed_path) if seed_path else DEFAULT_SEED_PATH

    df = _build_df(records)

    # Create dbt_transformation/seeds/ if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Plain UTF-8 — no BOM, no index
    df.to_csv(path, index=False, encoding="utf-8")

    log.info(f"✅ dbt seed saved to {path} ({len(df)} domains)")
    log.info(f"   Next: cd dbt_transformation && dbt seed --select leads_builtwith_fr")
    return df