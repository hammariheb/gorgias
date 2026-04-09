import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT  = st.secrets["BQ_PROJECT"]
BQ_LOCATION = "EU"
BQ_DATASET  = "analytics"


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(
        project=BQ_PROJECT,
        credentials=credentials,
        location=BQ_LOCATION,
    )


@st.cache_data(ttl=600)
def load_domain_insights() -> pd.DataFrame:
    """
    All domains — Gorgias leads + French top brands.
    Sorted: Gorgias priority leads first, then FR brands by BuiltWith rank.
    """
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_domain_insights`
        ORDER BY
            CASE domain_source
                WHEN 'target_leads_raw' THEN 0
                ELSE 1
            END,
            CASE outreach_signal
                WHEN 'priority_lead'          THEN 1
                WHEN 'warm_lead'              THEN 2
                WHEN 'no_stack_prospect'      THEN 3
                WHEN 'inbox_upgrade_prospect' THEN 4
                WHEN 'competitor_prospect'    THEN 5
                WHEN 'lightweight_prospect'   THEN 6
                WHEN 'low_priority'           THEN 7
                ELSE 8
            END,
            builtwith_rank ASC NULLS LAST,
            review_count DESC
    """
    return get_bq_client().query(query, location=BQ_LOCATION).to_dataframe()


@st.cache_data(ttl=600)
def load_reviews_for_domain(domain: str, domain_source: str = "target_leads_raw") -> pd.DataFrame:
    """
    All reviews for a domain filtered by source — negatives first.
    domain_source prevents mixing Gorgias and FR reviews when
    the same domain appears in both pipelines.
    """
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_reviews_detail`
        WHERE domain        = '{domain}'
          AND domain_source = '{domain_source}'
        ORDER BY
            CASE sentiment
                WHEN 'negative' THEN 1
                WHEN 'neutral'  THEN 2
                ELSE 3
            END,
            star_rating ASC
    """
    return get_bq_client().query(query, location=BQ_LOCATION).to_dataframe()


@st.cache_data(ttl=600)
def load_category_agg() -> pd.DataFrame:
    """Category breakdown — all domains, all sources."""
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.int_category_agg`
        ORDER BY domain, review_count DESC
    """
    return get_bq_client().query(query, location=BQ_LOCATION).to_dataframe()
