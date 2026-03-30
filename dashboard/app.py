import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
from bigquery_client import (
    load_domain_insights,
    load_category_agg,
    load_reviews_for_domain,
)
from components import overview, categories, pain_points, drilldown

st.set_page_config(
    page_title="Gorgias — Merchant Intelligence",
    page_icon="🛒",
    layout="wide",
)

# ── Dark header ───────────────────────────────────────────────
st.markdown("""
    <div style="
        background: #1a1a1a;
        padding: 2rem 2rem 1.5rem;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 1.5rem;
    ">
        <div style="font-size: 13px; color: #888; letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 8px;">
            Gorgias · Sales Intelligence
        </div>
        <div style="font-size: 28px; font-weight: 500; color: #ffffff;">
            Merchant Review Intelligence
        </div>
    </div>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────
with st.spinner("Loading data from BigQuery..."):
    df_domains    = load_domain_insights()
    df_categories = load_category_agg()

if df_domains.empty:
    st.error("No data available. Make sure `dbt build` has run successfully.")
    st.stop()

# ── Session state — selected domain shared across all tabs ────
# Initialise à None — aucun domaine sélectionné par défaut
if "selected_domain" not in st.session_state:
    st.session_state.selected_domain = None

# ── Sidebar filters ───────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    st.caption("Applied across all tabs")

    status_filter = st.selectbox("Trustpilot Status", ["All", "found", "not_found"])

    signal_options = ["All"] + sorted(df_domains["outreach_signal"].dropna().unique().tolist())
    signal_filter  = st.selectbox("Outreach Signal", signal_options)

    platform_options = ["All"] + sorted(df_domains["ecommerce_platform"].dropna().unique().tolist())
    platform_filter  = st.selectbox("Platform", platform_options)

    st.divider()
    domain_search = st.text_input("Search domain", placeholder="e.g. gymshark.com")

    st.divider()
    total = len(df_domains)
    found = len(df_domains[df_domains["trustpilot_status"] == "found"])
    st.caption(f"{found} / {total} domains on Trustpilot")

    # Afficher le domaine actif dans la sidebar
    if st.session_state.selected_domain:
        st.divider()
        st.caption(f"📍 Active domain:")
        st.markdown(f"**{st.session_state.selected_domain}**")
        if st.button("✕ Clear selection", use_container_width=True):
            st.session_state.selected_domain = None
            st.rerun()

# ── Apply filters ─────────────────────────────────────────────
filtered_domains = df_domains.copy()
if status_filter   != "All": filtered_domains = filtered_domains[filtered_domains["trustpilot_status"]  == status_filter]
if signal_filter   != "All": filtered_domains = filtered_domains[filtered_domains["outreach_signal"]     == signal_filter]
if platform_filter != "All": filtered_domains = filtered_domains[filtered_domains["ecommerce_platform"]  == platform_filter]
if domain_search:            filtered_domains = filtered_domains[filtered_domains["domain"].str.contains(domain_search.lower(), case=False, na=False)]

# ── Tabs ──────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Overview",
    "🔍 Drill-down",
    "📂 Categories",
    "🔸 Pain Points",
])

with tab1:
    overview.render(filtered_domains)

with tab2:
    drilldown.render(filtered_domains, load_reviews_for_domain, domain_search)

with tab3:
    categories.render(filtered_domains, df_categories)

with tab4:
    pain_points.render(filtered_domains, load_reviews_for_domain)