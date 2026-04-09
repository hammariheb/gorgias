import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import streamlit as st

from bigquery_client import load_domain_insights, load_reviews_for_domain, load_category_agg
from components import overview, drilldown, categories, pain_points, top_ecommerce

st.set_page_config(
    page_title="Merchant Review Intelligence",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
    <div style="
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 1.5rem;
        border: 1px solid #333;
    ">
        <div style="font-size:26px;font-weight:600;color:#ffffff;letter-spacing:-0.5px;">
            Merchant Review Intelligence
        </div>
    </div>
""", unsafe_allow_html=True)

# ── Data loading ──────────────────────────────────────────────
with st.spinner("Loading data..."):
    df_all        = load_domain_insights()
    df_categories = load_category_agg()

if df_all.empty:
    st.error("No data available.")
    st.stop()

# ── Split sources ─────────────────────────────────────────────
df_leads = df_all[df_all["domain_source"] == "target_leads_raw"].copy()
df_fr    = df_all[df_all["domain_source"] == "builtwith_top_ecommerce_fr"].copy()

# Split categories by source
has_source_col = "domain_source" in df_categories.columns
df_leads_cats  = df_categories[df_categories["domain_source"] == "target_leads_raw"].copy() \
    if has_source_col else df_categories.copy()
df_fr_cats     = df_categories[df_categories["domain_source"] == "builtwith_top_ecommerce_fr"].copy() \
    if has_source_col else pd.DataFrame()

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🎛 Filters")
    st.caption("Applied to tabs 1–4. Top eCommerce tab is always unfiltered.")

    status_filter = st.selectbox("Trustpilot Status", ["All", "found", "not_found"])

    signal_options = ["All"] + sorted(
        df_leads["outreach_signal"].dropna().unique().tolist()
    )
    signal_filter = st.selectbox("Outreach Signal", signal_options)

    platform_options = ["All"] + sorted(
        df_leads["ecommerce_platform"].dropna().unique().tolist()
    )
    platform_filter = st.selectbox("Platform", platform_options)

    st.divider()
    domain_search = st.text_input("Search domain", placeholder="e.g. sezane.com")

    st.divider()
    total    = len(df_leads)
    found    = int((df_leads["trustpilot_status"] == "found").sum())
    priority = int((df_leads["outreach_signal"] == "priority_lead").sum())
    no_stack = int((df_leads["outreach_signal"] == "no_stack_prospect").sum())

    st.caption("**Your leads**")
    st.caption(f"🟢 {found} / {total} on Trustpilot")
    st.caption(f"🔴 {priority} priority leads")
    st.caption(f"🟣 {no_stack} no-stack prospects")
    st.caption(f"🏆 {len(df_fr)} French top brands")
    st.divider()
    st.caption("📡 MerchantRadar v2.0")

# ── Apply filters (leads only) ────────────────────────────────
filtered = df_leads.copy()

if status_filter != "All":
    filtered = filtered[filtered["trustpilot_status"] == status_filter]
if signal_filter != "All":
    filtered = filtered[filtered["outreach_signal"] == signal_filter]
if platform_filter != "All":
    filtered = filtered[filtered["ecommerce_platform"] == platform_filter]
if domain_search:
    filtered = filtered[
        filtered["domain"].str.contains(domain_search.strip().lower(), case=False, na=False)
    ]

active = sum([
    status_filter   != "All",
    signal_filter   != "All",
    platform_filter != "All",
    bool(domain_search),
])
if active > 0:
    st.caption(
        f"🔍 {active} filter{'s' if active > 1 else ''} active "
        f"— **{len(filtered)}** / {total} leads"
    )

# ── Tabs ──────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "🔍 Drill-down",
    "📂 Categories",
    "🔸 Pain Points",
    "🏆 Top eCommerce",
])

with tab1:
    overview.render(filtered)

with tab2:
    drilldown.render(filtered, load_reviews_for_domain, df_leads_cats, domain_search)

with tab3:
    categories.render(filtered, df_leads_cats)

with tab4:
    pain_points.render(filtered, load_reviews_for_domain)

with tab5:
    top_ecommerce.render(df_fr, df_fr_cats, df_leads)
