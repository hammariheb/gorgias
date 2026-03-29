# dashboard/components/drilldown.py

import streamlit as st
import pandas as pd

SENTIMENT_BADGE = {"positive": "🟢", "neutral": "🟡", "negative": "🔴"}

SIGNAL_LABEL = {
    "priority_lead":          "🔴 Priority Lead",
    "warm_lead":              "🟠 Warm Lead",
    "no_stack_prospect":      "🟣 No Support Stack",
    "inbox_upgrade_prospect": "🔵 Shopify Inbox → Upgrade",
    "competitor_prospect":    "🟤 On Competitor",
    "lightweight_prospect":   "🟢 Lightweight Tool",
    "low_priority":           "⚪ Low Priority",
    "research_needed":        "❓ Research Needed",
}

PITCH_MAP = {
    "no_stack_prospect":      "No support tool detected — ideal Gorgias candidate, pitch from scratch.",
    "inbox_upgrade_prospect": "Using Shopify Inbox (free/basic) — natural upgrade pitch to Gorgias.",
    "competitor_prospect":    "On a competitor (Zendesk/Intercom) — pitch eCommerce specialization.",
    "lightweight_prospect":   "Using a lightweight tool (Tidio/Chatra/Olark) — pitch full helpdesk upgrade.",
}


def _render_found(row: pd.Series, reviews: pd.DataFrame) -> None:
    """Renders the full review breakdown for a domain found on Trustpilot."""
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Rating",  f"{row.get('avg_rating', 0):.2f} ⭐")
    col2.metric("Reviews",     int(row.get("review_count", 0)))
    col3.metric("% Positive",  f"{row.get('pct_positive', 0):.1f}%")
    col4.metric("% Negative",  f"{row.get('pct_negative', 0):.1f}%")

    st.markdown(f"**Signal:** {SIGNAL_LABEL.get(row.get('outreach_signal', ''), '—')}")
    st.markdown(f"**Reply rate:** {row.get('reply_rate', 0):.1f}%")
    st.divider()

    sentiment_filter = st.radio(
        "Filter by sentiment",
        ["All", "negative", "neutral", "positive"],
        horizontal=True,
        key="dd_sentiment",
    )

    filtered = reviews.copy()
    if sentiment_filter != "All":
        filtered = filtered[filtered["sentiment"] == sentiment_filter]

    st.caption(f"{len(filtered)} reviews")

    for _, rev in filtered.iterrows():
        badge   = SENTIMENT_BADGE.get(rev.get("sentiment"), "⚪")
        stars   = "⭐" * int(rev.get("star_rating", 0))
        title   = rev.get("review_title") or ""
        text    = rev.get("review_text")  or ""
        pain    = rev.get("pain_point")
        insight = rev.get("actionable_insight")
        cat     = rev.get("category", "")

        with st.container(border=True):
            col_a, col_b = st.columns([1, 4])
            with col_a:
                st.markdown(f"### {badge}")
                st.caption(stars)
                st.caption(f"`{cat}`")
            with col_b:
                if title: st.markdown(f"**{title}**")
                if text:  st.markdown(text[:400] + ("..." if len(text) > 400 else ""))
                if pain:    st.error(f"🔸 **Pain point:** {pain}")
                if insight: st.info(f"💡 **Insight:** {insight}")


def _render_not_found(row: pd.Series) -> None:
    """Renders the prospect fiche for a domain not found on Trustpilot."""
    st.info("This merchant is not listed on Trustpilot.")

    col1, col2, col3 = st.columns(3)
    col1.metric("Platform",      row.get("ecommerce_platform", "—"))
    col2.metric("Helpdesk",      row.get("helpdesk") or "None")
    col3.metric("Tech Maturity", row.get("tech_maturity", "—"))

    st.markdown(f"**GMV Band:** {row.get('estimated_gmv_band', '—')}")
    st.markdown(f"**Signal:** {SIGNAL_LABEL.get(row.get('outreach_signal', ''), '—')}")

    outreach = row.get("outreach_signal", "")
    if outreach in PITCH_MAP:
        st.success(f"💬 **Pitch angle:** {PITCH_MAP[outreach]}")


def render(df_domains: pd.DataFrame, load_reviews_fn, domain_search: str = "") -> None:
    st.header("Domain Drill-down")

    domain_list = df_domains["domain"].tolist()

    # Filter list by sidebar search
    if domain_search:
        domain_list = [d for d in domain_list if domain_search.lower() in d.lower()]

    if not domain_list:
        st.info("No domains match your search. Try a different keyword.")
        return

    selected = st.selectbox("Select a domain", domain_list, key="dd_domain")
    if not selected:
        return

    row    = df_domains[df_domains["domain"] == selected].iloc[0]
    status = row.get("trustpilot_status")

    st.subheader(f"🏪 {selected}")
    st.caption(f"{row.get('ecommerce_platform', '')} · {row.get('estimated_gmv_band', '')}")

    if status == "found":
        reviews = load_reviews_fn(selected)
        _render_found(row, reviews)
    else:
        _render_not_found(row)