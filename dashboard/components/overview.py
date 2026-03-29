import streamlit as st
import pandas as pd
import plotly.express as px

SIGNAL_CONFIG = {
    "priority_lead":          {"label": "🔴 Priority Lead",          "color": "#E24B4A"},
    "warm_lead":              {"label": "🟠 Warm Lead",              "color": "#EF9F27"},
    "no_stack_prospect":      {"label": "🟣 No Stack Prospect",      "color": "#7F77DD"},
    "inbox_upgrade_prospect": {"label": "🔵 Inbox Upgrade",          "color": "#378ADD"},
    "competitor_prospect":    {"label": "🟤 Competitor Prospect",    "color": "#888780"},
    "lightweight_prospect":   {"label": "🟢 Lightweight Prospect",   "color": "#639922"},
    "low_priority":           {"label": "⚪ Low Priority",           "color": "#B4B2A9"},
    "research_needed":        {"label": "❓ Research Needed",        "color": "#D3D1C7"},
}

BASE_COLUMNS = {
    "domain":             "Domain",
    "ecommerce_platform": "Platform",
    "estimated_gmv_band": "GMV Band",
    "trustpilot_status":  "Trustpilot",
    "review_count":       "Reviews",
    "avg_rating":         "Avg Rating",
    "pct_negative":       "% Negative",
    "outreach_signal":    "Signal",
    "tech_maturity":      "Tech Maturity",
}

OPTIONAL_COLUMNS = {
    "helpdesk":                  "Helpdesk",
    "technologies_app_partners": "Tech Partners",
}


def render(df: pd.DataFrame) -> None:

    # ── KPI metrics ───────────────────────────────────────────
    total     = len(df)
    found     = len(df[df["trustpilot_status"] == "found"])
    not_found = len(df[df["trustpilot_status"] == "not_found"])
    priority  = len(df[df["outreach_signal"] == "priority_lead"])
    no_stack  = len(df[df["outreach_signal"] == "no_stack_prospect"])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Leads",      total)
    col2.metric("Found",            found,     f"{found/total*100:.0f}%" if total else "")
    col3.metric("Not Found",        not_found, f"{not_found/total*100:.0f}%" if total else "")
    col4.metric("Priority Leads",   priority,  "avg < 3.0 ⭐")
    col5.metric("No Support Stack", no_stack,  "Priority 1")

    st.caption(f"{total} domains displayed")
    st.divider()

    # ── Signal distribution chart ─────────────────────────────
    signal_counts = (
        df["outreach_signal"]
        .value_counts()
        .reset_index()
        .rename(columns={"outreach_signal": "signal", "count": "count"})
    )
    signal_counts["label"] = signal_counts["signal"].map(lambda x: SIGNAL_CONFIG.get(x, {}).get("label", x))
    signal_counts["color"] = signal_counts["signal"].map(lambda x: SIGNAL_CONFIG.get(x, {}).get("color", "#888"))

    fig = px.bar(
        signal_counts,
        x="count", y="label", orientation="h",
        color="signal",
        color_discrete_map={r["signal"]: r["color"] for _, r in signal_counts.iterrows()},
        title="Outreach signal distribution",
    )
    fig.update_layout(showlegend=False, yaxis_title="", xaxis_title="Number of domains")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Domain scroll selector ────────────────────────────────
    # Scrollable selectbox — lets the user click a domain in the overview
    # and see a quick summary inline without switching to drill-down tab
    st.subheader("Domain quick view")
    st.caption("Select a domain to see a summary inline")

    domain_list    = df["domain"].tolist()
    selected_quick = st.selectbox(
        "Jump to domain",
        ["— select a domain —"] + domain_list,
        key="overview_domain_scroll",
    )

    if selected_quick and selected_quick != "— select a domain —":
        row    = df[df["domain"] == selected_quick].iloc[0]
        status = row.get("trustpilot_status")

        with st.container(border=True):
            col_a, col_b, col_c = st.columns(3)
            col_a.markdown(f"**Platform:** {row.get('ecommerce_platform', '—')}")
            col_b.markdown(f"**GMV Band:** {row.get('estimated_gmv_band', '—')}")
            col_c.markdown(f"**Helpdesk:** {row.get('helpdesk') or 'None'}")

            col_d, col_e, col_f = st.columns(3)
            col_d.markdown(f"**Trustpilot:** {'✅ Found' if status == 'found' else '❌ Not found'}")
            col_e.markdown(f"**Tech Maturity:** {row.get('tech_maturity', '—')}")
            col_f.markdown(f"**Signal:** {row.get('outreach_signal', '—')}")

            if status == "found":
                avg     = row.get("avg_rating") or 0
                pct_neg = row.get("pct_negative") or 0
                count   = row.get("review_count") or 0
                st.markdown(
                    f"**Avg rating:** {avg:.2f} ⭐ &nbsp;|&nbsp; "
                    f"**Reviews:** {int(count)} &nbsp;|&nbsp; "
                    f"**% Negative:** {pct_neg:.1f}%"
                )

    st.divider()

    # ── Optional columns selector ─────────────────────────────
    st.subheader("All domains")

    selected_optional = st.multiselect(
        "Add columns",
        options=list(OPTIONAL_COLUMNS.values()),
        default=[],
        placeholder="Select optional columns to display...",
    )

    reverse_optional = {v: k for k, v in OPTIONAL_COLUMNS.items()}
    selected_keys    = [reverse_optional[label] for label in selected_optional]

    ordered_keys = []
    for key in BASE_COLUMNS:
        ordered_keys.append(key)
        if key == "ecommerce_platform":
            ordered_keys.extend(selected_keys)

    all_display = {**BASE_COLUMNS, **{k: OPTIONAL_COLUMNS[k] for k in selected_keys}}

    table = df[ordered_keys].copy().rename(columns=all_display)
    table["Avg Rating"] = table["Avg Rating"].round(2)
    table["% Negative"] = table["% Negative"].round(1)

    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Avg Rating": st.column_config.NumberColumn(format="%.2f ⭐"),
            "% Negative": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100),
        }
    )