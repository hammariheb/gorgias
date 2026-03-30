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

    # ── KPIs ──────────────────────────────────────────────────
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

    # ── Signal chart ──────────────────────────────────────────
    signal_counts = (
        df["outreach_signal"]
        .value_counts()
        .reset_index()
        .rename(columns={"outreach_signal": "signal", "count": "count"})
    )
    signal_counts["label"] = signal_counts["signal"].map(lambda x: SIGNAL_CONFIG.get(x, {}).get("label", x))
    signal_counts["color"] = signal_counts["signal"].map(lambda x: SIGNAL_CONFIG.get(x, {}).get("color", "#888"))

    fig = px.bar(
        signal_counts, x="count", y="label", orientation="h",
        color="signal",
        color_discrete_map={r["signal"]: r["color"] for _, r in signal_counts.iterrows()},
        title="Outreach signal distribution",
    )
    fig.update_layout(showlegend=False, yaxis_title="", xaxis_title="Number of domains")
    st.plotly_chart(fig, use_container_width=True)

    # ── Domain quick selector ─────────────────────────────────
    st.divider()
    st.subheader("Select a domain")
    st.caption("Selecting a domain here syncs the Drill-down and Categories tabs automatically.")

    domain_list = ["— select a domain —"] + df["domain"].tolist()

    # Pré-sélectionner le domaine actif si déjà choisi
    current = st.session_state.get("selected_domain")
    default_idx = 0
    if current and current in df["domain"].tolist():
        default_idx = domain_list.index(current)

    chosen = st.selectbox(
        "Jump to domain",
        domain_list,
        index=default_idx,
        key="overview_domain_select",
    )

    # Mettre à jour session_state quand l'utilisateur choisit un domaine
    if chosen != "— select a domain —" and chosen != current:
        st.session_state.selected_domain = chosen
        st.rerun()   # recharge pour que les autres tabs reflètent le changement

    # Afficher un résumé rapide du domaine sélectionné
    if st.session_state.selected_domain and st.session_state.selected_domain in df["domain"].values:
        row    = df[df["domain"] == st.session_state.selected_domain].iloc[0]
        status = row.get("trustpilot_status")

        with st.container(border=True):
            st.markdown(f"**{st.session_state.selected_domain}** · {row.get('ecommerce_platform', '—')} · {row.get('estimated_gmv_band', '—')}")
            col_a, col_b, col_c = st.columns(3)
            col_a.markdown(f"**Trustpilot:** {'✅ Found' if status == 'found' else '❌ Not found'}")
            col_b.markdown(f"**Signal:** {row.get('outreach_signal', '—')}")
            col_c.markdown(f"**Tech Maturity:** {row.get('tech_maturity', '—')}")

            if status == "found":
                col_d, col_e, col_f = st.columns(3)
                avg     = row.get("avg_rating") or 0
                pct_neg = row.get("pct_negative") or 0
                count   = row.get("review_count") or 0
                col_d.metric("Avg Rating",  f"{avg:.2f} ⭐")
                col_e.metric("Reviews",     int(count))
                col_f.metric("% Negative",  f"{pct_neg:.1f}%")

            st.caption("→ Switch to Drill-down or Categories tab to explore this domain in detail.")

    st.divider()

    # ── Optional columns ──────────────────────────────────────
    st.subheader("All domains")

    selected_optional = st.multiselect(
        "Add columns",
        options=list(OPTIONAL_COLUMNS.values()),
        default=[],
        placeholder="Select optional columns...",
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

    # Highlight la ligne du domaine sélectionné
    event = st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",       # ← clic sur une ligne déclenche un rerun
        selection_mode="single-row",
        column_config={
            "Avg Rating": st.column_config.NumberColumn(format="%.2f ⭐"),
            "% Negative": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100),
        }
    )

    # Si l'utilisateur clique sur une ligne dans la table → sélectionner ce domaine
    if event.selection and event.selection.rows:
        row_idx    = event.selection.rows[0]
        clicked_domain = df.iloc[row_idx]["domain"]
        if clicked_domain != st.session_state.selected_domain:
            st.session_state.selected_domain = clicked_domain
            st.rerun()