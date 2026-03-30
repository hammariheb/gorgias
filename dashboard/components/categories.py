# dashboard/components/categories.py

import streamlit as st
import pandas as pd
import plotly.express as px


def render(df_domains: pd.DataFrame, df_categories: pd.DataFrame) -> None:
    st.header("Category Breakdown")

    found_domains = df_domains[df_domains["trustpilot_status"] == "found"]["domain"].tolist()
    if not found_domains:
        st.info("No domains found on Trustpilot in the current filter selection.")
        return

    # ── Pré-sélectionner le domaine choisi dans l'Overview ───
    current = st.session_state.get("selected_domain")
    default_idx = 0

    # Si le domaine sélectionné est dans la liste des found domains → le pré-sélectionner
    if current and current in found_domains:
        default_idx = found_domains.index(current)
    # Si le domaine sélectionné est not_found → afficher un message et laisser le choix libre
    elif current and current not in found_domains:
        st.warning(f"**{current}** is not found on Trustpilot — no category data available. Showing all found domains below.")

    selected = st.selectbox(
        "Select a domain",
        found_domains,
        index=default_idx,
        key="cat_domain",
    )

    # Sync session_state si l'utilisateur change ici
    if selected != current:
        st.session_state.selected_domain = selected

    domain_cats = df_categories[df_categories["domain"] == selected].copy()

    if domain_cats.empty:
        st.info(f"No category data available for {selected}.")
        return

    domain_cats = domain_cats.sort_values("review_count", ascending=True)

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(
            domain_cats,
            x="review_count", y="category", orientation="h",
            color="avg_rating",
            color_continuous_scale=["#E24B4A", "#EF9F27", "#639922"],
            range_color=[1, 5],
            title=f"Review categories — {selected}",
            labels={
                "review_count": "Number of reviews",
                "category":     "Category",
                "avg_rating":   "Avg ⭐",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Details")
        for _, row in domain_cats.sort_values("review_count", ascending=False).iterrows():
            st.metric(
                label=row["category"],
                value=f"{int(row['review_count'])} reviews",
                delta=f"{row.get('pct_of_domain', 0):.1f}% of total",
            )