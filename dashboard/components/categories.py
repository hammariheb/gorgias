# dashboard/components/categories.py

import streamlit as st
import pandas as pd
import plotly.express as px


def render(df_domains: pd.DataFrame, df_categories: pd.DataFrame) -> None:
    st.header("Category Breakdown")

    # Only domains with reviews have category data
    found_domains = df_domains[df_domains["trustpilot_status"] == "found"]["domain"].tolist()

    if not found_domains:
        st.info("No domains found on Trustpilot in the current filter selection.")
        return

    selected    = st.selectbox("Select a domain", found_domains, key="cat_domain")
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