import streamlit as st
import pandas as pd
import plotly.express as px

SENTIMENT_BADGE = {"positive": "🟢", "neutral": "🟡", "negative": "🔴"}


def render(
    df_fr:          pd.DataFrame,
    df_fr_cats:     pd.DataFrame,
    df_leads:       pd.DataFrame,
) -> None:
    st.header("🏆 Top French eCommerce — Reference")
    st.caption("Best ranked French eCommerce brands from BuiltWith · Used as CX benchmark.")

    if df_fr.empty:
        st.info(
            "No French reference data yet. Run:\n"
            "```\npython -m scraper.main --source fr\n"
            "python -m ai_enrichment.main --source fr\n"
            "dbt build --no-partial-parse\n```"
        )
        return

    fr_found = df_fr[df_fr["trustpilot_status"] == "found"]

    # ── Global KPIs ───────────────────────────────────────────
    fr_avg     = fr_found["avg_rating"].mean()   if not fr_found.empty else None
    fr_pct_neg = fr_found["pct_negative"].mean() if not fr_found.empty else None
    fr_reply   = fr_found["reply_rate"].mean()   if not fr_found.empty else None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("FR Top Brands",       len(df_fr))
    c2.metric("Found on Trustpilot", len(fr_found),
              f"{len(fr_found)/len(df_fr)*100:.0f}%" if len(df_fr) > 0 else "")
    if fr_avg     is not None: c3.metric("Avg Rating",     f"{fr_avg:.2f} ⭐")
    if fr_pct_neg is not None: c4.metric("Avg % Negative", f"{fr_pct_neg:.1f}%")
    if fr_reply   is not None: c5.metric("Avg Reply Rate", f"{fr_reply:.1f}%")

    st.divider()

    # ── Ranking chart + Comparison ────────────────────────────
    col_l, col_r = st.columns([1.6, 1])

    with col_l:
        st.subheader("Ranking by Trustpilot score")
        top20 = fr_found.nlargest(20, "avg_rating")
        if not top20.empty:
            fig = px.bar(
                top20, x="avg_rating", y="domain", orientation="h",
                color="avg_rating",
                color_continuous_scale=["#E24B4A", "#EF9F27", "#52b788"],
                range_color=[1, 5], text="avg_rating",
                labels={"avg_rating": "Avg Rating ⭐", "domain": ""},
                title="Top 20 by avg rating",
            )
            fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
            fig.update_layout(
                showlegend=False, coloraxis_showscale=False,
                yaxis={"categoryorder": "total ascending"}, margin={"r": 60},
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("vs Your leads")
        if not df_leads.empty and fr_avg is not None:
            leads_found = df_leads[df_leads["trustpilot_status"] == "found"]
            if not leads_found.empty:
                leads_avg     = leads_found["avg_rating"].mean()
                leads_pct_neg = leads_found["pct_negative"].mean()
                leads_reply   = leads_found["reply_rate"].mean()

                r_d = leads_avg - fr_avg
                st.metric("Your leads avg", f"{leads_avg:.2f} ⭐",
                           f"{r_d:+.2f} vs FR top",
                           delta_color="normal" if r_d >= 0 else "inverse")

                if fr_pct_neg is not None:
                    n_d = leads_pct_neg - fr_pct_neg
                    st.metric("Your leads % negative", f"{leads_pct_neg:.1f}%",
                               f"{n_d:+.1f}% vs FR top",
                               delta_color="inverse" if n_d > 0 else "normal")

                if fr_reply is not None:
                    rep_d = leads_reply - fr_reply
                    st.metric("Your leads reply rate", f"{leads_reply:.1f}%",
                               f"{rep_d:+.1f}% vs FR top",
                               delta_color="normal" if rep_d >= 0 else "inverse")

                st.divider()
                gap = fr_avg - leads_avg
                if gap > 0.5:   st.error(f"**{gap:.1f}★ gap** — strong pitch opportunity.")
                elif gap > 0:   st.warning(f"**{gap:.1f}★ gap** — slightly below FR standard.")
                else:           st.success("Your leads match or exceed the FR benchmark.")

    st.divider()

    # ── Pain point distribution ───────────────────────────────
    if not df_fr_cats.empty:
        st.subheader("Pain point distribution across top FR brands")
        st.caption("Where even the best French brands struggle.")

        market_pain = (
            df_fr_cats[df_fr_cats["negative_count"] > 0]
            .groupby("category", as_index=False)
            .agg(negative_reviews=("negative_count", "sum"))
            .sort_values("negative_reviews", ascending=True)
        )
        if not market_pain.empty:
            fig2 = px.bar(
                market_pain, x="negative_reviews", y="category", orientation="h",
                color="negative_reviews",
                color_continuous_scale=["#52b788", "#EF9F27", "#E24B4A"],
                labels={"negative_reviews": "Negative reviews", "category": ""},
                title="Most common pain categories",
            )
            fig2.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig2, use_container_width=True)

    # ── Brand drill-down ──────────────────────────────────────
    if not fr_found.empty:
        st.divider()
        st.subheader("Brand drill-down")

        selected_fr = st.selectbox(
            "Select a brand", fr_found["domain"].tolist(), key="ref_domain_select"
        )
        row = fr_found[fr_found["domain"] == selected_fr].iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg Rating", f"{row.get('avg_rating', 0):.2f} ⭐")
        c2.metric("Reviews",    int(row.get("review_count", 0)))
        c3.metric("% Negative", f"{row.get('pct_negative', 0):.1f}%")
        c4.metric("Reply Rate", f"{row.get('reply_rate', 0):.1f}%")

        col_a, col_b = st.columns(2)
        cx       = row.get("cx_quality_tier", "")
        top_pain = row.get("top_pain_category", "")   # comes from mart_domain_insights
        if cx:       col_a.markdown(f"**CX Quality:** `{cx}`")
        if top_pain: col_b.markdown(f"**Top Pain Category:** `{top_pain}`")

        # Category chart for selected brand
        if not df_fr_cats.empty:
            brand_cats = df_fr_cats[df_fr_cats["domain"] == selected_fr].copy()
            if not brand_cats.empty:
                brand_cats = brand_cats.sort_values("review_count", ascending=True)
                fig3 = px.bar(
                    brand_cats, x="review_count", y="category", orientation="h",
                    color="avg_rating",
                    color_continuous_scale=["#E24B4A", "#EF9F27", "#639922"],
                    range_color=[1, 5],
                    title=f"Categories — {selected_fr}",
                    labels={"review_count": "Reviews", "category": "", "avg_rating": "Avg ⭐"},
                )
                st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    # ── Full reference table ──────────────────────────────────
    st.subheader("Full ranking table")

    display_cols = [c for c in [
        "builtwith_rank", "domain", "avg_rating", "review_count",
        "pct_negative", "reply_rate", "traffic_tier",
        "estimated_gmv_band", "cx_quality_tier", "top_pain_category",
    ] if c in df_fr.columns]

    rename_map = {
        "builtwith_rank":     "BW Rank",
        "domain":             "Domain",
        "avg_rating":         "Avg Rating",
        "review_count":       "Reviews",
        "pct_negative":       "% Negative",
        "reply_rate":         "Reply Rate %",
        "traffic_tier":       "Traffic",
        "estimated_gmv_band": "GMV Band",
        "cx_quality_tier":    "CX Tier",
        "top_pain_category":  "Top Pain",
    }

    table = df_fr[display_cols].rename(columns=rename_map)
    st.dataframe(table, use_container_width=True, hide_index=True, column_config={
        "Avg Rating":  st.column_config.NumberColumn(format="%.2f ⭐"),
        "% Negative":  st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100),
        "Reply Rate %": st.column_config.NumberColumn(format="%.1f%%"),
    })
