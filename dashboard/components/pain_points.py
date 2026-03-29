# dashboard/components/pain_points.py

import streamlit as st
import pandas as pd


def render(df_domains: pd.DataFrame, load_reviews_fn) -> None:
    st.header("Pain Point Spotlight")
    st.caption("High-priority domains — key insights for the sales rep before the call.")

    # Only found domains have reviews to show pain points
    priority = df_domains[
        (df_domains["trustpilot_status"] == "found") &
        (df_domains["outreach_signal"].isin(["priority_lead", "warm_lead"]))
    ].sort_values("avg_rating", ascending=True)

    if priority.empty:
        st.info("No priority leads with Trustpilot reviews in the current selection.")
        return

    for _, row in priority.head(10).iterrows():
        domain  = row["domain"]
        signal  = row["outreach_signal"]
        avg     = row.get("avg_rating") or 0
        pct_neg = row.get("pct_negative") or 0
        label   = "🔴 Priority Lead" if signal == "priority_lead" else "🟠 Warm Lead"

        with st.expander(f"{label} — **{domain}** ({avg:.2f} ⭐ | {pct_neg:.0f}% negative)"):
            col1, col2, col3 = st.columns(3)
            col1.metric("Avg Rating",  f"{avg:.2f} ⭐")
            col2.metric("% Negative",  f"{pct_neg:.1f}%")
            col3.metric("Platform",    row.get("ecommerce_platform", "—"))

            reviews     = load_reviews_fn(domain)
            neg_reviews = reviews[reviews["sentiment"] == "negative"].head(3)

            if not neg_reviews.empty:
                st.markdown("**Top pain points:**")
                for _, rev in neg_reviews.iterrows():
                    pain    = rev.get("pain_point")
                    insight = rev.get("actionable_insight")
                    if pain:
                        st.markdown(f"- 🔸 {pain}")
                    if insight:
                        st.markdown(f"  → 💡 *{insight}*")
            else:
                st.info("No enriched negative reviews for this domain.")