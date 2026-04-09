import streamlit as st
import pandas as pd


def render(df_domains: pd.DataFrame, load_reviews_fn) -> None:
    st.header("Pain Point Spotlight")
    st.caption("High-priority domains — key insights before the call.")

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
        b_label = row.get("benchmark_label", "")

        title = f"{label} — **{domain}** ({avg:.2f} ⭐ | {pct_neg:.0f}% negative)"
        if b_label and pd.notna(b_label):
            title += f" | {b_label}"

        with st.expander(title):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Avg Rating", f"{avg:.2f} ⭐")
            c2.metric("% Negative", f"{pct_neg:.1f}%")
            c3.metric("Platform",   row.get("ecommerce_platform", "—"))
            r_gap = row.get("rating_gap")
            if pd.notna(r_gap):
                c4.metric("vs FR Rating", f"{r_gap:+.2f} ⭐",
                           delta_color="normal" if r_gap >= 0 else "inverse")

            reviews     = load_reviews_fn(domain)
            neg_reviews = reviews[reviews["sentiment"] == "negative"].head(3)

            if not neg_reviews.empty:
                st.markdown("**Top pain points:**")
                for _, rev in neg_reviews.iterrows():
                    if rev.get("pain_point"):
                        st.markdown(f"- 🔸 {rev['pain_point']}")
                    if rev.get("actionable_insight"):
                        st.markdown(f"  → 💡 *{rev['actionable_insight']}*")
            else:
                st.info("No enriched negative reviews for this domain.")
