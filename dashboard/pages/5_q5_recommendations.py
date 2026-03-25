"""Q5 — Strategic advisor (business-driven dashboard)."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import plotly.express as px
import streamlit as st

from config.settings import settings
from dashboard.components.charts import style_plotly
from dashboard.components.tables import show_df
from dashboard.components.style import ACCENT, apply_global_style

apply_global_style()

st.title("Q5 — Strategic advisor")

root = Path(settings.gold_uri)
p_tx = root / "q5_results"
p_adv = root / "q5_advisory_summary"
p_report = Path("docs/Q5_BUSINESS_REPORT.md")

if not p_adv.exists():
    st.error("Missing `q5_advisory_summary` — run Gold Q5 first (`python -m pipelines.runner --question q5`).")
    st.stop()

df_adv = pd.read_parquet(p_adv)


def sec(section: str) -> pd.DataFrame:
    return df_adv[df_adv["section"] == section].copy()


st.subheader("Executive KPIs (from advisory aggregates)")

q5a = sec("q5a_cities")
q5b = sec("q5b_categories")
q5c = sec("q5c_months")
q5d = sec("q5d_hours")
q5_inst_impact = sec("q5_installments_impact")

col1, col2, col3, col4 = st.columns(4)
if len(q5a) > 0:
    q5a_sorted = q5a.sort_values("total_amount", ascending=False).reset_index(drop=True)
    with col1:
        st.metric("Best entry city", str(q5a_sorted.loc[0, "detail_label"]))
    with col2:
        st.metric("Best city revenue", f"{float(q5a_sorted.loc[0, 'total_amount']):,.0f}")
    with col3:
        st.metric("Best city avg ticket", f"{float(q5a_sorted.loc[0, 'avg_ticket']):,.2f}")
else:
    col1.metric("Best entry city", "N/A")
    col2.metric("Best city revenue", "N/A")
    col3.metric("Best city avg ticket", "N/A")

if len(q5c) > 0:
    q5c_sorted = q5c.sort_values("detail_key").reset_index(drop=True)
    peak_row = q5c_sorted.loc[q5c_sorted["total_amount"].idxmax()]
    with col4:
        st.metric("Peak month", str(peak_row["detail_label"]))

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Q5a Cities", "Q5b Categories", "Q5c Months", "Q5d Hours", "Q5e Installments EV"]
)

with tab1:
    st.subheader("Cities: revenue + volume + ticket value")
    if len(q5a) == 0:
        st.info("No `q5a_cities` data.")
    else:
        q5a_plot = q5a.sort_values("total_amount", ascending=False).reset_index(drop=True)
        total_rev = float(q5a_plot["total_amount"].sum())
        q5a_plot["cum_share_revenue"] = q5a_plot["total_amount"].cumsum() / total_rev

        topN = st.slider("Top N cities", 5, 15, 10)
        top_cities = q5a_plot.head(topN)

        st.plotly_chart(
            style_plotly(
                px.bar(
                top_cities,
                x="detail_label",
                y="total_amount",
                title=f"Top {topN} cities — total revenue",
                color_discrete_sequence=[ACCENT],
                ).update_layout(xaxis_tickangle=-35),
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            style_plotly(
                px.line(
                q5a_plot,
                x="detail_label",
                y="cum_share_revenue",
                markers=True,
                title="Pareto concentration — cumulative revenue share",
                color_discrete_sequence=[ACCENT],
                ).update_layout(xaxis_tickangle=-35),
            ),
            use_container_width=True,
        )
        show_df(top_cities)

with tab2:
    st.subheader("Categories: scale vs value (segmented)")
    if len(q5b) == 0:
        st.info("No `q5b_categories` data.")
    else:
        topN = st.slider("Top N categories", 3, 10, 4)
        q5b_plot = q5b.sort_values("total_amount", ascending=False).head(topN)
        fig = px.bar(
            q5b_plot,
            x="detail_label",
            y="total_amount",
            color="segment",
            title=f"Top categories — total revenue (segmented)",
        )
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(style_plotly(fig), use_container_width=True)

        fig2 = px.scatter(
            q5b,
            x="avg_ticket",
            y="total_amount",
            color="segment",
            size="transaction_count",
            title="High-value vs high-volume trade-off",
        )
        st.plotly_chart(style_plotly(fig2), use_container_width=True)
        show_df(q5b)

with tab3:
    st.subheader("Monthly trends: seasonality and peaks")
    if len(q5c) == 0:
        st.info("No `q5c_months` data.")
    else:
        q5c_plot = q5c.sort_values("detail_key")
        fig = px.line(
            q5c_plot,
            x="detail_label",
            y="total_amount",
            markers=True,
            title="Monthly revenue (total_amount)",
            color_discrete_sequence=[ACCENT],
        )
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(style_plotly(fig), use_container_width=True)
        show_df(q5c_plot)

with tab4:
    st.subheader("Hours optimization: volume peak vs revenue peak")
    if len(q5d) == 0:
        st.info("No `q5d_hours` data.")
    else:
        d = q5d.copy()
        d["hour"] = d["detail_key"].astype(int)
        peak_vol = d.loc[d["transaction_count"].idxmax()]
        peak_rev = d.loc[d["total_amount"].idxmax()]
        col1h, col2h = st.columns(2)
        with col1h:
            st.metric("Peak hour (volume)", str(peak_vol["detail_label"]))
        with col2h:
            st.metric("Peak hour (revenue)", str(peak_rev["detail_label"]))

        d_sorted = d.sort_values("hour")
        fig_vol = px.bar(
            d_sorted,
            x="hour",
            y="transaction_count",
            title="Transaction volume by hour",
            color_discrete_sequence=[ACCENT],
        )
        st.plotly_chart(style_plotly(fig_vol), use_container_width=True)

        fig_rev = px.bar(
            d_sorted,
            x="hour",
            y="total_amount",
            title="Revenue by hour",
            color_discrete_sequence=[ACCENT],
        )
        st.plotly_chart(style_plotly(fig_rev), use_container_width=True)
        show_df(d_sorted)

with tab5:
    st.subheader("Installments: expected value model (EV)")
    if len(q5_inst_impact) == 0:
        st.info("No `q5_installments_impact` data.")
    else:
        pivot = (
            q5_inst_impact[["detail_label", "expected_profit", "expected_revenue", "avg_ticket"]]
            .dropna()
            .set_index("detail_label")
        )

        if "with_installments" in pivot.index and "without_installments" in pivot.index:
            gap_profit = float(
                pivot.loc["with_installments", "expected_profit"]
                - pivot.loc["without_installments", "expected_profit"]
            )
            col1i, col2i, col3i = st.columns(3)
            with col1i:
                st.metric(
                    "EV with installments (profit)",
                    f"{float(pivot.loc['with_installments','expected_profit']):,.0f}",
                )
            with col2i:
                st.metric(
                    "EV without installments (profit)",
                    f"{float(pivot.loc['without_installments','expected_profit']):,.0f}",
                )
            with col3i:
                st.metric("Profit gap (with - without)", f"{gap_profit:,.0f}")

        plot_df = q5_inst_impact.copy()
        fig = px.bar(
            plot_df,
            x="detail_label",
            y="expected_profit",
            title="Expected profit: installments vs no-installments",
            color_discrete_sequence=[ACCENT],
        )
        st.plotly_chart(style_plotly(fig), use_container_width=True)

        st.subheader("Segmented by category (small table)")
        show_df(sec("q5e_installments_by_category"))

        st.subheader("Segmented by city (interactive focus)")
        by_city = sec("q5e_installments_by_city")
        if len(by_city) > 0:
            by_city = by_city.copy()
            by_city["city_id"] = by_city["detail_key"].astype(str).str.split("|").str[0]
            city_options = sorted(by_city["city_id"].dropna().unique().tolist())
            selected_city_id = st.selectbox("City id", city_options, index=0)
            focused = by_city[by_city["city_id"] == selected_city_id].copy()

            fig_city = px.bar(
                focused,
                x="detail_label",
                y="expected_profit",
                title=f"EV profit by installments — {selected_city_id}",
                color_discrete_sequence=[ACCENT],
            )
            fig_city.update_layout(xaxis_tickangle=-35)
            st.plotly_chart(style_plotly(fig_city), use_container_width=True)
            show_df(focused)
        else:
            st.info("No `q5e_installments_by_city` data.")

st.subheader("Optional: per-transaction sample (heavy)")
show_tx = st.checkbox("Load `q5_results` (per-transaction)", value=False)
if show_tx:
    if not p_tx.exists():
        st.error("Missing `q5_results` — run Gold Q5 first.")
    else:
        df_tx = pd.read_parquet(p_tx)
        show_df(df_tx.head(500))

if p_report.exists():
    with st.expander("Consulting-style business report (Markdown)"):
        st.markdown(p_report.read_text(encoding="utf-8"))
