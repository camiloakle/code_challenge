"""Q1 — Month / City / Merchant totals."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st
import plotly.express as px

from config.settings import settings
from dashboard.components.charts import bar_chart
from dashboard.components.charts import style_plotly
from dashboard.components.tables import show_df
from dashboard.components.style import ACCENT, apply_global_style

apply_global_style()

st.title("Q1 — Merchant totals")

p = Path(settings.gold_uri) / "q1_results"
if not p.exists():
    st.error("Missing Gold output: run `python -m pipelines.runner --question q1`")
    st.stop()
df = pd.read_parquet(p)

st.subheader("Executive view (filters + KPIs)")
months = sorted(df["Month"].dropna().unique().tolist())
cities = sorted(df["City"].dropna().unique().tolist())

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
with col_kpi1:
    st.metric("Months", len(months))
with col_kpi2:
    st.metric("Cities", len(cities))
with col_kpi3:
    st.metric("Merchants (unique)", df["Merchant"].nunique())
with col_kpi4:
    st.metric("Total Purchase Total", f"{df['Purchase Total'].sum():,.0f}")

st.caption("Q1 output already contains only the top 5 merchants per (Month, City).")

filters = st.columns(3)
with filters[0]:
    month_sel = st.selectbox("Month", months, index=min(3, len(months) - 1))
with filters[1]:
    city_sel = st.selectbox("City", cities, index=0)
with filters[2]:
    top_k = st.slider("Top K to show", min_value=3, max_value=5, value=5)

df_fc = df[(df["Month"] == month_sel) & (df["City"] == city_sel)].copy()
df_fc = df_fc.sort_values("Purchase Total", ascending=False).head(top_k)

left, right = st.columns(2)
with left:
    st.subheader(f"Top merchants in {city_sel} ({month_sel})")
    show_df(df_fc)
with right:
    st.subheader("Purchase total by merchant")
    fig = px.bar(
        df_fc,
        x="Merchant",
        y="Purchase Total",
        title=f"Top {top_k} merchants — {city_sel} — {month_sel}",
        color_discrete_sequence=[ACCENT],
    )
    fig.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(style_plotly(fig), use_container_width=True)

st.subheader("Trend (Top-5 aggregate) — by month")
df_city_trend = (
    df[df["City"] == city_sel]
    .groupby("Month", as_index=False)
    .agg({"Purchase Total": "sum"})
    .sort_values("Month")
)
fig2 = px.line(
    df_city_trend,
    x="Month",
    y="Purchase Total",
    markers=True,
    title=f"Top-5 merchants aggregate purchase total — {city_sel}",
    color_discrete_sequence=[ACCENT],
)
st.plotly_chart(style_plotly(fig2), use_container_width=True)
