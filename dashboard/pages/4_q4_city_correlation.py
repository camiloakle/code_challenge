"""Q4 — City metrics, global top merchants, association, local ranks."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st
import plotly.express as px

from config.settings import settings
from dashboard.components.charts import style_plotly
from dashboard.components.tables import show_df
from dashboard.components.style import ACCENT, apply_global_style

apply_global_style()

st.title("Q4 — Cities, global merchants & category association")

root = Path(settings.gold_uri)
q4_root = root / "q4"
p = q4_root / "results"
if not p.exists():
    st.error("Run Gold Q4 first.")
    st.stop()

df = pd.read_parquet(p)
st.subheader("City metrics (volume)")
df["City"] = df["City"].astype(str)
top_cities = df.sort_values("total_transactions", ascending=False).head(10).copy()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Top city by transactions", top_cities.iloc[0]["City"])
with col2:
    st.metric("Top city transactions", f"{int(top_cities.iloc[0]['total_transactions']):,}")
with col3:
    st.metric("Total transactions (sum)", f"{int(df['total_transactions'].sum()):,}")

fig_city = px.bar(
    top_cities,
    x="City",
    y="total_transactions",
    title="Top 10 cities — transaction volume",
    color_discrete_sequence=[ACCENT],
)
fig_city.update_layout(xaxis_tickangle=-35)
st.plotly_chart(style_plotly(fig_city), use_container_width=True)
show_df(top_cities)

pa = q4_root / "city_category_association"
if pa.exists():
    st.subheader("City × category — Cramér's V (global, one row)")
    assoc = pd.read_parquet(pa)
    show_df(assoc)
    try:
        v = float(assoc["city_category_cramers_v"].iloc[0])
        st.info(f"Interpretación rápida: Cramér's V = {v:.3f} (asociación ciudad↔categoría).")
    except Exception:
        pass
else:
    st.caption("Re-run Gold Q4 for `q4/city_category_association`.")

pg = q4_root / "top_merchants_global"
if pg.exists():
    st.subheader("Top merchants globally + primary city")
    global_top = pd.read_parquet(pg).copy()
    # For dashboard/report visuals, exclude null merchant identity artifacts.
    if "merchant_id" in global_top.columns:
        global_top = global_top[global_top["merchant_id"].notna()].copy()
    if "merchant_name" in global_top.columns:
        global_top = global_top[global_top["merchant_name"].notna()].copy()
    show_df(global_top.head(50))
    fig_merch = px.bar(
        global_top.head(10),
        x="merchant_name",
        y="total_transactions",
        title="Top 10 merchants — total transactions",
        color_discrete_sequence=[ACCENT],
    )
    fig_merch.update_layout(xaxis_tickangle=-35, xaxis_tickfont_size=10, bargap=0.2)
    st.plotly_chart(style_plotly(fig_merch), use_container_width=True)
else:
    st.caption("Re-run Gold Q4 for `q4/top_merchants_global`.")

pdist = q4_root / "top_merchants_distribution_by_city"
if pdist.exists():
    st.subheader("Distribution of top-K merchants by primary city")
    dist = pd.read_parquet(pdist).copy()
    # For dashboard/report visuals, exclude null primary city artifacts.
    if "primary_city_id" in dist.columns:
        dist = dist[dist["primary_city_id"].notna()].copy()
    if "City" in dist.columns:
        dist = dist[dist["City"].notna()].copy()
    dist["pct_of_top_k"] = dist["pct_of_top_k"].astype(float)
    dist = dist.sort_values("pct_of_top_k", ascending=False)
    show_df(dist)
    fig_dist = px.bar(
        dist,
        x="City",
        y="pct_of_top_k",
        title="Share of top-K merchants by primary city",
        color_discrete_sequence=[ACCENT],
    )
    fig_dist.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(style_plotly(fig_dist), use_container_width=True)
else:
    st.caption("Re-run Gold Q4 for `q4/top_merchants_distribution_by_city`.")

p2 = q4_root / "merchant_popularity_by_city"
if p2.exists():
    st.subheader("Local: top merchants per city (complementary)")
    local = pd.read_parquet(p2).copy()
    cities = sorted(local["City"].dropna().unique().tolist())
    selected_city = st.selectbox("City (local popularity focus)", cities, index=0)
    local_city = (
        local[local["City"] == selected_city]
        .sort_values("transaction_count", ascending=False)
        .head(15)
    )
    show_df(local_city)
    fig_local = px.bar(
        local_city,
        x="merchant_name",
        y="transaction_count",
        title=f"Top local merchants — {selected_city}",
    )
    fig_local.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(style_plotly(fig_local), use_container_width=True)
else:
    st.caption("Re-run Gold Q4 for `q4/merchant_popularity_by_city`.")
