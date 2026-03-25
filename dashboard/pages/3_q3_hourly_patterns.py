"""Q3 — Hourly patterns."""

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

st.title("Q3 — Peak hours")

p = Path(settings.gold_uri) / "q3_results"
if not p.exists():
    st.error("Run Gold Q3 first.")
    st.stop()
df = pd.read_parquet(p)

st.subheader("Executive view (peak hours by category)")
show_df(df)

overall_peak = df.loc[df["total_amount"].idxmax()]
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Peak hour (overall)", int(overall_peak["hour_of_day"]))
with col2:
    st.metric("Peak category", str(overall_peak["category"]))
with col3:
    st.metric("Peak total amount", f"{float(overall_peak['total_amount']):,.0f}")

categories = sorted(df["category"].dropna().unique().tolist())
selected_category = st.selectbox("Filter category", categories, index=0)
df_cat = df[df["category"] == selected_category].copy()
df_cat = df_cat.sort_values("hour_of_day")

left, right = st.columns(2)
with left:
    fig_bar = px.bar(
        df_cat,
        x="hour_of_day",
        y="total_amount",
        title=f"Top hours — category {selected_category}",
        color_discrete_sequence=[ACCENT],
    )
    st.plotly_chart(style_plotly(fig_bar), use_container_width=True)

with right:
    fig_line = px.line(
        df_cat,
        x="hour_of_day",
        y="total_amount",
        markers=True,
        title=f"Revenue curve — category {selected_category}",
        color_discrete_sequence=[ACCENT],
    )
    st.plotly_chart(style_plotly(fig_line), use_container_width=True)

st.subheader("Compare categories (top hours only)")
fig_cmp = px.bar(
    df,
    x="hour_of_day",
    y="total_amount",
    color="category",
    barmode="group",
    title="Top hours by category (purchase amount, top-3 hours per category)",
)
st.plotly_chart(style_plotly(fig_cmp), use_container_width=True)
