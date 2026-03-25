"""Q2 — Averages by state."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st

from config.settings import settings
from dashboard.components.charts import style_plotly
from dashboard.components.tables import show_df
from dashboard.components.style import ACCENT, apply_global_style
import plotly.express as px

apply_global_style()

st.title("Q2 — Average purchase by state")

p = Path(settings.gold_uri) / "q2_results"
if not p.exists():
    st.error("Run Gold Q2 first.")
    st.stop()
df = pd.read_parquet(p)

st.subheader("Executive view (KPIs + filters)")
df = df.dropna(subset=["State ID", "Average Amount"])

exclude_unknown_state = st.checkbox("Exclude unknown State ID (-1)", value=True)
df_states = df.copy()
if exclude_unknown_state:
    df_states = df_states[df_states["State ID"] != -1]

state_summary = (
    df_states.groupby("State ID")
    .agg(
        n_merchants=("Merchant", "nunique"),
        avg_of_avg=("Average Amount", "mean"),
    )
    .reset_index()
    .sort_values("avg_of_avg", ascending=False)
)

col_k1, col_k2, col_k3 = st.columns(3)
with col_k1:
    st.metric("States", int(state_summary["State ID"].nunique()))
with col_k2:
    st.metric("Top state (by mean avg)", str(state_summary.iloc[0]["State ID"]))
with col_k3:
    st.metric("Top state avg_of_avg", f"{state_summary.iloc[0]['avg_of_avg']:,.0f}")

top_states = state_summary.head(10)["State ID"].astype(str).tolist()
selected_state = st.selectbox("State ID (focus)", top_states, index=0)

df_state = df[df["State ID"].astype(str) == str(selected_state)].copy()
df_state_top = df_state.sort_values("Average Amount", ascending=False).head(15)

left, right = st.columns(2)
with left:
    st.subheader(f"Top merchants — State {selected_state}")
    show_df(df_state_top)
with right:
    st.subheader(f"Top merchants by Average Amount — State {selected_state}")
    fig = px.bar(
        df_state_top,
        x="Merchant",
        y="Average Amount",
        title=f"State {selected_state}: top merchants",
        color_discrete_sequence=[ACCENT],
    )
    fig.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(style_plotly(fig), use_container_width=True)

st.subheader("States leaderboard (top 10)")
state_top10 = state_summary.head(10).copy()
state_top10["State ID"] = state_top10["State ID"].astype(str)
ymin = float(state_top10["avg_of_avg"].min())
ymax = float(state_top10["avg_of_avg"].max())
fig2 = px.bar(
    state_top10,
    x="State ID",
    y="avg_of_avg",
    text="avg_of_avg",
    title="Top 10 states by mean of merchant average amounts",
    color_discrete_sequence=[ACCENT],
)
fig2.update_traces(texttemplate="%{text:,.1f}", textposition="outside", cliponaxis=False)
fig2.update_layout(
    xaxis_title="State ID",
    xaxis_tickangle=-45,
    yaxis_title="Mean of merchant averages",
    yaxis_range=[ymin - 5, ymax + 25],
    margin=dict(t=60, b=40, l=40, r=20),
)
st.plotly_chart(style_plotly(fig2), use_container_width=True)
