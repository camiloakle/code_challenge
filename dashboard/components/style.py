"""Dashboard styling helpers (global CSS + Plotly aesthetics)."""

from __future__ import annotations

import streamlit as st

# Brand accent used across the dashboard.
ACCENT = "#00CC96"


def apply_global_style() -> None:
    """Inject a minimal dark theme + typography for a more 'professional' look."""
    st.markdown(
        """
<style>
/* Background */
html, body, [data-testid="stAppViewContainer"] {
  background-color: #0b0f17;
  color: #e8eef7;
}

/* Titles */
[data-testid="stHeader"] h1, [data-testid="stHeader"] h2, h1, h2, h3 {
  color: #e8eef7;
}

/* Plotly embeds often need a consistent background */
.stPlotlyChart iframe, .stPlotlyChart > div {
  background: transparent !important;
}

/* Metric cards */
.stMetric {
  border-radius: 12px;
  padding: 12px 14px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
}

.stMetricLabel {
  color: rgba(232, 238, 247, 0.75) !important;
}
.stMetricValue {
  color: #e8eef7 !important;
}
</style>
""",
        unsafe_allow_html=True,
    )

