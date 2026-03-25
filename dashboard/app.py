"""Streamlit entry — reads Gold Parquet outputs."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from config.settings import settings
from dashboard.components.style import apply_global_style

apply_global_style()

st.set_page_config(page_title="Billups OOH", layout="wide")
st.title("Billups OOH — Gold layer preview")

gold = Path(settings.gold_uri)
if not gold.exists():
    st.warning("Run pipelines first: `make run-all` or `python -m pipelines.runner --all`.")
    st.stop()

st.subheader("Gold datasets")
for sub in sorted(gold.iterdir()):
    if sub.is_dir():
        st.write(f"- `{sub.name}`")

st.info("Use the sidebar pages for Q1–Q5 detail views.")
