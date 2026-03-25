"""Table display helpers."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def show_df(df: pd.DataFrame) -> None:
    """Render dataframe with Streamlit."""
    # For small tables we prefer `st.table` (static) because it renders consistently
    # across themes and is easier to read than the interactive grid.
    if df is None:
        st.info("No data.")
        return
    try:
        n_rows = len(df)
    except Exception:
        n_rows = 0

    if n_rows <= 200:
        st.table(df.head(200))
    else:
        st.dataframe(df, use_container_width=True, hide_index=True, height=600)
