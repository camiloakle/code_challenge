"""Streamlit filters."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def month_filter(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Filter dataframe by selected month values."""
    if column not in df.columns:
        return df
    months = sorted(df[column].dropna().unique().tolist())
    choice = st.multiselect("Month", months, default=months[: min(3, len(months))])
    if not choice:
        return df
    return df[df[column].isin(choice)]
