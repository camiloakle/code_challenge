"""Plotly helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

from dashboard.components.style import ACCENT


def style_plotly(fig: Figure, *, template: str = "plotly_dark") -> Figure:
    """Apply consistent Plotly styling (dark theme, grid, fonts, bar polish)."""
    fig.update_layout(
        template=template,
        font=dict(family="Inter, Arial, sans-serif", size=12, color="#e8eef7"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(t=60, b=50, l=50, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False)

    # Bar "forms": rounded corners + subtle border.
    fig.update_traces(
        selector={"type": "bar"},
        marker=dict(line=dict(width=0.8, color="rgba(255,255,255,0.18)")),
        opacity=0.95,
    )

    return fig


def bar_chart(df: pd.DataFrame, x: str, y: str, title: str) -> Any:
    """Simple bar chart."""
    fig = px.bar(df, x=x, y=y, title=title, color_discrete_sequence=[ACCENT])
    return style_plotly(fig)
