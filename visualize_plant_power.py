# Copyright 2025 Steven Michiels
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#!/usr/bin/env python3

"""Visualize SEMS plant power data from the CSV export using Plotly.

Usage
-----
CLI:
    python visualize_plant_power.py --series PCurve_Power_PV --resample 15min

Notebook:
    from visualize_plant_power import visualize
    df, fig = visualize(
        series=["PCurve_Power_PV"],
        resample="15min",
        renderer="notebook_connected",
        show=False,
    )
    fig
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple, Union

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


DEFAULT_CSV = Path("json_export_example/plant_power_v2.csv")

SERIES_ALIASES = {"PCurve_Power_PV": "Power [W]"}


def load_dataset(csv_path: Path) -> pd.DataFrame:
    """Load the plant power CSV and normalize columns for plotting."""
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"No rows found in {csv_path}.")

    if "day" in df.columns:
        df["day"] = pd.to_datetime(df["day"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if {"day", "timestamp"}.issubset(df.columns):
        day_str = df["day"].dt.strftime("%Y-%m-%d")
        timestamp_str = df["timestamp"].astype(str).str.strip()
        df["period_start"] = pd.to_datetime(
            day_str + " " + timestamp_str,
            format="%Y-%m-%d %H:%M",
            errors="coerce",
        )
        missing_periods = df["period_start"].isna() & df["day"].notna()
        df.loc[missing_periods, "period_start"] = df.loc[missing_periods, "day"]
    elif "day" in df.columns:
        df["period_start"] = df["day"]
    elif "timestamp" in df.columns:
        df["period_start"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df


def resample_time_series(df: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """Resample the time series to the requested frequency per series."""
    if "period_start" not in df.columns:
        raise KeyError("Cannot resample without a 'period_start' column.")
    if frequency is None:
        return df

    resampled = (
        df.set_index("period_start")
        .groupby("series")["value"]
        .resample(frequency)
        .mean()
        .reset_index()
    )

    resampled["day"] = resampled["period_start"].dt.normalize()
    resampled["timestamp"] = resampled["period_start"].dt.strftime("%H:%M")
    return resampled


def build_figure(df: pd.DataFrame, *, slider: bool = True) -> go.Figure:
    """Create a Plotly line chart grouped by series."""
    x_column = "period_start" if "period_start" in df.columns else "day"
    required = {"value", "series", x_column}
    missing = required - set(df.columns)
    if missing:
        raise KeyError("CSV is missing required columns: " + ", ".join(sorted(missing)))

    filtered = df.dropna(subset=["value", "series"])
    if x_column in filtered.columns:
        filtered = filtered.dropna(subset=[x_column])
        filtered = filtered.sort_values(x_column)

    fig = px.line(
        filtered,
        x=x_column,
        y="value",
        color="series",
        markers=True,
        title="SEMS Plant Power Metrics",
        labels={
            x_column: "Period" if x_column == "period_start" else x_column.title(),
            "value": "Power [W]",
            "series": "Power_W",
        },
    )

    fig.update_layout(hovermode="x unified")

    if slider:
        fig.update_xaxes(
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=[
                    dict(count=7, label="1w", step="day", stepmode="backward"),
                    dict(count=14, label="2w", step="day", stepmode="backward"),
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(step="all", label="All"),
                ]
            ),
        )

    return fig


def apply_series_aliases(fig: go.Figure) -> go.Figure:
    """Rename legend entries using aliases if defined."""
    for trace in fig.data:
        alias = SERIES_ALIASES.get(trace.name)
        if not alias:
            continue
        hovertemplate = trace.hovertemplate
        if hovertemplate:
            hovertemplate = hovertemplate.replace(trace.name, alias)
        trace.update(name=alias, legendgroup=alias, hovertemplate=hovertemplate)
    return fig


def _parse_series_args(series_args: Optional[Iterable[str]]) -> Optional[list[str]]:
    if not series_args:
        return None
    series: list[str] = []
    for entry in series_args:
        parts = [item.strip() for item in entry.split(",") if item.strip()]
        series.extend(parts)
    return series or None


def visualize(
    csv_path: Union[str, Path] = DEFAULT_CSV,
    *,
    output: Optional[Path] = None,
    renderer: Optional[str] = None,
    show: bool = True,
    series: Optional[Sequence[str]] = None,
    resample: Optional[str] = None,
    slider: bool = True,
) -> Tuple[pd.DataFrame, go.Figure]:
    """Load the CSV, build the figure, and optionally display/save it."""
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = load_dataset(csv_path)

    if series:
        selected = set(series)
        df = df[df["series"].isin(selected)]
        if df.empty:
            raise ValueError(
                "No rows remain after filtering for series: "
                + ", ".join(sorted(selected))
            )

    if resample:
        df = resample_time_series(df, resample)

    fig = build_figure(df, slider=slider)
    fig = apply_series_aliases(fig)

    if renderer is not None:
        from plotly import io as pio

        pio.renderers.default = renderer

    if output is not None:
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(output)
        print(f"Saved figure to {output}")

    if show:
        fig.show()

    return df, fig


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Visualize SEMS plant power CSV data using Plotly."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=DEFAULT_CSV,
        type=Path,
        help="Path to the plant_power_v2 CSV file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Optional path to save the figure as an interactive HTML file. "
            "If omitted, the figure is only displayed."
        ),
    )
    parser.add_argument(
        "--renderer",
        help=(
            "Set a specific Plotly renderer (e.g. 'browser', 'notebook_connected')."
        ),
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Skip calling fig.show(); handy for batch or headless runs.",
    )
    parser.add_argument(
        "--series",
        action="append",
        help=(
            "Series to include. Repeat the option or provide a comma-separated list."
        ),
    )
    parser.add_argument(
        "--resample",
        help=(
            "Pandas resample rule applied per series (e.g. '15min' for quarter-hour)."
        ),
    )
    parser.add_argument(
        "--no-slider",
        action="store_true",
        help="Disable the x-axis range slider and quick range buttons.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    visualize(
        csv_path=args.csv_path,
        output=args.output,
        renderer=args.renderer,
        show=not args.no_show,
        series=_parse_series_args(args.series),
        resample=args.resample,
        slider=not args.no_slider,
    )


if __name__ == "__main__":
    main()
