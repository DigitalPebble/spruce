#!/usr/bin/env python3
"""
Interactive dashboard for SPRUCE-enriched CUR output.

Run with:
    streamlit run reporting/dashboard.py -- --input output/
"""

import argparse
import base64
import html
import sys
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

try:
    import duckdb
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
except ImportError:
    sys.exit(
        "dashboard.py requires: pip install -r reporting/requirements-dashboard.txt"
    )

DEFAULT_TOP_N = 10
REGION_EXPR = "coalesce(nullif(region, ''), product_region_code)"
REPO_ROOT = Path(__file__).resolve().parents[1]
LOGO_PATH = REPO_ROOT / "logo.png"
STYLES_PATH = Path(__file__).resolve().parent / "styles.css"

BRAND_COLOR = "#BC6554"
BRAND_PALETTE = ("#BC6554", "#8C4A3D", "#E0A64A", "#5A7C8A", "#7A8E55")
COLORS = MappingProxyType(
    {
        "background": "#F7F6F2",
        "surface": "#F9F8F5",
        "sidebar": "#ECE6DC",
        "border": "#D4D1CA",
        "grid": "#DCD9D5",
        "text": "#28251D",
        "muted": "#6F6A61",
        "accent": BRAND_COLOR,
        "accent_dark": "#8C4A3D",
        "energy": "#E0A64A",
        "emissions": "#8C4A3D",
        "water": "#5A7C8A",
        "green": "#7A8E55",
    }
)

QUERY_CACHE_TTL_SECONDS = 300
CONNECTION_CACHE_MAX_ENTRIES = 2
PLOT_CONFIG = {"displaylogo": False, "scrollZoom": True}

DASHBOARD_COLUMN_DEFAULTS = MappingProxyType(
    {
        "BILLING_PERIOD": "VARCHAR",
        "region": "VARCHAR",
        "product_region_code": "VARCHAR",
        "line_item_product_code": "VARCHAR",
        "product_servicecode": "VARCHAR",
        "line_item_operation": "VARCHAR",
        "line_item_line_item_type": "VARCHAR",
        "line_item_unblended_cost": "DOUBLE",
        "pricing_public_on_demand_cost": "DOUBLE",
        "operational_energy_kwh": "DOUBLE",
        "operational_emissions_co2eq_g": "DOUBLE",
        "embodied_emissions_co2eq_g": "DOUBLE",
        "water_cooling_l": "DOUBLE",
        "water_electricity_production_l": "DOUBLE",
        "water_consumption_stress_area_l": "DOUBLE",
        "carbon_intensity": "DOUBLE",
        "power_usage_effectiveness": "DOUBLE",
    }
)


# ---------------------------------------------------------------------------
# CLI, data loading, and SQL helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "-i",
        "--input",
        default="",
        help="Path to enriched Parquet directory, glob, file, or S3 URI.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help="Number of top emitters to show.",
    )
    args, _ = parser.parse_known_args()
    return args


def normalize_parquet_path(input_path: str) -> str:
    path = input_path.strip().rstrip("/")
    if not path:
        return ""
    if path.endswith(".parquet") or any(char in path for char in "*?["):
        return path
    return f"{path}/**/*.parquet"


def sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def to_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if item is not None]
    if pd.isna(value):
        return []
    return [str(value)]


def filter_clause(
    periods: list[str], regions: list[str]
) -> tuple[str, tuple[str, ...]]:
    clauses = []
    params = []

    periods = [p for p in periods if p]
    if periods:
        clauses.append(f"BILLING_PERIOD IN ({','.join(['?'] * len(periods))})")
        params.extend(periods)

    regions = [r for r in regions if r]
    if regions:
        clauses.append(f"{REGION_EXPR} IN ({','.join(['?'] * len(regions))})")
        params.extend(regions)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, tuple(params)


def ensure_dashboard_columns(con: duckdb.DuckDBPyConnection) -> None:
    existing = {row[1] for row in con.execute("PRAGMA table_info('cur')").fetchall()}
    for column_name, column_type in DASHBOARD_COLUMN_DEFAULTS.items():
        if column_name not in existing:
            con.execute(f'ALTER TABLE cur ADD COLUMN "{column_name}" {column_type}')


@st.cache_resource(show_spinner=False, max_entries=CONNECTION_CACHE_MAX_ENTRIES)
def load_connection(input_path: str) -> duckdb.DuckDBPyConnection:
    parquet_path = normalize_parquet_path(input_path)
    if not parquet_path:
        raise ValueError("Provide a local path, glob, .parquet file, or s3:// URI.")

    con = duckdb.connect(":memory:")
    if parquet_path.startswith("s3://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")

    con.execute(
        """
        CREATE TABLE cur AS
        SELECT * FROM read_parquet(?, hive_partitioning=true)
        """,
        [parquet_path],
    )
    ensure_dashboard_columns(con)
    return con


@st.cache_data(show_spinner=False, ttl=QUERY_CACHE_TTL_SECONDS)
def query_df(
    input_path: str, sql: str, params: tuple[object, ...] = ()
) -> pd.DataFrame:
    con = load_connection(input_path)
    return con.execute(sql, params).df()


def get_filter_options(input_path: str) -> tuple[list[str], list[str]]:
    options = query_df(
        input_path,
        f"""
        SELECT
            array_agg(DISTINCT BILLING_PERIOD ORDER BY BILLING_PERIOD)
                FILTER (WHERE BILLING_PERIOD IS NOT NULL) AS periods,
            array_agg(DISTINCT {REGION_EXPR} ORDER BY {REGION_EXPR})
                FILTER (WHERE {REGION_EXPR} IS NOT NULL AND {REGION_EXPR} != '')
                AS regions
        FROM cur
        """,
    )
    row = options.iloc[0] if not options.empty else {}
    return to_string_list(row.get("periods")), to_string_list(row.get("regions"))


@st.cache_data(show_spinner=False, ttl=QUERY_CACHE_TTL_SECONDS)
def get_tag_keys(input_path: str) -> list[str]:
    try:
        return (
            query_df(
                input_path,
                """
            SELECT DISTINCT unnest(map_keys(resource_tags)) AS tag_key
            FROM cur
            WHERE resource_tags IS NOT NULL
            ORDER BY tag_key
            """,
            )["tag_key"]
            .dropna()
            .astype(str)
            .tolist()
        )
    except duckdb.Error:
        return []


# These query shapes mirror report.py so the dashboard and generated report stay
# comparable when metrics evolve.
def overview_query(where: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT * FROM cur {where}
        ), filtered_agg AS (
            SELECT
                count(*) AS row_count,
                sum(line_item_unblended_cost)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage') AS total_cost,
                sum(operational_energy_kwh) AS energy_kwh,
                sum(operational_emissions_co2eq_g) AS operational_g,
                sum(embodied_emissions_co2eq_g) AS embodied_g,
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0) AS water_l
            FROM filtered
        ), coverage AS (
            SELECT
                sum(line_item_unblended_cost)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage') AS usage_cost,
                sum(line_item_unblended_cost)
                    FILTER (
                        WHERE line_item_line_item_type LIKE '%Usage'
                          AND operational_emissions_co2eq_g IS NOT NULL
                    ) AS covered_cost
            FROM cur
        )
        SELECT
            row_count,
            round(coalesce(total_cost, 0), 2) AS total_cost,
            round(coalesce(energy_kwh, 0), 2) AS energy_kwh,
            round(coalesce(operational_g, 0) / 1000, 2) AS operational_kg,
            round(coalesce(embodied_g, 0) / 1000, 2) AS embodied_kg,
            round((coalesce(operational_g, 0) + coalesce(embodied_g, 0)) / 1000, 2)
                AS total_emissions_kg,
            round(coalesce(water_l, 0), 2) AS water_l,
            round(covered_cost * 100.0 / NULLIF(usage_cost, 0), 2) AS coverage_pct
        FROM filtered_agg
        CROSS JOIN coverage
    """


def billing_query(where: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT * FROM cur {where}
        )
        SELECT
            BILLING_PERIOD,
            round(
                sum(line_item_unblended_cost)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                2
            ) AS total_cost,
            round(
                sum(operational_energy_kwh)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                2
            ) AS energy_kwh,
            round(
                sum(operational_emissions_co2eq_g)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage') / 1000,
                2
            ) AS operational_kg,
            round(
                sum(embodied_emissions_co2eq_g)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage') / 1000,
                2
            ) AS embodied_kg,
            round(
                (
                    coalesce(
                        sum(operational_emissions_co2eq_g)
                            FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                        0
                    )
                    + coalesce(
                        sum(embodied_emissions_co2eq_g)
                            FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                        0
                    )
                ) / 1000,
                2
            ) AS total_emissions_kg,
            round(
                coalesce(
                    sum(water_cooling_l)
                        FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                    0
                )
                + coalesce(
                    sum(water_electricity_production_l)
                        FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                    0
                ),
                2
            ) AS water_usage_l
        FROM filtered
        GROUP BY BILLING_PERIOD
        ORDER BY BILLING_PERIOD
    """


def top_emitters_query(where: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT *, {REGION_EXPR} AS dashboard_region FROM cur {where}
        )
        SELECT
            coalesce(nullif(line_item_product_code, ''), 'Unknown product')
                AS line_item_product_code,
            coalesce(nullif(product_servicecode, ''), 'Unknown service')
                AS product_servicecode,
            coalesce(nullif(line_item_operation, ''), 'Unknown operation')
                AS line_item_operation,
            coalesce(nullif(dashboard_region, ''), 'Unknown region') AS region,
            round(
                sum(line_item_unblended_cost)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                2
            ) AS total_cost,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS co2_usage_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2) AS co2_embodied_kg,
            round(
                (
                    coalesce(sum(operational_emissions_co2eq_g), 0)
                    + coalesce(sum(embodied_emissions_co2eq_g), 0)
                ) / 1000,
                2
            ) AS total_emissions_kg,
            round(sum(operational_energy_kwh), 2) AS energy_kwh,
            round(
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0),
                2
            ) AS water_usage_l
        FROM filtered
        WHERE operational_emissions_co2eq_g IS NOT NULL
        GROUP BY 1, 2, 3, 4
        ORDER BY total_emissions_kg DESC, co2_usage_kg DESC, energy_kwh DESC
        LIMIT ?
    """


def regional_query(where: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT *, {REGION_EXPR} AS dashboard_region FROM cur {where}
        ), agg AS (
            SELECT
                coalesce(nullif(dashboard_region, ''), 'Unknown region') AS region,
                sum(operational_emissions_co2eq_g) AS operational_g,
                sum(embodied_emissions_co2eq_g) AS embodied_g,
                sum(operational_energy_kwh) AS energy_kwh,
                sum(line_item_unblended_cost)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage') AS usage_cost,
                avg(carbon_intensity) AS avg_ci,
                avg(power_usage_effectiveness) AS pue,
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0) AS water_l,
                coalesce(sum(water_consumption_stress_area_l), 0) AS water_stress_l
            FROM filtered
            WHERE operational_emissions_co2eq_g IS NOT NULL
            GROUP BY 1
        )
        SELECT
            region,
            round(coalesce(operational_g, 0) / 1000, 2) AS co2_usage_kg,
            round(coalesce(embodied_g, 0) / 1000, 2) AS co2_embodied_kg,
            round((coalesce(operational_g, 0) + coalesce(embodied_g, 0)) / 1000, 2)
                AS total_emissions_kg,
            round(coalesce(energy_kwh, 0), 2) AS energy_kwh,
            round(avg_ci, 2) AS carbon_intensity,
            round(water_l, 2) AS water_usage_l,
            round(water_stress_l, 2) AS water_stress_area_l,
            round(pue, 2) AS pue,
            round(
                (coalesce(operational_g, 0) + coalesce(embodied_g, 0))
                    / NULLIF(usage_cost, 0),
                2
            )
                AS g_co2_per_dollar
        FROM agg
        ORDER BY total_emissions_kg DESC, energy_kwh DESC, region ASC
    """


def tag_breakdown_query(where: str, tag_key: str) -> str:
    key = sql_literal(tag_key)
    return f"""
        WITH filtered AS (
            SELECT * FROM cur {where}
        )
        SELECT
            resource_tags[{key}] AS tag_value,
            round(
                sum(line_item_unblended_cost)
                    FILTER (WHERE line_item_line_item_type LIKE '%Usage'),
                2
            ) AS total_cost,
            round(sum(operational_energy_kwh), 2) AS energy_kwh,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS operational_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2) AS embodied_kg,
            round(
                (
                    coalesce(sum(operational_emissions_co2eq_g), 0)
                    + coalesce(sum(embodied_emissions_co2eq_g), 0)
                ) / 1000,
                2
            ) AS total_emissions_kg,
            round(
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0),
                2
            ) AS water_usage_l
        FROM filtered
        WHERE resource_tags[{key}] IS NOT NULL
          AND resource_tags[{key}] != ''
        GROUP BY 1
    """


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def configure_branding() -> None:
    page_icon = str(LOGO_PATH) if LOGO_PATH.exists() else None
    st.set_page_config(
        page_title="SPRUCE Dashboard",
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    if LOGO_PATH.exists() and hasattr(st, "logo"):
        st.logo(str(LOGO_PATH))


def load_css() -> None:
    if STYLES_PATH.exists():
        st.markdown(
            f"<style>{STYLES_PATH.read_text(encoding='utf-8')}</style>",
            unsafe_allow_html=True,
        )


def logo_data_uri() -> str:
    if not LOGO_PATH.exists():
        return ""
    encoded = base64.b64encode(LOGO_PATH.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def render_header() -> None:
    logo_src = logo_data_uri()
    logo_html = (
        f'<img class="spruce-logo" src="{logo_src}" alt="SPRUCE" />'
        if logo_src
        else ""
    )
    st.markdown(
        f"""
        <div class="spruce-header">
            {logo_html}
            <div>
                <h1>SPRUCE dashboard</h1>
                <p>Scalable Platform for Reporting Usage and Cloud Emissions</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_html(markup: str) -> None:
    if hasattr(st, "html"):
        st.html(markup)
        return
    st.markdown(markup, unsafe_allow_html=True)


def metric_value(value: object, suffix: str = "", default: str = "n/a") -> str:
    if value is None or pd.isna(value):
        return default
    return f"{float(value):,.2f}{suffix}"


def compact_metric_value(value: object, suffix: str = "", default: str = "n/a") -> str:
    if value is None or pd.isna(value):
        return default
    number = float(value)
    if abs(number) >= 1_000_000:
        return f"{number / 1_000_000:,.2f}M{suffix}"
    if abs(number) >= 1_000:
        return f"{number / 1_000:,.2f}K{suffix}"
    return f"{number:,.2f}{suffix}"


def chart_label(*parts: object, max_length: int = 72) -> str:
    label = " / ".join(str(part) for part in parts if part is not None and part != "")
    if not label:
        return "Unknown"
    if len(label) <= max_length:
        return label
    return f"{label[: max_length - 3]}..."


def render_metric_cards(cards: list[tuple[str, str, str]]) -> None:
    card_html = []
    for label, value, meta in cards:
        meta_html = f"<div class='spruce-card-meta'>{html.escape(meta)}</div>" if meta else ""
        card_html.append(
            f"""
            <div class="spruce-card">
                <div class="spruce-card-label">{html.escape(label)}</div>
                <div class="spruce-card-value">{html.escape(value)}</div>
                {meta_html}
            </div>
            """
        )
    render_html(f"<div class='spruce-card-grid'>{''.join(card_html)}</div>")


def render_table(
    data: pd.DataFrame,
    columns: list[str],
    labels: dict[str, str],
    *,
    max_rows: int | None = None,
) -> None:
    if data.empty:
        st.info("No rows found for the current filters.")
        return

    table = data[columns].copy()
    if max_rows is not None:
        table = table.head(max_rows)

    column_config = {}
    for col in columns:
        label = labels.get(col, col)
        if pd.api.types.is_float_dtype(table[col]):
            column_config[col] = st.column_config.NumberColumn(label, format="%.2f")
        else:
            column_config[col] = st.column_config.TextColumn(label)

    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )


def style_plotly(fig: go.Figure, height: int = 420, legend_y: float = -0.22) -> go.Figure:
    axis_style = {
        "gridcolor": COLORS["grid"],
        "zerolinecolor": COLORS["grid"],
        "linecolor": COLORS["border"],
        "tickfont": {"color": COLORS["text"]},
        "title_font": {"color": COLORS["text"]},
    }
    fig.update_layout(
        paper_bgcolor=COLORS["surface"],
        plot_bgcolor=COLORS["surface"],
        font={"color": COLORS["text"], "family": "Inter, Segoe UI, sans-serif"},
        height=height,
        margin={"l": 56, "r": 28, "t": 34, "b": 58},
        hoverlabel={"bgcolor": "#FFFFFF", "font_color": COLORS["text"]},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": legend_y,
            "xanchor": "left",
            "x": 0,
            "font": {"color": COLORS["text"]},
            "title": {"font": {"color": COLORS["text"]}},
        },
    )
    fig.update_xaxes(**axis_style)
    fig.update_yaxes(**axis_style)
    return fig


def line_area_chart(
    data: pd.DataFrame,
    value_column: str,
    title: str,
    y_title: str,
    color: str,
    height: int = 390,
) -> go.Figure:
    rgba_fill = {
        COLORS["energy"]: "rgba(224, 166, 74, 0.18)",
        COLORS["emissions"]: "rgba(140, 74, 61, 0.18)",
        COLORS["water"]: "rgba(90, 124, 138, 0.18)",
    }.get(color, "rgba(188, 101, 84, 0.16)")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["BILLING_PERIOD"],
            y=data[value_column],
            mode="lines+markers",
            line={"color": color, "width": 3},
            marker={"color": color, "size": 7},
            fill="tozeroy",
            fillcolor=rgba_fill,
            name=title,
            hovertemplate="%{x}<br>%{y:,.2f}<extra></extra>",
        )
    )
    fig.update_layout(title={"text": title, "x": 0, "xanchor": "left"})
    fig.update_xaxes(title="Billing period")
    fig.update_yaxes(title=y_title)
    return style_plotly(fig, height=height)


# ---------------------------------------------------------------------------
# Tab renderers
# ---------------------------------------------------------------------------


def render_overview(overview: pd.DataFrame) -> None:
    if overview.empty:
        st.info("No data available for the current filters.")
        return

    row = overview.iloc[0]
    render_metric_cards(
        [
            ("Total usage cost", metric_value(row["total_cost"], " USD"), ""),
            ("Energy", metric_value(row["energy_kwh"], " kWh"), ""),
            ("Operational emissions", metric_value(row["operational_kg"], " kg"), ""),
            ("Embodied emissions", metric_value(row["embodied_kg"], " kg"), ""),
            ("Water", metric_value(row["water_l"], " L"), ""),
            ("Coverage (dataset-wide)", metric_value(row["coverage_pct"], " %"), ""),
        ]
    )


def render_trend(trend: pd.DataFrame) -> None:
    if trend.empty:
        st.info("No billing period data found for the current filters.")
        return

    trend = trend.fillna(0)
    st.plotly_chart(
        line_area_chart(trend, "energy_kwh", "Energy", "kWh", COLORS["energy"], 410),
        use_container_width=True,
        config=PLOT_CONFIG,
    )
    st.plotly_chart(
        line_area_chart(
            trend,
            "total_emissions_kg",
            "Emissions",
            "kg CO2e",
            COLORS["emissions"],
            410,
        ),
        use_container_width=True,
        config=PLOT_CONFIG,
    )
    st.plotly_chart(
        line_area_chart(trend, "water_usage_l", "Water", "L", COLORS["water"], 410),
        use_container_width=True,
        config=PLOT_CONFIG,
    )

    render_table(
        trend,
        [
            "BILLING_PERIOD",
            "total_cost",
            "energy_kwh",
            "total_emissions_kg",
            "operational_kg",
            "embodied_kg",
            "water_usage_l",
        ],
        {
            "BILLING_PERIOD": "Billing period",
            "total_cost": "Cost (USD)",
            "energy_kwh": "Energy (kWh)",
            "total_emissions_kg": "Emissions (kg CO2e)",
            "operational_kg": "Operational kg",
            "embodied_kg": "Embodied kg",
            "water_usage_l": "Water (L)",
        },
    )


def render_top_emitters(emitters: pd.DataFrame) -> None:
    if emitters.empty:
        st.info("No emitters found for the current filters.")
        return

    table = emitters.copy()
    table["emitter"] = table.apply(
        lambda row: chart_label(
            row["line_item_product_code"],
            row["product_servicecode"],
            row["line_item_operation"],
        ),
        axis=1,
    )
    table = table.sort_values("total_emissions_kg", ascending=False)

    top = table.iloc[0]
    render_metric_cards(
        [
            (
                "Largest emitter",
                compact_metric_value(top["total_emissions_kg"], " kg"),
                str(top["emitter"]),
            ),
            (
                "Operational emissions",
                metric_value(table["co2_usage_kg"].sum(), " kg"),
                f"Top {len(table)} rows",
            ),
            (
                "Usage cost",
                metric_value(table["total_cost"].sum(), " USD"),
                "Top emitters only",
            ),
        ]
    )
    render_table(
        table,
        [
            "emitter",
            "region",
            "total_cost",
            "total_emissions_kg",
            "co2_usage_kg",
            "co2_embodied_kg",
            "energy_kwh",
            "water_usage_l",
        ],
        {
            "emitter": "Emitter",
            "region": "Region",
            "total_cost": "Cost (USD)",
            "total_emissions_kg": "Emissions (kg CO2e)",
            "co2_usage_kg": "Operational kg",
            "co2_embodied_kg": "Embodied kg",
            "energy_kwh": "Energy (kWh)",
            "water_usage_l": "Water (L)",
        },
    )


def render_regions(regions: pd.DataFrame) -> None:
    if regions.empty:
        st.info("No regional data found for the current filters.")
        return

    regions = regions.sort_values("total_emissions_kg", ascending=False)
    fig = px.pie(
        regions,
        values="total_emissions_kg",
        names="region",
        hole=0.45,
        color_discrete_sequence=[
            BRAND_COLOR,
            COLORS["accent_dark"],
            COLORS["energy"],
            COLORS["water"],
            COLORS["green"],
        ],
    )
    fig.update_traces(
        textinfo="percent+label",
        marker={"line": {"color": COLORS["surface"], "width": 2}},
        hovertemplate="%{label}<br>%{value:,.2f} kg CO2e<br>%{percent}<extra></extra>",
    )
    fig.update_layout(
        paper_bgcolor=COLORS["surface"],
        plot_bgcolor=COLORS["surface"],
        font={"color": COLORS["text"], "family": "Inter, Segoe UI, sans-serif"},
        height=430,
        margin={"l": 24, "r": 24, "t": 20, "b": 20},
        legend={"font": {"color": COLORS["text"]}},
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG)

    region_cards = []
    for _, row in regions.iterrows():
        region_cards.append(
            (
                str(row["region"]),
                metric_value(row["total_emissions_kg"], " kg"),
                f"{metric_value(row['energy_kwh'], ' kWh')} - {metric_value(row['water_usage_l'], ' L')}",
            )
        )
    render_metric_cards(region_cards)
    render_table(
        regions,
        [
            "region",
            "total_emissions_kg",
            "co2_usage_kg",
            "co2_embodied_kg",
            "energy_kwh",
            "water_usage_l",
            "carbon_intensity",
            "pue",
            "g_co2_per_dollar",
        ],
        {
            "region": "Region",
            "total_emissions_kg": "Emissions (kg CO2e)",
            "co2_usage_kg": "Operational kg",
            "co2_embodied_kg": "Embodied kg",
            "energy_kwh": "Energy (kWh)",
            "water_usage_l": "Water (L)",
            "carbon_intensity": "Carbon intensity",
            "pue": "PUE",
            "g_co2_per_dollar": "gCO2 per USD",
        },
    )


def render_tags(
    input_path: str,
    where: str,
    params: tuple[object, ...],
    tag_key: str,
    allowed_tag_keys: list[str],
) -> None:
    if not tag_key:
        st.info("Select a resource tag key in the sidebar to chart tag values.")
        return

    if tag_key not in allowed_tag_keys:
        st.error(f"Tag '{tag_key}' is not present in the loaded data.")
        return

    try:
        tags = query_df(input_path, tag_breakdown_query(where, tag_key), params)
    except duckdb.Error as exc:
        st.error(f"Could not read resource_tags for tag '{tag_key}': {exc}")
        return

    if tags.empty:
        st.info("No non-empty values found for the selected tag and filters.")
        return

    metric = st.radio(
        "Metric",
        ["emissions", "energy", "cost", "water"],
        horizontal=True,
        label_visibility="collapsed",
    )
    metric_column = {
        "emissions": "total_emissions_kg",
        "energy": "energy_kwh",
        "cost": "total_cost",
        "water": "water_usage_l",
    }[metric]
    metric_label = {
        "emissions": "Emissions (kg CO2e)",
        "energy": "Energy (kWh)",
        "cost": "Cost (USD)",
        "water": "Water (L)",
    }[metric]
    metric_color = {
        "emissions": COLORS["emissions"],
        "energy": COLORS["energy"],
        "cost": BRAND_COLOR,
        "water": COLORS["water"],
    }[metric]

    tags = tags.copy()
    tags["tag_value_label"] = tags["tag_value"].fillna("(missing)").replace("", "(empty)")
    tags = tags.sort_values(metric_column, ascending=False)

    fig = px.bar(
        tags,
        x=metric_column,
        y="tag_value_label",
        orientation="h",
        color_discrete_sequence=[metric_color],
        labels={metric_column: metric_label, "tag_value_label": "Tag value"},
    )
    fig.update_traces(marker={"line": {"color": COLORS["surface"], "width": 1}})
    fig.update_yaxes(categoryorder="array", categoryarray=list(reversed(tags["tag_value_label"])))
    styled_fig = style_plotly(fig, height=max(420, 38 * len(tags) + 120), legend_y=-0.35)
    styled_fig.update_layout(showlegend=False, margin={"l": 180, "r": 28, "t": 34, "b": 64})
    st.plotly_chart(styled_fig, use_container_width=True, config=PLOT_CONFIG)

    render_table(
        tags,
        [
            "tag_value_label",
            "total_emissions_kg",
            "energy_kwh",
            "total_cost",
            "water_usage_l",
            "operational_kg",
            "embodied_kg",
        ],
        {
            "tag_value_label": "Tag value",
            "total_emissions_kg": "Emissions (kg CO2e)",
            "energy_kwh": "Energy (kWh)",
            "total_cost": "Cost (USD)",
            "water_usage_l": "Water (L)",
            "operational_kg": "Operational kg",
            "embodied_kg": "Embodied kg",
        },
    )


# ---------------------------------------------------------------------------
# Sidebar, orchestration, and app entrypoint
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FilterSelection:
    input_path: str
    selected_periods: tuple[str, ...]
    selected_regions: tuple[str, ...]
    tag_key: str
    top_n: int


@dataclass(frozen=True)
class DashboardData:
    overview: pd.DataFrame
    trend: pd.DataFrame
    emitters: pd.DataFrame
    regions: pd.DataFrame


def render_input_sidebar(args: argparse.Namespace) -> tuple[str, int]:
    with st.sidebar:
        st.header("Input")
        input_path = st.text_input(
            "Enriched Parquet path",
            value=args.input,
            placeholder="output/ or s3://bucket/prefix/",
        )
        default_top_n = min(50, max(5, args.top_n))
        top_n = st.slider(
            "Top emitters", min_value=5, max_value=50, value=default_top_n
        )
    return input_path, int(top_n)


def render_filters_sidebar(
    periods: list[str], regions: list[str], tag_keys: list[str]
) -> tuple[list[str], list[str], str]:
    with st.sidebar:
        st.header("Filters")
        selected_periods = st.multiselect(
            "Billing period",
            periods,
            default=[],
            placeholder="All billing periods",
            help="Leave empty to include all billing periods.",
        )
        selected_regions = st.multiselect(
            "Region",
            regions,
            default=[],
            placeholder="All regions",
            help="Leave empty to include all regions.",
        )
        tag_key = st.selectbox(
            "Resource tag", [""] + tag_keys, format_func=lambda x: x or "None"
        )
    return selected_periods, selected_regions, tag_key


def run_queries(selection: FilterSelection) -> DashboardData:
    where, params = filter_clause(
        list(selection.selected_periods), list(selection.selected_regions)
    )
    overview = query_df(selection.input_path, overview_query(where), params)
    trend = query_df(selection.input_path, billing_query(where), params)
    emitters = query_df(
        selection.input_path,
        top_emitters_query(where),
        params + (max(1, int(selection.top_n)),),
    )
    regions = query_df(selection.input_path, regional_query(where), params)
    return DashboardData(
        overview=overview, trend=trend, emitters=emitters, regions=regions
    )


def main() -> None:
    args = parse_args()
    configure_branding()
    load_css()
    render_header()

    input_path, top_n = render_input_sidebar(args)
    if not input_path:
        st.warning("Enter a SPRUCE-enriched Parquet path or run with --input.")
        st.stop()

    try:
        load_connection(input_path)
        periods, regions = get_filter_options(input_path)
        tag_keys = get_tag_keys(input_path)
    except (duckdb.Error, ValueError, OSError) as exc:
        st.error(f"Failed to load enriched Parquet data: {exc}")
        st.stop()

    selected_periods, selected_regions, tag_key = render_filters_sidebar(
        periods, regions, tag_keys
    )
    selection = FilterSelection(
        input_path=input_path,
        selected_periods=tuple(selected_periods),
        selected_regions=tuple(selected_regions),
        tag_key=tag_key,
        top_n=top_n,
    )

    try:
        data = run_queries(selection)
    except duckdb.Error as exc:
        st.error(f"Dashboard query failed: {exc}")
        st.stop()

    where, params = filter_clause(selected_periods, selected_regions)
    overview_tab, trend_tab, emitters_tab, regions_tab, tags_tab = st.tabs(
        ["Overview", "Trend", "Top emitters", "Regions", "Tags"]
    )
    with overview_tab:
        render_overview(data.overview)
    with trend_tab:
        render_trend(data.trend)
    with emitters_tab:
        render_top_emitters(data.emitters)
    with regions_tab:
        render_regions(data.regions)
    with tags_tab:
        render_tags(input_path, where, params, tag_key, tag_keys)


if __name__ == "__main__":
    main()
