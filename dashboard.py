#!/usr/bin/env python3
"""
Interactive dashboard for SPRUCE-enriched CUR output.

Run with:
    streamlit run dashboard.py -- --input output/
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

try:
    import duckdb
    import pandas as pd
    import streamlit as st
except ImportError:
    sys.exit("dashboard.py requires: pip install -r requirements-dashboard.txt")

DEFAULT_TOP_N = 10
REGION_EXPR = "coalesce(nullif(region, ''), product_region_code)"
LOGO_PATH = Path(__file__).resolve().parent / "logo.png"
BRAND_COLOR = "#BC6554"
BRAND_PALETTE = ("#BC6554", "#8C4A3D", "#E08A78", "#5A2E26")
QUERY_CACHE_TTL_SECONDS = 300
CONNECTION_CACHE_MAX_ENTRIES = 2

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
            round(sum(operational_energy_kwh), 2) AS energy_kwh,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS operational_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2) AS embodied_kg,
            round(
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0),
                2
            ) AS water_usage_l
        FROM filtered
        GROUP BY BILLING_PERIOD
        ORDER BY BILLING_PERIOD
    """


def top_emitters_query(where: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT * FROM cur {where}
        )
        SELECT
            line_item_product_code,
            product_servicecode,
            line_item_operation,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS co2_usage_kg,
            round(sum(operational_energy_kwh), 2) AS energy_kwh,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2) AS co2_embodied_kg,
            round(
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0),
                2
            ) AS water_usage_l
        FROM filtered
        WHERE operational_emissions_co2eq_g IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY co2_usage_kg DESC, co2_embodied_kg DESC, energy_kwh DESC, line_item_operation
        LIMIT ?
    """


def regional_query(where: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT *, {REGION_EXPR} AS dashboard_region FROM cur {where}
        ), agg AS (
            SELECT
                dashboard_region AS region,
                sum(operational_emissions_co2eq_g) AS operational_g,
                sum(embodied_emissions_co2eq_g) AS embodied_g,
                sum(operational_energy_kwh) AS energy_kwh,
                sum(pricing_public_on_demand_cost) AS public_cost,
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
            round(operational_g / 1000, 2) AS co2_usage_kg,
            round(energy_kwh, 2) AS energy_kwh,
            round(avg_ci, 2) AS carbon_intensity,
            round(water_l, 2) AS water_usage_l,
            round(water_stress_l, 2) AS water_stress_area_l,
            round(pue, 2) AS pue,
            round((operational_g + embodied_g) / NULLIF(public_cost, 0), 2)
                AS g_co2_per_dollar
        FROM agg
        ORDER BY energy_kwh DESC, co2_usage_kg DESC, region DESC
    """


def tag_breakdown_query(where: str, tag_key: str) -> str:
    key = sql_literal(tag_key)
    return f"""
        WITH filtered AS (
            SELECT * FROM cur {where}
        )
        SELECT
            resource_tags[{key}] AS tag_value,
            round(sum(operational_energy_kwh), 2) AS energy_kwh,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS operational_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2) AS embodied_kg,
            round(
                coalesce(sum(water_cooling_l), 0)
                    + coalesce(sum(water_electricity_production_l), 0),
                2
            ) AS water_usage_l
        FROM filtered
        GROUP BY 1
        ORDER BY operational_kg DESC
    """


def metric_value(value: object, suffix: str = "", default: str = "n/a") -> str:
    if value is None or pd.isna(value):
        return default
    return f"{float(value):,.2f}{suffix}"


def chart_label(*parts: object, max_length: int = 70) -> str:
    label = " / ".join(str(part) for part in parts if part is not None and part != "")
    if not label:
        return "Unknown"
    if len(label) <= max_length:
        return label
    return f"{label[: max_length - 3]}..."


def render_overview(overview: pd.DataFrame) -> None:
    row = overview.iloc[0]
    first_row = st.columns(3)
    first_row[0].metric("Total usage cost", metric_value(row["total_cost"], " USD"))
    first_row[1].metric("Energy", metric_value(row["energy_kwh"], " kWh"))
    first_row[2].metric(
        "Operational emissions", metric_value(row["operational_kg"], " kg")
    )

    second_row = st.columns(3)
    second_row[0].metric("Embodied emissions", metric_value(row["embodied_kg"], " kg"))
    second_row[1].metric("Water", metric_value(row["water_l"], " L"))
    second_row[2].metric(
        "Coverage (dataset-wide)", metric_value(row["coverage_pct"], " %")
    )


def render_top_emitters(emitters: pd.DataFrame) -> None:
    if emitters.empty:
        st.info("No emitters found for the current filters.")
        return

    chart_data = emitters.copy()
    chart_data["emitter"] = chart_data.apply(
        lambda row: chart_label(
            row["line_item_product_code"],
            row["product_servicecode"],
            row["line_item_operation"],
        ),
        axis=1,
    )
    st.bar_chart(
        chart_data.set_index("emitter")[["co2_usage_kg"]],
        color=BRAND_COLOR,
        width="stretch",
    )
    st.dataframe(emitters, width="stretch", hide_index=True)


def render_regions(regions: pd.DataFrame) -> None:
    if regions.empty:
        st.info("No regional data found for the current filters.")
        return

    st.bar_chart(
        regions.set_index("region")[["co2_usage_kg", "energy_kwh"]],
        color=BRAND_PALETTE[:2],
        width="stretch",
    )
    st.dataframe(regions, width="stretch", hide_index=True)


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

    chart_data = tags.copy()
    chart_data["tag_value_label"] = (
        chart_data["tag_value"].fillna("(missing)").replace("", "(empty)")
    )
    st.bar_chart(
        chart_data.set_index("tag_value_label")[["operational_kg", "embodied_kg"]],
        color=BRAND_PALETTE[:2],
        width="stretch",
    )
    st.dataframe(tags, width="stretch", hide_index=True)


def configure_branding() -> None:
    """Set page icon, theme-aware page config, and sidebar logo when supported."""
    page_icon = str(LOGO_PATH) if LOGO_PATH.exists() else None
    st.set_page_config(
        page_title="SPRUCE Dashboard", page_icon=page_icon, layout="wide"
    )
    # st.logo was added in Streamlit 1.34; render_header() shows the inline logo
    # for older versions, so no fallback warning is needed.
    if LOGO_PATH.exists() and hasattr(st, "logo"):
        st.logo(str(LOGO_PATH))


def render_header() -> None:
    if LOGO_PATH.exists():
        col_logo, col_title = st.columns([1, 6], vertical_alignment="center")
        with col_logo:
            st.image(str(LOGO_PATH), width=96)
        with col_title:
            st.title("SPRUCE dashboard")
    else:
        st.title("SPRUCE dashboard")


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
        selected_periods = st.multiselect("Billing period", periods, default=periods)
        selected_regions = st.multiselect("Region", regions, default=regions)
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


def render_trend(trend: pd.DataFrame) -> None:
    if trend.empty:
        st.info("No billing period data found for the current filters.")
        return
    st.bar_chart(
        trend.set_index("BILLING_PERIOD")[
            ["energy_kwh", "operational_kg", "embodied_kg", "water_usage_l"]
        ],
        color=list(BRAND_PALETTE),
        width="stretch",
    )
    st.dataframe(trend, width="stretch", hide_index=True)


def main() -> None:
    args = parse_args()
    configure_branding()
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
