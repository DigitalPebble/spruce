#!/usr/bin/env python3
"""
Cloud Environmental Impact Report
Reads enriched Parquet output, runs analytical queries via DuckDB,
and produces a Markdown report with recommendations.

Queries use the FOCUS (FinOps Open Cost & Usage Specification) columns emitted
by all SPRUCE pipelines (AWS CUR, AWS FOCUS, Azure, Azure FOCUS); columns a
given input does not carry are added as NULLs so every query runs everywhere.
"""

import argparse
import os
import sys
from datetime import date

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required — run: pip install -r requirements-report.txt")

import equivalences


# Columns the queries below rely on. Any of them missing from the input is
# added as a typed NULL so the same queries run on the output of every
# pipeline (SkuMeter is not populated by Azure exports; ListCost is absent
# from Azure non-FOCUS exports). Tags is handled separately as its type
# differs per provider (MAP on AWS, JSON string on Azure).
REPORT_COLUMN_DEFAULTS = {
    "BILLING_PERIOD": "VARCHAR",
    "region": "VARCHAR",
    "ServiceName": "VARCHAR",
    "ChargeCategory": "VARCHAR",
    "BilledCost": "DOUBLE",
    "ListCost": "DOUBLE",
    "SkuMeter": "VARCHAR",
    "operational_energy_kwh": "DOUBLE",
    "operational_emissions_co2eq_g": "DOUBLE",
    "embodied_emissions_co2eq_g": "DOUBLE",
    "water_cooling_l": "DOUBLE",
    "water_electricity_production_l": "DOUBLE",
    "water_consumption_stress_area_l": "DOUBLE",
    "carbon_intensity": "DOUBLE",
    "power_usage_effectiveness": "DOUBLE",
}


# When the input is not hive-partitioned by BILLING_PERIOD, the YYYY-MM split
# is inferred from the FOCUS ChargePeriodStart column, which pipelines emit as
# a timestamp, an ISO 8601 string, or a US-style date string.
BILLING_PERIOD_FROM_CHARGE_PERIOD = r"""
    CASE WHEN regexp_matches(CAST(ChargePeriodStart AS VARCHAR), '^\d{4}-\d{2}')
         THEN substr(CAST(ChargePeriodStart AS VARCHAR), 1, 7)
         ELSE strftime(coalesce(
                  try_strptime(CAST(ChargePeriodStart AS VARCHAR), '%m/%d/%Y'),
                  try_strptime(CAST(ChargePeriodStart AS VARCHAR), '%-m/%-d/%Y')),
              '%Y-%m')
    END
""".strip()


def create_cur_table(con, parquet_glob):
    source = f"read_parquet('{parquet_glob}', hive_partitioning=true)"
    schema_rows = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    existing = [str(row[0]) for row in schema_rows]
    existing_lower = {c.lower() for c in existing}
    # Inputs may carry a known column under a different case (e.g. a
    # billing_period parquet column instead of the BILLING_PERIOD hive
    # partition); alias it to the canonical name the queries use.
    canonical = {name.lower(): name for name in REPORT_COLUMN_DEFAULTS}
    exprs = []
    for c in existing:
        quoted = '"' + c.replace('"', '""') + '"'
        wanted = canonical.get(c.lower())
        if wanted is not None and wanted != c:
            exprs.append(f'{quoted} AS "{wanted}"')
        else:
            exprs.append(quoted)
    for name, sql_type in REPORT_COLUMN_DEFAULTS.items():
        if name.lower() in existing_lower:
            continue
        if name == "BILLING_PERIOD" and "chargeperiodstart" in existing_lower:
            exprs.append(f'{BILLING_PERIOD_FROM_CHARGE_PERIOD} AS "{name}"')
        else:
            exprs.append(f'CAST(NULL AS {sql_type}) AS "{name}"')
    con.execute(f"CREATE TABLE cur AS SELECT {', '.join(exprs)} FROM {source}")


# ---------------------------------------------------------------------------
# Markdown helpers
# ---------------------------------------------------------------------------

def md_table(rows, headers):
    """Render a list-of-tuples as a GFM table."""
    if not rows:
        return "_No data._\n"
    col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                  for i, h in enumerate(headers)]
    sep = "| " + " | ".join("-" * w for w in col_widths) + " |"
    header_line = "| " + " | ".join(str(h).ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    lines = [header_line, sep]
    for row in rows:
        lines.append("| " + " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(headers))) + " |")
    return "\n".join(lines) + "\n"


def section(title, body):
    return f"## {title}\n\n{body}\n"


# ---------------------------------------------------------------------------
# Fixed analytical queries (from test.sql)
# ---------------------------------------------------------------------------

def q_row_count(con):
    return con.execute("SELECT count(*) FROM cur").fetchone()[0]


def q_billing_summary(con):
    sql = """
        SELECT
            BILLING_PERIOD,
            round(sum(operational_energy_kwh), 2)              AS energy_kwh,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS operational_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)    AS embodied_kg,
            round(coalesce(sum(water_cooling_l), 0) + coalesce(sum(water_electricity_production_l), 0), 2) AS water_usage_l
        FROM cur
        GROUP BY BILLING_PERIOD
        ORDER BY BILLING_PERIOD
    """
    return con.execute(sql).fetchall()


def q_top_emitters(con):
    sql = """
        SELECT
            ServiceName,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS co2_usage_kg,
            round(sum(operational_energy_kwh), 2)                AS energy_kwh,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)    AS co2_embodied_kg,
            round(coalesce(sum(water_cooling_l), 0) + coalesce(sum(water_electricity_production_l), 0), 2) AS water_usage_l
        FROM cur
        WHERE operational_emissions_co2eq_g is not null
        GROUP BY 1
        ORDER BY co2_usage_kg DESC, co2_embodied_kg DESC, energy_kwh DESC, ServiceName
        LIMIT 20
    """
    return con.execute(sql).fetchall()


def q_top_instance_types(con):
    # The instance type is derived from SkuMeter (FOCUS 1.2). On AWS it
    # carries the usage type (e.g. EUW2-BoxUsage:t3.xlarge) and the instance
    # type is the dot-separated lowercase token after the colon. Providers
    # that do not populate SkuMeter yield no rows for this section.
    sql = r"""
        WITH derived AS (
            SELECT
                regexp_extract(SkuMeter, ':((?:[a-z][a-z0-9-]*\.)+[a-z0-9]+)$', 1)
                    AS instance_type,
                operational_emissions_co2eq_g,
                embodied_emissions_co2eq_g,
                water_cooling_l,
                water_electricity_production_l
            FROM cur
        )
        SELECT
            instance_type,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS co2_usage_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)    AS co2_embodied_kg,
            round(coalesce(sum(water_cooling_l), 0) + coalesce(sum(water_electricity_production_l), 0), 2) AS water_usage_l
        FROM derived
        WHERE len(instance_type) > 0
        GROUP BY instance_type
        ORDER BY co2_usage_kg DESC
        LIMIT 20
    """
    return con.execute(sql).fetchall()


def q_coverage(con):
    """Returns (coverage_pct, covered_cost, total_cost)."""
    sql = """
        SELECT
            round(covered * 100.0 / NULLIF("total_cost", 0), 2) AS pct,
            round(covered, 2)    AS covered_cost,
            round("total_cost", 2) AS total_cost
        FROM (
            SELECT
                sum(BilledCost) AS "total_cost",
                sum(BilledCost)
                    FILTER (WHERE operational_emissions_co2eq_g IS NOT NULL) AS covered
            FROM cur
            WHERE ChargeCategory LIKE '%Usage'
        )
    """
    row = con.execute(sql).fetchone()
    return row if row else (None, None, None)


def q_uncovered_services(con):
    sql = """
        SELECT
            ServiceName,
            round(sum(BilledCost), 2) AS cost
        FROM cur
        WHERE operational_emissions_co2eq_g IS NULL
          AND ChargeCategory LIKE '%Usage'
        GROUP BY 1
        ORDER BY cost DESC
        LIMIT 20
    """
    return con.execute(sql).fetchall()


def q_regional(con):
    sql = """
        WITH agg AS (
            SELECT
                region,
                sum(operational_emissions_co2eq_g)  AS operational_g,
                sum(embodied_emissions_co2eq_g)     AS embodied_g,
                sum(operational_energy_kwh)         AS energy_kwh,
                sum(ListCost)                       AS public_cost,
                avg(carbon_intensity)               AS avg_ci,
                avg(power_usage_effectiveness)      AS pue,
                coalesce(sum(water_cooling_l), 0) + coalesce(sum(water_electricity_production_l), 0) AS water_l,
                coalesce(sum(water_consumption_stress_area_l), 0) AS water_stress_l
            FROM cur
            WHERE operational_emissions_co2eq_g is not null
            GROUP BY 1
        )
        SELECT
            region,
            round(operational_g / 1000, 2)                      AS co2_usage_kg,
            round(energy_kwh, 2)                                 AS energy_kwh,
            round(avg_ci, 2)                                     AS carbon_intensity,
            round(water_l, 2)                                    AS water_usage_l,
            round(water_stress_l, 2)                             AS water_stress_area_l,
            round(pue, 2)                                        AS pue,
            round((operational_g + embodied_g) / NULLIF(public_cost, 0), 2) AS g_co2_per_dollar
        FROM agg
        ORDER BY energy_kwh DESC, co2_usage_kg DESC, region DESC
    """
    return con.execute(sql).fetchall()

# ---------------------------------------------------------------------------
# Tag discovery
# ---------------------------------------------------------------------------

def tag_value_expr(con):
    """SQL expression extracting a tag value from the Tags column, with a {key}
    placeholder for the SQL-escaped tag key. AWS outputs carry Tags as a MAP,
    Azure ones as a JSON string. Returns None when Tags is absent."""
    row = con.execute(
        "SELECT column_type FROM (DESCRIBE cur) WHERE lower(column_name) = 'tags'"
    ).fetchone()
    if row is None:
        return None
    if row[0].upper().startswith("MAP"):
        return "Tags['{key}']"
    return "json_extract_string(TRY_CAST(Tags AS JSON), '$.\"{key}\"')"


def escape_tag_key(key):
    return key.replace("'", "''")


def find_tags_by_coverage(con, value_expr, top_n):
    """Return list of (key, coverage_pct) ordered by coverage desc, limited to top_n."""
    if value_expr is None:
        return []
    keys_expr = ("map_keys(Tags)" if value_expr.startswith("Tags[")
                 else "json_keys(TRY_CAST(Tags AS JSON))")
    try:
        keys_rows = con.execute(f"""
            SELECT DISTINCT unnest({keys_expr}) AS tag_key
            FROM cur
            WHERE Tags IS NOT NULL
        """).fetchall()
    except Exception:
        return []

    keys = [r[0] for r in keys_rows if r[0]]
    if not keys:
        return []

    total = con.execute("SELECT COUNT(*) FROM cur").fetchone()[0]
    if total == 0:
        return []

    scored = []
    for key in keys:
        value = value_expr.format(key=escape_tag_key(key))
        try:
            count = con.execute(f"""
                SELECT COUNT(*) FROM cur
                WHERE {value} IS NOT NULL
                  AND {value} != ''
            """).fetchone()[0]
            pct = round(count * 100.0 / total, 1)
            if pct > 0:
                scored.append((key, pct))
        except Exception:
            continue

    scored.sort(key=lambda x: -x[1])
    return scored[:top_n]


def q_tag_breakdown(con, value_expr, key):
    sql = f"""
        SELECT
            {value_expr.format(key=escape_tag_key(key))}                    AS tag_value,
            round(sum(operational_energy_kwh), 2)                           AS energy_kwh,
            round(sum(operational_emissions_co2eq_g) / 1000, 2)            AS operational_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)               AS embodied_kg,
            round(coalesce(sum(water_cooling_l), 0) + coalesce(sum(water_electricity_production_l), 0), 2) AS water_usage_l
        FROM cur
        GROUP BY 1
        ORDER BY operational_kg DESC
    """
    return con.execute(sql).fetchall()


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

GRAVITON_MAP = {
    "m5": "m6g", "m5a": "m6g", "m5d": "m6gd",
    "c5": "c6g", "c5a": "c6g", "c5d": "c6gd",
    "r5": "r6g", "r5a": "r6g", "r5d": "r6gd",
    "x1": "x2g",
    "i3": "i4g",
}


def build_recommendations(coverage_pct, uncovered_rows, regional_rows, billing_rows, instance_rows):
    recs = []

    # Coverage
    if coverage_pct is not None and coverage_pct < 80:
        top_uncovered = [r for r in uncovered_rows if r[1] and float(r[1]) > 0][:3]
        for r in top_uncovered:
            svc = r[0] or "unknown"
            cost = r[1]
            recs.append(f"⚠ Coverage gap: **{svc}** (${cost:,} uncovered) — SPRUCE does not yet model this service")

    # Region carbon intensity
    if regional_rows:
        ci_list = [(r[0], float(r[3])) for r in regional_rows if r[3] is not None]
        high_ci = [(reg, ci) for reg, ci in ci_list if ci > 300]
        if high_ci and len(ci_list) >= 2:
            low_reg, low_ci = min(ci_list, key=lambda x: x[1])
            for reg, ci in sorted(high_ci, key=lambda x: -x[1])[:2]:
                factor = round(ci / max(low_ci, 1), 1)
                recs.append(
                    f"🌍 **{reg}** ({ci:.0f} gCO2/kWh) has {factor}× higher carbon intensity "
                    f"than **{low_reg}** ({low_ci:.0f} gCO2/kWh) — consider migrating workloads"
                )

    # Instance type Graviton suggestions
    if instance_rows:
        top_inst = instance_rows[0][0] if instance_rows else None
        if top_inst:
            family = top_inst.split(".")[0] if "." in top_inst else top_inst
            replacement = GRAVITON_MAP.get(family)
            if replacement:
                new_inst = top_inst.replace(family, replacement, 1)
                recs.append(
                    f"💻 **{top_inst}** is your top compute emitter — consider **{new_inst}** "
                    f"(Graviton, ~40 % lower embodied emissions)"
                )

    # Billing period trend
    valid_periods = [(r[0], float(r[2])) for r in billing_rows if r[2] is not None]
    if len(valid_periods) >= 2:
        first_period, first_kg = valid_periods[0]
        last_period, last_kg = valid_periods[-1]
        if first_kg > 0 and last_kg > first_kg * 1.10:
            pct_change = round((last_kg / first_kg - 1) * 100)
            recs.append(
                f"📈 Emissions rose **{pct_change} %** from {first_period} to {last_period} "
                f"— investigate billing period trend"
            )

    return recs


# ---------------------------------------------------------------------------
# HTML post-processing
# ---------------------------------------------------------------------------

_LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logo.png")


def _inject_logo(html):
    """Embed logo.png as a base64 data URI after the 'Enriched with SPRUCE' line."""
    import base64, re
    if not os.path.exists(_LOGO_PATH):
        return html
    with open(_LOGO_PATH, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    img_tag = (
        f'<p><img src="data:image/png;base64,{b64}" '
        f'style="height:180px;margin-top:6px;" alt="SPRUCE logo"></p>'
    )
    return re.sub(
        r'(<p>Enriched with.*?</p>)',
        r'\1' + img_tag,
        html,
        count=1,
        flags=re.DOTALL,
    )


_LANDSCAPE_SECTIONS = {"Top Emitters by Service", "Regional Analysis"}


def _wrap_landscape_sections(html):
    """Wrap wide table sections in a div that WeasyPrint renders in landscape."""
    import re
    parts = re.split(r'(<h2>[^<]*</h2>)', html)
    result = []
    in_landscape = False
    for part in parts:
        m = re.match(r'<h2>([^<]*)</h2>', part)
        if m:
            if in_landscape:
                result.append('</div>')
                in_landscape = False
            if m.group(1) in _LANDSCAPE_SECTIONS:
                result.append('<div class="landscape-section">')
                in_landscape = True
        result.append(part)
    if in_landscape:
        result.append('</div>')
    return ''.join(result)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate a report from CUR files enriched with SPRUCE"
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Path to enriched Parquet directory, glob, or S3 URI (e.g. output/, output/**/*.parquet, s3://bucket/prefix/).")
    parser.add_argument("-o", "--output", default=None,
                        help="Output file. Format is inferred from suffix: .md (Markdown), .html (HTML), .pdf (PDF via weasyprint). Defaults to Markdown on stdout.")
    parser.add_argument("-t", "--top-tags", type=int, default=10, metavar="N",
                        help="Maximum number of resource tags to offer for breakdown (default: 10). Use 0 or negative to skip tag selection entirely.")
    args = parser.parse_args()

    # Normalise input path to a glob DuckDB can use
    inp = args.input.rstrip("/")
    if inp.endswith(".parquet"):
        parquet_glob = inp
    else:
        parquet_glob = f"{inp}/**/*.parquet"

    # -----------------------------------------------------------------------
    # Load data
    # -----------------------------------------------------------------------
    con = duckdb.connect(":memory:")
    if args.input.startswith("s3"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")
    try:
        create_cur_table(con, parquet_glob)
    except Exception as exc:
        sys.exit(f"Failed to load Parquet data from '{parquet_glob}': {exc}")

    # -----------------------------------------------------------------------
    # Run queries
    # -----------------------------------------------------------------------
    row_count = q_row_count(con)
    billing_rows = q_billing_summary(con)
    emitter_rows = q_top_emitters(con)
    instance_rows = q_top_instance_types(con)
    coverage_pct, covered_cost, total_cost = q_coverage(con)
    uncovered_rows = q_uncovered_services(con)
    regional_rows = q_regional(con)

    # -----------------------------------------------------------------------
    # Interactive tag breakdown
    # -----------------------------------------------------------------------
    available_tags = []
    tag_sections = []  # list of (key, coverage_pct, breakdown_rows)

    if args.top_tags >= 1:
        value_expr = tag_value_expr(con)
        available_tags = find_tags_by_coverage(con, value_expr, args.top_tags)

        if not available_tags:
            sys.stderr.write("\nNo consistent resource tags found in the data.\n")
        else:
            remaining = list(available_tags)
            sys.stderr.write("\nResource tags found (by line-item coverage):\n")
            while remaining:
                for i, (key, pct) in enumerate(remaining, 1):
                    sys.stderr.write(f"  {i}. {key}  ({pct} %)\n")
                sys.stderr.write("Enter number to see breakdown, or press Enter to finish: ")
                sys.stderr.flush()
                choice = sys.stdin.readline().strip()
                if not choice:
                    break
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(remaining):
                        key, pct = remaining.pop(idx)
                        breakdown = q_tag_breakdown(con, value_expr, key)
                        tag_sections.append((key, pct, breakdown))
                        sys.stderr.write(f"\n### Tag: {key}  ({pct} % coverage)\n\n")
                        sys.stderr.write(md_table(breakdown, ["tag_value", "energy_kwh", "operational_kg", "embodied_kg", "water_usage_l"]))
                        sys.stderr.write("\n")
                    else:
                        sys.stderr.write(f"Please enter a number between 1 and {len(remaining)}.\n")
                except ValueError:
                    sys.stderr.write("Please enter a number or press Enter to finish.\n")

    # -----------------------------------------------------------------------
    # Assemble report
    # -----------------------------------------------------------------------
    today = date.today().isoformat()
    parts = []

    parts.append(
        f"# Cloud Environmental Impact Report\n"
        f"_{today} | Rows: {row_count:,}_\n\n"
        f"Enriched with [SPRUCE](http://opensourcegreenops.cloud/)\n"
    )

    # Billing summary
    billing_with_total = list(billing_rows)
    if billing_rows:
        billing_with_total.append(tuple(
            f"**{v}**" for v in (
                "TOTAL",
                round(sum(r[1] for r in billing_rows if r[1] is not None), 2),
                round(sum(r[2] for r in billing_rows if r[2] is not None), 2),
                round(sum(r[3] for r in billing_rows if r[3] is not None), 2),
                round(sum(r[4] for r in billing_rows if r[4] is not None), 2),
            )
        ))
    parts.append(section(
        "Summary by Billing Period",
        md_table(billing_with_total, ["BILLING_PERIOD", "energy_kwh", "operational_kg", "embodied_kg", "water_usage_l"])
    ))

    # Everyday equivalences (same conversions as the dashboard overview)
    if billing_rows:
        total_energy = sum(r[1] for r in billing_rows if r[1] is not None)
        total_emissions = (
            sum(r[2] for r in billing_rows if r[2] is not None)
            + sum(r[3] for r in billing_rows if r[3] is not None)
        )
        total_water = sum(r[4] for r in billing_rows if r[4] is not None)
        equiv_lines = []
        for group in equivalences.overview_equivalences(
            total_emissions, total_energy, total_water
        ):
            comparisons = " / ".join(
                f"{equivalences.format_quantity(item.quantity)} {item.unit}"
                + (f" ({item.note})" if item.note else "")
                for item in group.items
            )
            equiv_lines.append(f"- **{group.metric}** ≈ {comparisons}")
        source_links = ", ".join(
            f"[{name}]({url})" for name, url in equivalences.SOURCES
        )
        equiv_body = (
            "\n".join(equiv_lines)
            + "\n\n_Order-of-magnitude conversion factors — sources: "
            + source_links
            + "._\n"
        )
        parts.append(section("In Everyday Terms", equiv_body))

    # Top emitters
    parts.append(section(
        "Top Emitters by Service",
        md_table(emitter_rows, [
            "service", "co2_usage_kg", "energy_kwh", "co2_embodied_kg", "water_usage_l"
        ])
    ))

    # Instance types
    parts.append(section(
        "Top Instance Types",
        md_table(instance_rows, ["instance_type", "co2_usage_kg", "co2_embodied_kg", "water_usage_l"])
    ))

    # Coverage
    if coverage_pct is not None:
        coverage_body = f"- **{coverage_pct} %** of unblended costs have emissions data\n\n"
    else:
        coverage_body = "- _Coverage data unavailable_\n\n"
    coverage_body += md_table(uncovered_rows, ["service", "cost_usd"])
    parts.append(section("Coverage", coverage_body))

    # Regional
    parts.append(section(
        "Regional Analysis",
        md_table(regional_rows, [
            "region", "co2_usage_kg", "energy_kwh",
            "carbon_intensity", "water_usage_l", "water_stress_area_l", "pue", "g_co2_per_dollar"
        ])
    ))

    # Tag breakdowns
    if not available_tags:
        parts.append(section("Tag Breakdown", "_No consistent resource tags found in the data._\n"))
    elif tag_sections:
        for key, cov_pct, breakdown in tag_sections:
            title = f"Tag Breakdown: {key}  _(coverage {cov_pct} %)_"
            parts.append(section(
                title,
                md_table(breakdown, ["tag_value", "energy_kwh", "operational_kg", "embodied_kg", "water_usage_l"])
            ))

    # Recommendations
    recs = build_recommendations(coverage_pct, uncovered_rows, regional_rows, billing_rows, instance_rows)
    if recs:
        rec_body = "\n".join(f"- {r}" for r in recs) + "\n"
    else:
        rec_body = "_No actionable recommendations at this time._\n"
    parts.append(section("Recommendations", rec_body))

    report = "\n".join(parts)

    # -----------------------------------------------------------------------
    # Output
    # -----------------------------------------------------------------------
    suffix = args.output.lower().rsplit(".", 1)[-1] if args.output else ""

    if suffix in ("html", "pdf"):
        try:
            import markdown as md_lib
        except ImportError:
            sys.exit("HTML/PDF output requires: pip install markdown weasyprint")
        html_body = md_lib.markdown(report, extensions=["tables", "nl2br"])
        html_body = _inject_logo(html_body)
        html_body = _wrap_landscape_sections(html_body)
        styled = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  @page {{ size: A4 portrait; margin: 2cm; }}
  @page landscape-page {{ size: A4 landscape; margin: 1.5cm; }}
  body {{ font-family: sans-serif; font-size: 11px; color: #111; }}
  h1 {{ font-size: 20px; }} h2 {{ font-size: 15px; border-bottom: 1px solid #ccc; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 1em; font-size: 10px; }}
  th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: left; }}
  th {{ background: #f0f0f0; }}
  em {{ color: #555; }}
  .landscape-section {{ page: landscape-page; }}
</style></head><body>{html_body}</body></html>"""
        if suffix == "pdf":
            try:
                import weasyprint
            except ImportError:
                sys.exit("PDF output requires: pip install weasyprint")
            weasyprint.HTML(string=styled).write_pdf(args.output)
        else:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(styled)
        print(f"Report written to {args.output}", file=sys.stderr)
    elif args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
