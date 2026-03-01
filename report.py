#!/usr/bin/env python3
"""
AWS Environmental Impact Report
Reads enriched Parquet output, runs analytical queries via DuckDB,
and produces a Markdown report with recommendations.
"""

import argparse
import sys
from datetime import date

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required — run: pip install -r requirements-report.txt")


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
            round(sum(embodied_adp_sbeq_g), 2)                  AS adp_g
        FROM cur
        GROUP BY BILLING_PERIOD
        ORDER BY BILLING_PERIOD
    """
    return con.execute(sql).fetchall()


def q_top_emitters(con):
    sql = """
        SELECT
            line_item_product_code,
            product_servicecode,
            line_item_operation,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS co2_usage_kg,
            round(sum(operational_energy_kwh), 2)                AS energy_kwh,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)    AS co2_embodied_kg
        FROM cur
        WHERE operational_emissions_co2eq_g > 1
        GROUP BY 1, 2, 3
        ORDER BY co2_usage_kg DESC, co2_embodied_kg DESC, energy_kwh DESC, line_item_operation
        LIMIT 20
    """
    return con.execute(sql).fetchall()


def q_top_instance_types(con):
    sql = """
        SELECT
            product_instance_type,
            round(sum(operational_emissions_co2eq_g) / 1000, 2) AS co2_usage_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)    AS co2_embodied_kg
        FROM cur
        WHERE len(product_instance_type) > 0
        GROUP BY product_instance_type
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
                sum(line_item_unblended_cost) AS "total_cost",
                sum(line_item_unblended_cost)
                    FILTER (WHERE operational_emissions_co2eq_g IS NOT NULL) AS covered
            FROM cur
            WHERE line_item_line_item_type LIKE '%Usage'
        )
    """
    row = con.execute(sql).fetchone()
    return row if row else (None, None, None)


def q_uncovered_services(con):
    sql = """
        SELECT
            line_item_product_code,
            product_servicecode,
            line_item_operation,
            round(sum(line_item_unblended_cost), 2) AS cost
        FROM cur
        WHERE operational_emissions_co2eq_g IS NULL
          AND line_item_line_item_type LIKE '%Usage'
        GROUP BY 1, 2, 3
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
                sum(pricing_public_on_demand_cost)  AS public_cost,
                avg(carbon_intensity)               AS avg_ci,
                avg(power_usage_effectiveness)      AS pue
            FROM cur
            WHERE operational_emissions_co2eq_g > 1
            GROUP BY 1
        )
        SELECT
            region,
            round(operational_g / 1000, 2)                      AS co2_usage_kg,
            round(energy_kwh, 2)                                 AS energy_kwh,
            round(avg_ci, 2)                                     AS carbon_intensity,
            round(pue, 2)                                        AS pue,
            round((operational_g + embodied_g) / NULLIF(public_cost, 0), 2) AS g_co2_per_dollar
        FROM agg
        ORDER BY energy_kwh DESC, co2_usage_kg DESC, region DESC
    """
    return con.execute(sql).fetchall()


# ---------------------------------------------------------------------------
# Tag discovery
# ---------------------------------------------------------------------------

def find_tags_by_coverage(con, top_n):
    """Return list of (key, coverage_pct) ordered by coverage desc, limited to top_n."""
    try:
        keys_rows = con.execute("""
            SELECT DISTINCT unnest(map_keys(resource_tags)) AS tag_key
            FROM cur
            WHERE resource_tags IS NOT NULL
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
        try:
            count = con.execute(f"""
                SELECT COUNT(*) FROM cur
                WHERE resource_tags['{key}'] IS NOT NULL
                  AND resource_tags['{key}'] != ''
            """).fetchone()[0]
            pct = round(count * 100.0 / total, 1)
            if pct > 0:
                scored.append((key, pct))
        except Exception:
            continue

    scored.sort(key=lambda x: -x[1])
    return scored[:top_n]


def q_tag_breakdown(con, key):
    sql = f"""
        SELECT
            resource_tags['{key}']                                          AS tag_value,
            round(sum(operational_energy_kwh), 2)                           AS energy_kwh,
            round(sum(operational_emissions_co2eq_g) / 1000, 2)            AS operational_kg,
            round(sum(embodied_emissions_co2eq_g) / 1000, 2)               AS embodied_kg
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
        top_uncovered = [r for r in uncovered_rows if r[3] and float(r[3]) > 0][:3]
        for r in top_uncovered:
            svc = r[1] or r[0] or "unknown"
            cost = r[3]
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
    parser.add_argument("--top-tags", type=int, default=10, metavar="N",
                        help="Maximum number of resource tags to offer for breakdown (default: 10)")
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
        con.execute(f"""
            CREATE TABLE cur AS
            SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        """)
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
    available_tags = find_tags_by_coverage(con, args.top_tags)
    tag_sections = []  # list of (key, coverage_pct, breakdown_rows)

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
                    breakdown = q_tag_breakdown(con, key)
                    tag_sections.append((key, pct, breakdown))
                    sys.stderr.write(f"\n### Tag: {key}  ({pct} % coverage)\n\n")
                    sys.stderr.write(md_table(breakdown, ["tag_value", "energy_kwh", "operational_kg", "embodied_kg"]))
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
        f"# AWS Environmental Impact Report\n"
        f"_{today} | Rows: {row_count:,}_\n\n"
        f"Enriched with [SPRUCE](http://opensourcegreenops.cloud/)\n"
    )

    # Billing summary
    parts.append(section(
        "Summary by Billing Period",
        md_table(billing_rows, ["BILLING_PERIOD", "energy_kwh", "operational_kg", "embodied_kg", "adp_g"])
    ))

    # Top emitters
    parts.append(section(
        "Top Emitters by Service",
        md_table(emitter_rows, [
            "product_code", "service_code", "operation",
            "co2_usage_kg", "energy_kwh", "co2_embodied_kg"
        ])
    ))

    # Instance types
    parts.append(section(
        "Top Instance Types",
        md_table(instance_rows, ["instance_type", "co2_usage_kg", "co2_embodied_kg"])
    ))

    # Coverage
    if coverage_pct is not None:
        coverage_body = f"- **{coverage_pct} %** of unblended costs have emissions data\n\n"
    else:
        coverage_body = "- _Coverage data unavailable_\n\n"
    coverage_body += md_table(uncovered_rows, ["product_code", "service_code", "operation", "cost_usd"])
    parts.append(section("Coverage", coverage_body))

    # Regional
    parts.append(section(
        "Regional Analysis",
        md_table(regional_rows, [
            "region", "co2_usage_kg", "energy_kwh",
            "carbon_intensity", "pue", "g_co2_per_dollar"
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
                md_table(breakdown, ["tag_value", "energy_kwh", "operational_kg", "embodied_kg"])
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
        styled = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  body {{ font-family: sans-serif; font-size: 11px; margin: 2cm; color: #111; }}
  h1 {{ font-size: 20px; }} h2 {{ font-size: 15px; border-bottom: 1px solid #ccc; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 1em; font-size: 10px; }}
  th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: left; }}
  th {{ background: #f0f0f0; }}
  em {{ color: #555; }}
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
