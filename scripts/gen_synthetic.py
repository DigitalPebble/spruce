#!/usr/bin/env python3
"""Generate synthetic SPRUCE-enriched Parquet data for dashboard review."""

import argparse
from pathlib import Path
import sys

try:
    import duckdb
except ImportError:
    sys.exit(
        "duckdb is required - run: pip install -r reporting/requirements-dashboard.txt"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic SPRUCE-enriched Parquet data"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="output/synth.parquet",
        help="Output Parquet file (default: output/synth.parquet).",
    )
    parser.add_argument(
        "-n",
        "--rows",
        type=int,
        default=500,
        help="Number of synthetic rows to generate (default: 500).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = max(1, args.rows)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT
                ('2025-' || lpad((((i % 5) + 5)::VARCHAR), 2, '0'))
                    AS BILLING_PERIOD,
                CASE
                    WHEN i % 13 = 0 THEN NULL
                    ELSE ['us-east-1', 'eu-west-2', 'eu-north-1'][1 + (i % 3)]
                END AS region,
                ['us-east-1', 'eu-west-2', 'eu-north-1'][1 + (i % 3)]
                    AS product_region_code,
                ['AmazonEC2', 'AmazonS3', 'AmazonECS'][1 + (i % 3)]
                    AS line_item_product_code,
                ['AmazonEC2', 'AmazonS3', 'AmazonECS'][1 + (i % 3)]
                    AS product_servicecode,
                ['RunInstances', 'StandardStorage', 'FargateTask'][1 + (i % 3)]
                    AS line_item_operation,
                ['m5.large', 'c5.xlarge', 'r5.2xlarge', 'm6g.large'][1 + (i % 4)]
                    AS product_instance_type,
                'Usage' AS line_item_line_item_type,
                round(3.5 + (i % 29) * 0.37, 2)::DOUBLE AS line_item_unblended_cost,
                round(2.0 + (i % 23) * 0.31, 2)::DOUBLE
                    AS pricing_public_on_demand_cost,
                CASE
                    WHEN i % 11 = 0 THEN NULL
                    ELSE round(8.0 + (i % 50) * 1.8, 2)
                END::DOUBLE AS operational_energy_kwh,
                CASE
                    WHEN i % 11 = 0 THEN NULL
                    ELSE round((8.0 + (i % 50) * 1.8) * (180 + (i % 4) * 80), 2)
                END::DOUBLE AS operational_emissions_co2eq_g,
                CASE
                    WHEN i % 11 = 0 THEN NULL
                    ELSE round(200 + (i % 31) * 23.5, 2)
                END::DOUBLE AS embodied_emissions_co2eq_g,
                round((i % 40) * 1.7, 2)::DOUBLE AS water_cooling_l,
                round((i % 17) * 0.9, 2)::DOUBLE AS water_electricity_production_l,
                CASE
                    WHEN i % 3 = 0 THEN round((i % 17) * 0.9, 2)
                    ELSE 0
                END::DOUBLE AS water_consumption_stress_area_l,
                [400.33, 175.03, 20.42][1 + (i % 3)]::DOUBLE AS carbon_intensity,
                [1.15, 1.11, 1.10][1 + (i % 3)]::DOUBLE
                    AS power_usage_effectiveness,
                MAP(
                    ['team', 'env'],
                    [
                        ['data', 'platform', 'ml'][1 + (i % 3)],
                        ['prod', 'dev'][1 + (i % 2)]
                    ]
                ) AS resource_tags
            FROM range({rows}) t(i)
        ) TO ? (FORMAT PARQUET)
        """,
        [str(output)],
    )
    print(f"Wrote {rows} synthetic rows to {output}")


if __name__ == "__main__":
    main()
