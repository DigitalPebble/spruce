#!/usr/bin/env python3
"""Generate synthetic SPRUCE-enriched Parquet data for dashboard review.

Models a multi-account, multi-region enterprise footprint across many AWS
services so the dashboard exercises realistic cardinality (top-N slider,
regional pie, tag breakdowns).
"""

import argparse
from pathlib import Path
import sys

try:
    import duckdb
except ImportError:
    sys.exit(
        "duckdb is required - run: pip install -r reporting/requirements-dashboard.txt"
    )


# (product_code, servicecode, [operations], carbon_weight, embodied_weight)
SERVICE_CATALOG = [
    ("AmazonEC2", "AmazonEC2", ["RunInstances", "RunInstances:0002", "StartInstances"], 1.0, 1.0),
    ("AmazonS3", "AmazonS3", ["StandardStorage", "GetObject", "PutObject"], 0.4, 0.2),
    ("AmazonECS", "AmazonECS", ["FargateTask", "ECSTask-Spot"], 0.8, 0.7),
    ("AmazonRDS", "AmazonRDS", ["CreateDBInstance", "RDSStorageGP3"], 1.2, 1.4),
    ("AWSLambda", "AWSLambda", ["Invoke", "Invoke-ARM"], 0.3, 0.1),
    ("AmazonCloudFront", "AmazonCloudFront", ["DataTransfer-Out", "Requests-HTTPS"], 0.5, 0.2),
    ("AmazonEBS", "AmazonEBS", ["VolumeUsage.gp3", "VolumeUsage.io2", "SnapshotUsage"], 0.3, 0.6),
    ("AmazonELB", "AWSELB", ["LoadBalancerUsage", "LCUUsage"], 0.6, 0.3),
    ("AmazonDynamoDB", "AmazonDynamoDB", ["WriteCapacityUnit-Hrs", "ReadCapacityUnit-Hrs", "TimedStorage-ByteHrs"], 0.7, 0.4),
    ("AmazonEKS", "AmazonEKS", ["ClusterUsage", "FargatePodUsage"], 1.1, 0.9),
    ("AmazonKinesis", "AmazonKinesis", ["ShardHour", "PutRecords"], 0.9, 0.5),
    ("AmazonRedshift", "AmazonRedshift", ["NodeUsage", "RMS"], 1.5, 1.6),
    ("AWSGlue", "AWSGlue", ["ETLJobRun", "CrawlerRun"], 0.8, 0.4),
    ("AmazonOpenSearch", "AmazonOpenSearch", ["ESInstance", "ESStorage"], 1.3, 1.2),
]

REGIONS = [
    ("us-east-1", 400.33),
    ("us-east-2", 458.10),
    ("us-west-2", 280.40),
    ("eu-west-1", 295.20),
    ("eu-west-2", 175.03),
    ("eu-north-1", 20.42),
    ("eu-central-1", 311.50),
    ("ap-southeast-1", 408.70),
    ("ap-northeast-1", 462.90),
    ("ca-central-1", 130.20),
]

INSTANCE_TYPES = [
    "m5.large", "m5.xlarge", "m5.2xlarge",
    "c5.large", "c5.xlarge", "c5.2xlarge",
    "r5.large", "r5.xlarge", "r5.2xlarge",
    "m6g.large", "m6g.xlarge", "c6g.xlarge",
    "t3.medium", "t3.large", "i3.2xlarge",
]

ACCOUNTS = [
    ("111111111111", "platform-prod"),
    ("222222222222", "data-prod"),
    ("333333333333", "ml-prod"),
    ("444444444444", "web-prod"),
    ("555555555555", "platform-dev"),
    ("666666666666", "sandbox"),
]

TEAMS = ["platform", "data", "ml", "web", "infra", "security"]
ENVS = ["prod", "staging", "dev", "qa"]
PROJECTS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"]
COST_CENTERS = ["CC-1001", "CC-1002", "CC-2001", "CC-2002", "CC-3001"]


def sql_str_array(values: list[str]) -> str:
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"


def sql_double_array(values: list[float]) -> str:
    return "[" + ", ".join(f"{v}::DOUBLE" for v in values) + "]"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic SPRUCE-enriched Parquet data"
    )
    parser.add_argument(
        "-o", "--output",
        default="output/synth.parquet",
        help="Output Parquet file (default: output/synth.parquet).",
    )
    parser.add_argument(
        "-n", "--rows",
        type=int,
        default=20000,
        help="Number of synthetic rows to generate (default: 20000).",
    )
    parser.add_argument(
        "-m", "--months",
        type=int,
        default=12,
        help="Distinct billing periods, ending 2025-12 (default: 12).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = max(1, args.rows)
    months = max(1, min(24, args.months))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    products = sql_str_array([s[0] for s in SERVICE_CATALOG])
    services = sql_str_array([s[1] for s in SERVICE_CATALOG])
    carbon_weights = sql_double_array([s[3] for s in SERVICE_CATALOG])
    embodied_weights = sql_double_array([s[4] for s in SERVICE_CATALOG])
    n_services = len(SERVICE_CATALOG)

    op_lookup_cases = []
    for idx, (_, _, ops, _, _) in enumerate(SERVICE_CATALOG, start=1):
        ops_arr = sql_str_array(ops)
        op_lookup_cases.append(
            f"WHEN service_idx = {idx} THEN {ops_arr}[1 + (i % {len(ops)})]"
        )
    op_case = "CASE " + " ".join(op_lookup_cases) + " END"

    regions_arr = sql_str_array([r[0] for r in REGIONS])
    ci_arr = sql_double_array([r[1] for r in REGIONS])
    n_regions = len(REGIONS)

    inst_arr = sql_str_array(INSTANCE_TYPES)
    n_inst = len(INSTANCE_TYPES)

    account_ids = sql_str_array([a[0] for a in ACCOUNTS])
    account_names = sql_str_array([a[1] for a in ACCOUNTS])
    n_accounts = len(ACCOUNTS)

    teams_arr = sql_str_array(TEAMS)
    envs_arr = sql_str_array(ENVS)
    projects_arr = sql_str_array(PROJECTS)
    cc_arr = sql_str_array(COST_CENTERS)

    pue_by_region = sql_double_array(
        [1.08 + (i % 5) * 0.04 for i in range(n_regions)]
    )

    sql = f"""
        COPY (
            WITH base AS (
                SELECT
                    i,
                    1 + (i % {n_services}) AS service_idx,
                    1 + ((i * 7) % {n_regions}) AS region_idx,
                    1 + ((i * 11) % {n_inst}) AS inst_idx,
                    1 + ((i * 13) % {n_accounts}) AS acct_idx,
                    -- sequential month buckets so each month sees full
                    -- rotation of services/regions/accounts -> trend smooth
                    1 + (i * {months} // {rows}) AS chrono_month_raw,
                    ({months} - (i * {months} // {rows})) AS month_idx,
                    (i * 23) % 100 AS jitter,
                    1 + (i * {months} // {rows}) AS chrono_month
                FROM range({rows}) t(i)
            ), shaped AS (
                SELECT
                    *,
                    -- 6% MoM growth + gentle seasonal sine (~+/-12%)
                    (1.0 + (chrono_month - 1) * 0.06)
                        * (1.0 + 0.12 * sin(chrono_month * 0.62))
                        AS month_factor,
                    -- damp jitter from +/-50% to +/-12% so monthly totals smooth
                    (0.88 + (jitter / 100.0) * 0.24) AS smooth_jitter
                FROM base
            )
            SELECT
                ('2025-' || lpad((((13 - month_idx))::VARCHAR), 2, '0'))
                    AS BILLING_PERIOD,
                CASE WHEN i % 19 = 0 THEN NULL
                     ELSE {regions_arr}[region_idx] END AS region,
                {regions_arr}[region_idx] AS product_region_code,
                {products}[service_idx] AS line_item_product_code,
                {services}[service_idx] AS product_servicecode,
                {op_case} AS line_item_operation,
                CASE WHEN service_idx IN (1, 4, 10) THEN {inst_arr}[inst_idx]
                     ELSE NULL END AS product_instance_type,
                CASE WHEN service_idx IN (1, 4, 10) THEN
                        CASE
                          WHEN {inst_arr}[inst_idx] LIKE 'm%' THEN 'm'
                          WHEN {inst_arr}[inst_idx] LIKE 'c%' THEN 'c'
                          WHEN {inst_arr}[inst_idx] LIKE 'r%' THEN 'r'
                          WHEN {inst_arr}[inst_idx] LIKE 't%' THEN 't'
                          WHEN {inst_arr}[inst_idx] LIKE 'i%' THEN 'i'
                        END
                     ELSE NULL END AS product_instance_family,
                {account_ids}[acct_idx] AS line_item_usage_account_id,
                {account_names}[acct_idx] AS line_item_usage_account_name,
                CASE WHEN i % 97 = 0 THEN 'Tax'
                     WHEN i % 89 = 0 THEN 'Credit'
                     ELSE 'Usage' END AS line_item_line_item_type,
                round(
                    (0.2 + (jitter * 0.05) + (service_idx * 1.7))
                        * month_factor * smooth_jitter,
                2)::DOUBLE AS line_item_unblended_cost,
                round(
                    (0.4 + (jitter * 0.06) + (service_idx * 1.85))
                        * month_factor * smooth_jitter,
                2)::DOUBLE AS pricing_public_on_demand_cost,
                CASE
                    WHEN i % 23 = 0 THEN NULL
                    WHEN i % 97 = 0 THEN NULL
                    WHEN i % 89 = 0 THEN NULL
                    ELSE round(
                        (1.5 + (jitter * 0.025))
                            * {carbon_weights}[service_idx]
                            * month_factor * smooth_jitter,
                    2)
                END::DOUBLE AS operational_energy_kwh,
                CASE
                    WHEN i % 23 = 0 THEN NULL
                    WHEN i % 97 = 0 THEN NULL
                    WHEN i % 89 = 0 THEN NULL
                    ELSE round(
                        (1.5 + (jitter * 0.025))
                            * {carbon_weights}[service_idx]
                            * {ci_arr}[region_idx]
                            * month_factor * smooth_jitter,
                    2)
                END::DOUBLE AS operational_emissions_co2eq_g,
                CASE
                    WHEN i % 23 = 0 THEN NULL
                    WHEN i % 97 = 0 THEN NULL
                    WHEN i % 89 = 0 THEN NULL
                    ELSE round(
                        (40 + jitter * 0.4)
                            * {embodied_weights}[service_idx]
                            * month_factor * smooth_jitter,
                    2)
                END::DOUBLE AS embodied_emissions_co2eq_g,
                round(
                    (jitter * 0.025)
                        * {carbon_weights}[service_idx]
                        * month_factor * smooth_jitter,
                2)::DOUBLE AS water_cooling_l,
                round(
                    (jitter * 0.012)
                        * {carbon_weights}[service_idx]
                        * month_factor * smooth_jitter,
                2)::DOUBLE AS water_electricity_production_l,
                CASE WHEN region_idx IN (1, 2, 8, 9) THEN
                        round(
                            (jitter * 0.02)
                                * {carbon_weights}[service_idx]
                                * month_factor * smooth_jitter,
                        2)
                     ELSE 0 END::DOUBLE AS water_consumption_stress_area_l,
                {ci_arr}[region_idx]::DOUBLE AS carbon_intensity,
                {pue_by_region}[region_idx]::DOUBLE AS power_usage_effectiveness,
                MAP(
                    ['team', 'env', 'project', 'cost_center'],
                    [
                        {teams_arr}[1 + ((i * 5) % {len(TEAMS)})],
                        {envs_arr}[1 + ((i * 3) % {len(ENVS)})],
                        {projects_arr}[1 + ((i * 19) % {len(PROJECTS)})],
                        {cc_arr}[1 + ((i * 29) % {len(COST_CENTERS)})]
                    ]
                ) AS resource_tags
            FROM shaped
        ) TO ? (FORMAT PARQUET)
    """

    con = duckdb.connect()
    con.execute(sql, [str(output)])
    print(
        f"Wrote {rows} synthetic rows to {output} "
        f"({len(SERVICE_CATALOG)} services, {len(REGIONS)} regions, "
        f"{len(ACCOUNTS)} accounts, {months} months)"
    )


if __name__ == "__main__":
    main()
