# Querying SPRUCE output with Athena

Athena lets a BI tool query the enriched Parquet on S3 directly, at line-item
grain, without downloading or aggregating it first. Grafana, QuickSight,
Superset and Power BI all connect to it, and unlike the in-memory BI models
there is no row count at which it stops working.

What it needs is a table in the Glue Data Catalog. Athena has no
`read_parquet()`: `CREATE EXTERNAL TABLE` always takes an explicit column
list, because the catalog holds the schema and the SerDe maps Parquet fields
onto it by name. An AWS CUR carries around 150 columns, so `generate_ddl.py`
reads them from the output rather than leaving them to be typed by hand.

Reading the schema also keeps the DDL correct across pipelines. AWS CUR, AWS
FOCUS, Azure cost details and Azure FOCUS produce four different column sets,
and a DDL fixed for one of them is wrong for the other three.

## 1. Generate the DDL

```bash
pip install -r reporting/requirements-report.txt
python3 reporting/athena/generate_ddl.py \
    -i output/ \
    -l s3://my-bucket/enriched/ \
    -f 2025-01 \
    -o spruce-athena-table.sql
```

- `-i / --input`: enriched Parquet directory, glob, or S3 URI. Only the schema
  is read, so one partition is enough.
- `-l / --location`: the S3 prefix holding the `BILLING_PERIOD=` directories.
- `-f / --first-period`: earliest billing period the projection range covers
  (default `2024-01`). Periods before it are invisible to the table, so err
  early.
- `-t / --table`: qualified table name (default `spruce.enriched`).
- `-o / --output`: file to write to; stdout otherwise.

### Partition projection

The generated table uses partition projection rather than registered
partitions:

```sql
'projection.enabled' = 'true',
'projection.billing_period.type' = 'date',
'projection.billing_period.format' = 'yyyy-MM',
'projection.billing_period.range' = '2025-01,NOW',
'storage.location.template' = 's3://my-bucket/enriched/BILLING_PERIOD=${billing_period}/'
```

Athena derives the partition list arithmetically from that range instead of
listing S3, so no Glue crawler is involved and no `MSCK REPAIR TABLE` is needed
after an enrichment run. A new month is queryable as soon as it lands.

A crawler would infer the same columns, but registering partitions by listing
S3 is exactly what projection avoids: crawl the table and every enrichment run
leaves a month the catalog does not know about until the crawler runs again. To
go that way regardless, crawl once for the column types and then
`ALTER TABLE ... SET TBLPROPERTIES` with the projection properties above.

## 2. Create the database and workgroup

```bash
aws glue create-database --database-input Name=spruce
```

```bash
aws athena create-work-group --name spruce --configuration '{"ResultConfiguration":{"OutputLocation":"s3://my-bucket/athena-results/"},"EnforceWorkGroupConfiguration":true,"PublishCloudWatchMetricsEnabled":true,"BytesScannedCutoffPerQuery":10737418240}'
```

The 10 GiB per-query cutoff is a guard rail rather than a budget. Ordinary
queries come nowhere near it, which is the point: it stops a runaway query from
a mistyped join before it reaches the bill. `EnforceWorkGroupConfiguration`
keeps a BI tool from overriding the result location.

Put an S3 lifecycle rule on the results prefix to expire objects after a week.
Athena writes every result set there and never cleans up.

## 3. Create the table

```bash
aws athena start-query-execution --work-group spruce --query-string file://spruce-athena-table.sql
```

Pasting the file into the Athena console query editor does the same thing.

## 4. Verify

Run these in the console and read the "Data scanned" figure under each.

```sql
SELECT count(*) FROM spruce.enriched WHERE billing_period = '2026-09';
```

```sql
SELECT count(*) FROM spruce.enriched;
```

With projection working the first scans a fraction of the second. If the first
returns no rows, check `storage.location.template` against the real S3 paths:
the partition directories are `BILLING_PERIOD=` in upper case while Glue
lower-cases the column name, so the literal prefix and the `${billing_period}`
placeholder differ in case by design.

Then check the mixed-case columns resolve:

```sql
SELECT "BilledCost", "ServiceName", "SkuMeter", operational_emissions_co2eq_g
FROM spruce.enriched
WHERE billing_period = '2026-09'
LIMIT 5;
```

Glue lower-cases column names in the catalog while Parquet keeps them as
written. Athena's Parquet reader matches case-insensitively, so these resolve,
but a column of NULLs where data is expected points here first, and the FOCUS
bridge columns are the mixed-case ones.

## What it costs

Athena bills per terabyte scanned, with a 10 MB minimum per query, and Parquet
means only the columns a query names are read.

On a 2.2 GB, 21-month AWS CUR enriched by SPRUCE (124 MB per month, one file
per partition), a dashboard panel selecting ten columns scans about 22 MB of a
month, or roughly 460 MB across the full 21 months. At $5 per terabyte that is
a fraction of a penny per panel, and an eight-panel dashboard rebuilt fifty
times over a working day costs well under a pound.

Cost only becomes interesting on a client CUR one or two orders of magnitude
larger, which is the case the configuration above is really for.

## Keeping it fast

Latency is the thing to manage, not spend. Athena takes one to three seconds
per query whatever it scans, and every dashboard panel is one query.

- Filter on `billing_period` in every panel. A BI tool's time picker expects a
  timestamp column and the partition is a `YYYY-MM` string, so the two do not
  connect by themselves; drive it from a dashboard variable instead.
- Turn dashboard auto-refresh off. A dashboard left refreshing every 30 seconds
  is the one realistic way to spend money here.
- Leave `identity_line_item_id` out of anything aggregated. It is the single
  largest column, around 10% of each file, because a high-cardinality unique
  string barely compresses.

When panels stop feeling quick, the next step is a summary table rather than
more tuning: one `CREATE TABLE AS SELECT` aggregating to the grain the
dashboard actually uses (billing period, region, service, charge category),
written back to S3 as Parquet, with the dashboard pointed at that.

Sum the additive columns there, but not `carbon_intensity`,
`power_usage_effectiveness` or `cpu_load_percentage`. Those are ratios, and
averaging them over aggregated rows weights an hour of a small instance the
same as a month of a large one. Store `sum(carbon_intensity *
operational_energy_kwh)` alongside `sum(operational_energy_kwh)` and let the
dashboard divide, which gives a correctly energy-weighted average under
whatever the user has filtered.
