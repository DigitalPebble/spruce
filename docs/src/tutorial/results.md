# Explore the results

## Interactive dashboard

For an interactive view of SPRUCE-enriched Parquet output, use the Streamlit
example described in [Build a simple dashboard](../howto/dashboard.md). It reads
the same local paths, globs, and S3 URIs as `report.py`, then charts cost,
energy, emissions, water, regional, and tag breakdowns with DuckDB queries.

## Automated report

The easiest way to explore SPRUCE output is `report.py`, a Python script that reads enriched Parquet files, runs all the analyses described on this page automatically, and writes a formatted report — no SQL required.

### Installation

```shell
pip install -r requirements-report.txt   # duckdb (+ markdown, weasyprint for html/pdf)
```

### Usage

```shell
# Markdown to stdout
python report.py -i output/

# Write to a file — format is inferred from the suffix
python report.py -i output/ -o report.md
python report.py -i output/ -o report.html
python report.py -i output/ -o report.pdf

# Read directly from S3 (uses ambient AWS credentials)
python report.py -i s3://my-bucket/spruced/ -o report.html
```

| Flag | Default | Description |
|---|---|---|
| `-i / --input` | required | Local directory, glob, or S3 URI |
| `-o / --output` | stdout | Output file; format inferred from suffix (`.md`, `.html`, `.pdf`) |
| `-t / --top-tags` | 10 | Maximum number of resource tags offered for interactive breakdown |

### What it produces

The report covers the following sections, drawn from the queries documented below:

| Section | What it shows                                                                                                |
|---|--------------------------------------------------------------------------------------------------------------|
| Summary by Billing Period | Energy (kWh), operational CO₂ (kg), embodied CO₂ (kg), water usage (l)                                       |
| Top Emitters by Service | Top 20 product/service/operation combinations by operational CO₂                                             |
| Top Instance Types | Top 20 instance families by operational + embodied CO₂                                                       |
| Coverage | % of unblended costs that have emissions data; top 20 uncovered services by cost                             |
| Regional Analysis | CO₂, energy, carbon intensity, water, PUE, and gCO₂/$ per AWS region                                         |
| Tag Breakdown | Interactive: emissions split by any resource tag present in the data                                         |
| Recommendations | Automatically generated from coverage gaps, regional carbon intensity, instance families, and billing trends |

After the fixed sections, the script scans `resource_tags` and presents an interactive menu of the most consistently used tag keys, ordered by the percentage of line items that carry a non-empty value. Select a tag to see the emissions breakdown by tag value; press Enter when done. If no tags are found, the report states this clearly.

Recommendations are generated automatically from the data:

| Signal | Condition | Message |
|---|---|---|
| Coverage gap | < 80 % of costs covered | Lists top uncovered services by cost |
| Carbon intensity | Region > 300 gCO₂/kWh with a lower-intensity alternative present | Suggests migration |
| Instance family | Top emitter is an x86 family with a Graviton equivalent | Names the Graviton replacement |
| Emissions trend | ≥ 2 billing periods, last > first × 1.10 | Flags the percentage increase |

## Manual queries

As an alternative to the script above, you can also write equivalent SQL queries using [DuckDB](https://duckdb.org/) locally
 (or [Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html) if the output was [written to S3](../howto/s3.md)):

### Breakdown by billing period

```sql
create table enriched_curs as select * from 'output/**/*.parquet';

select 
       BILLING_PERIOD,
       round(sum(operational_emissions_co2eq_g) / 1000, 2) as co2_usage_kg,
       round(sum(embodied_emissions_co2eq_g) / 1000, 2) as co2_embodied_kg,
       round(sum(operational_energy_kwh),2) as energy_usage_kwh
       from enriched_curs
       group by BILLING_PERIOD
       order by BILLING_PERIOD;
```

This should give an output similar to

| BILLING_PERIOD | co2_usage_kg | co2_embodied_kg | energy_usage_kwh |
|----------------|-------------:|----------------:|-----------------:|
| 2025-05        | 863.51       | 89.73           | 2029.15          |
| 2025-06        | 774.01       | 85.01           | 1811.98          |
| 2025-07        | 812.07       | 87.19           | 1901.13          |
| 2025-08        | 848.7        | 88.15           | 1982.56          |
| 2025-09        | 866.24       | 86.76           | 2017.36          |


### Breakdown per product, service and operation

```sql
select line_item_product_code, product_servicecode, line_item_operation,
       round(sum(operational_emissions_co2eq_g)/1000,2) as co2_usage_kg,
       round(sum(embodied_emissions_co2eq_g)/1000, 2) as co2_embodied_kg,
       round(sum(operational_energy_kwh),2) as energy_usage_kwh
       from enriched_curs where operational_emissions_co2eq_g > 0.01
       group by all
       order by 4 desc, 5 desc, 6 desc, 2;
```

This should give an output similar to

| line_item_product_code |    product_servicecode     |      line_item_operation       | co2_usage_kg | co2_embodied_kg | energy_usage_kwh |
|------------------------|----------------------------|--------------------------------|-------------:|-----------------|-----------------:|
| AmazonECS              | AmazonECS                  | FargateTask                    | 1499.93      | NULL            | 3784.91          |
| AmazonEC2              | AmazonEC2                  | RunInstances                   | 1365.7       | 433.57          | 3068.87          |
| AmazonS3               | AmazonS3                   | GlacierInstantRetrievalStorage | 554.85       | NULL            | 1224.13          |
| AmazonS3               | AmazonS3                   | OneZoneIAStorage               | 249.22       | NULL            | 548.48           |
| AmazonS3               | AmazonS3                   | StandardStorage                | 210.57       | NULL            | 469.54           |
| AmazonEC2              | AmazonEC2                  | CreateVolume-Gp3               | 102.27       | NULL            | 230.39           |
| AmazonEC2              | AmazonEC2                  | RunInstances:SV001             | 66.41        | 3.27            | 146.15           |
| AmazonDocDB            | AmazonDocDB                | CreateCluster                  | 49.93        | NULL            | 109.89           |
| AmazonS3               | AmazonS3                   | IntelligentTieringAIAStorage   | 17.02        | NULL            | 37.47            |
| AmazonEC2              | AmazonEC2                  | CreateVolume-Gp2               | 11.03        | NULL            | 34.37            |
| AmazonS3               | AmazonS3                   | StandardIAStorage              | 9.02         | NULL            | 19.85            |
| AmazonEC2              | AmazonEC2                  | CreateSnapshot                 | 8.05         | NULL            | 20.6             |
| AmazonECR              | AmazonECR                  | TimedStorage-ByteHrs           | 6.96         | NULL            | 15.31            |
| AmazonEC2              | AWSDataTransfer            | RunInstances                   | 2.8          | NULL            | 6.25             |
| AmazonS3               | AmazonS3                   | OneZoneIASizeOverhead          | 2.23         | NULL            | 4.9              |
| AmazonS3               | AWSDataTransfer            | GetObjectForRepl               | 1.84         | NULL            | 4.06             |
| AmazonS3               | AWSDataTransfer            | UploadPartForRepl              | 1.66         | NULL            | 3.64             |
| AmazonS3               | AmazonS3                   | DeleteObject                   | 1.45         | NULL            | 3.2              |
| AmazonMQ               | AmazonMQ                   | CreateBroker:0001              | 1.16         | NULL            | 2.55             |
| AmazonECR              | AmazonECR                  | EUW2-TimedStorage-ByteHrs      | 0.43         | NULL            | 2.16             |
| AmazonS3               | AmazonS3                   | StandardIASizeOverhead         | 0.3          | NULL            | 0.65             |
| AmazonS3               | AWSDataTransfer            | PutObjectForRepl               | 0.19         | NULL            | 0.42             |
| AWSBackup              | AWSBackup                  | Storage                        | 0.13         | NULL            | 0.64             |
| AmazonS3               | AmazonS3GlacierDeepArchive | DeepArchiveStorage             | 0.1          | NULL            | 0.22             |
| AmazonS3               | AWSDataTransfer            | PutObject                      | 0.08         | NULL            | 0.17             |
| AmazonECR              | AWSDataTransfer            | downloadLayer                  | 0.07         | NULL            | 0.19             |
| AmazonEC2              | AWSDataTransfer            | PublicIP-In                    | 0.06         | NULL            | 0.14             |
| AmazonCloudWatch       | AmazonCloudWatch           | HourlyStorageMetering          | 0.02         | NULL            | 0.05             |
| AmazonEFS              | AmazonEFS                  | Storage                        | 0.01         | NULL            | 0.03             |


### Cost coverage 

To measure the proportion of the costs for which emissions were calculated

```sql
select
  round(covered * 100 / "total costs", 2) as percentage_costs_covered
from (
  select
    sum(line_item_unblended_cost) as "total costs",
    sum(line_item_unblended_cost) filter (where operational_emissions_co2eq_g is not null) as covered
  from
    enriched_curs
  where
    line_item_line_item_type like '%Usage'
);
```

The figure will vary depending on the services you use. We have measured up to *77%* coverage for some users. 


### Breakdown per region

```sql
with agg as (
    select
        region as region_code,
        sum(operational_emissions_co2eq_g) as operational_emissions_g,
        sum(embodied_emissions_co2eq_g) as embodied_emissions_g,
        sum(operational_energy_kwh) as energy_kwh,
        sum(pricing_public_on_demand_cost) as public_cost,
        avg(carbon_intensity) as avg_carbon_intensity,
        avg(power_usage_effectiveness) as pue
    from enriched_curs
    where operational_emissions_co2eq_g is not null
    group by 1
)
select
    region_code,
    round(operational_emissions_g / 1000, 2) as co2_usage_kg,
    round(energy_kwh, 2) as energy_usage_kwh,
    round(avg_carbon_intensity, 2) as carbon_intensity,
    round(pue,2) as pue,
    round((operational_emissions_g + embodied_emissions_g) / public_cost, 2) as g_co2_per_dollar
from agg
order by energy_usage_kwh desc, co2_usage_kg desc, region_code desc;
```

*g_co2_per_dollar* being the total emissions (usage + embodied) divided by the public on demand cost.

Below is an example of what the results might look like.

| region_code | co2_usage_kg | energy_usage_kwh | carbon_intensity |  pue | g_co2_per_dollar |
|-------------|-------------:|-----------------:|-----------------:|-----:|-----------------:|
| us-east-1   |      9292.05 |         17969.69 |           400.33 | 1.15 |            33.94 |
| us-east-2   |      1569.79 |          3089.49 |           400.33 | 1.13 |           164.47 |
| eu-west-2   |       583.54 |          2674.09 |           175.03 | 1.11 |            22.32 |
| eu-north-1  |         0.35 |           13.95  |           20.42  |  1.1 |            6.84  |

### Breakdown per user tag

[User tags](https://docs.aws.amazon.com/tag-editor/latest/userguide/tagging.html) are how environmental impacts can be allocated to a business unit, team, product, environment etc... It is as fundamental for a GreenOps practice as it is for [FinOps](https://www.finops.org/wg/cloud-cost-allocation/).
By enriching data at the finest possible level, SPRUCE allows to aggregate the impacts by the tags that are relevant for a given organisation. The syntax to do so for a tag `cost_category_top_level` would be for instance 

```sql
select resource_tags['cost_category_top_level'],
       round(sum(operational_energy_kwh),2) as energie_kwh,
       round(sum(operational_emissions_co2eq_g) / 1000, 2) as operational_kg,
       round(sum(embodied_emissions_co2eq_g) / 1000, 2)    as embodied_kg
       from enriched_curs
       group by 1
       order by 3 desc;
```

