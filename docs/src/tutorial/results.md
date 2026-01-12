# Explore the results

Using [DuckDB](https://duckdb.org/) locally (or [Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html) if the output was [written to S3](../howto/s3.md)):

## Breakdown by billing period

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


## Breakdown per product, service and operation

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


## Cost coverage 

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


## Breakdown per region

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
    where operational_emissions_co2eq_g > 1
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

| region_code | co2_usage_kg | energy_usage_kwh | carbon_intensity | pue  | g_co2_per_dollar |
|-------------|-------------:|-----------------:|-----------------:|-----:|-----------------:|
| us-east-1   | 6607.83      | 14542.69         | 400.33           | 1.13 | 30.85            |
| us-east-2   | 1150.96      | 2533.06          | 400.33           | 1.13 | 152.91           |
| eu-west-2   | 385.54       | 1940.72          | 175.03           | 1.14 | 27.15            |

## Breakdown per user tag

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



