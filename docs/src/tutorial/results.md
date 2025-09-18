# Explore the results

Using [DuckDB](https://duckdb.org/) locally (or [Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html) if the output was [written to S3](../howto/s3.md)):

```sql
create table enriched_curs as select * from 'output/**/*.parquet';

select line_item_product_code, product_servicecode,
       round(sum(operational_emissions_co2eq_g)/1000,2) as co2_usage_kg,
       round(sum(embodied_emissions_co2eq_g)/1000, 2) as co2_embodied_kg,
       round(sum(energy_usage_kwh),2) as energy_usage_kwh
       from enriched_curs where operational_emissions_co2eq_g > 0.01
       group by line_item_product_code, product_servicecode
       order by co2_usage_kg desc, co2_embodied_kg desc, energy_usage_kwh desc, product_servicecode;
```

This should give an output similar to

| line_item_product_code | product_servicecode |      line_item_operation       | co2_usage_kg | energy_usage_kwh | co2_embodied_kg |
|------------------------|---------------------|--------------------------------|-------------:|-----------------:|----------------:|
| AmazonEC2              | AmazonEC2           | RunInstances                   | 538.3        | 1220.14          | 303.41          |
| AmazonECS              | AmazonECS           | FargateTask                    | 181.32       | 399.05           | NULL            |
| AmazonS3               | AmazonS3            | OneZoneIAStorage               | 102.3        | 225.15           | NULL            |
| AmazonS3               | AmazonS3            | GlacierInstantRetrievalStorage | 75.89        | 167.03           | NULL            |
| AmazonEC2              | AmazonEC2           | CreateVolume-Gp3               | 41.63        | 91.62            | NULL            |
| AmazonS3               | AmazonS3            | StandardStorage                | 28.51        | 62.81            | NULL            |
| AmazonDocDB            | AmazonDocDB         | CreateCluster                  | 19.79        | 43.56            | NULL            |
| AmazonECS              | AmazonECS           | ECSTask-EC2                    | 9.26         | 20.37            | NULL            |
| AmazonS3               | AmazonS3            | IntelligentTieringAIAStorage   | 2.33         | 5.13             | NULL            |
| AmazonEC2              | AmazonEC2           | CreateSnapshot                 | 2.31         | 5.82             | NULL            |
| AmazonEC2              | AmazonEC2           | RunInstances:SV001             | 1.79         | 3.94             | 0.78            |
| AmazonS3               | AmazonS3            | StandardIAStorage              | 1.19         | 2.61             | NULL            |
| AmazonS3               | AWSDataTransfer     | GetObjectForRepl               | 1.17         | 2.58             | NULL            |
| AmazonS3               | AWSDataTransfer     | UploadPartForRepl              | 1.01         | 2.22             | NULL            |
| AmazonS3               | AmazonS3            | OneZoneIASizeOverhead          | 0.89         | 1.96             | NULL            |
| AmazonEC2              | AmazonEC2           | CreateVolume-Gp2               | 0.84         | 1.84             | NULL            |
| AmazonEC2              | AWSDataTransfer     | RunInstances                   | 0.18         | 0.39             | NULL            |
| AmazonS3               | AWSDataTransfer     | PutObjectForRepl               | 0.16         | 0.36             | NULL            |
| AmazonS3               | AmazonS3            | DeleteObject                   | 0.16         | 0.35             | NULL            |
| AWSBackup              | AWSBackup           | Storage                        | 0.1          | 0.49             | NULL            |
| AmazonMQ               | AmazonMQ            | CreateBroker:0001              | 0.02         | 0.04             | NULL            |
| AmazonECR              | AWSDataTransfer     | downloadLayer                  | 0.01         | 0.01             | NULL            |
| AmazonS3               | AWSDataTransfer     | PutObject                      | 0.0          | 0.0              | NULL            |

To measure the proportion of the costs for which emissions where calculated

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
