# Grafana dashboard over SPRUCE output

A Grafana instance in Docker, querying the enriched Parquet on S3 through
Athena. No aggregation step, no in-memory model and no row count at which it
stops working, because every panel is a query rather than a slice of a cached
copy.

The dashboard and the data source are provisioned from files in this
directory, so the whole thing is text that can be reviewed in a pull request
and handed to someone else, which a `.pbix` cannot.

## Before you start

Create the Athena table first, following
[reporting/athena/README.md](../athena/README.md). That covers the DDL
generator, the database, the workgroup and the partition projection this
dashboard relies on.

The AWS identity Grafana uses needs read access to Athena, Glue and the two S3
buckets. Grafana's documented minimal policy, with the S3 resource narrowed to
your own buckets rather than the `aws-athena-query-results-*` wildcard:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AthenaQueryAccess",
      "Effect": "Allow",
      "Action": [
        "athena:ListDatabases", "athena:ListDataCatalogs", "athena:ListWorkGroups",
        "athena:GetDatabase", "athena:GetDataCatalog", "athena:GetQueryExecution",
        "athena:GetQueryResults", "athena:GetTableMetadata", "athena:GetWorkGroup",
        "athena:ListTableMetadata", "athena:StartQueryExecution", "athena:StopQueryExecution"
      ],
      "Resource": ["*"]
    },
    {
      "Sid": "GlueReadAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable",
        "glue:GetTables", "glue:GetPartition", "glue:GetPartitions",
        "glue:BatchGetPartition"
      ],
      "Resource": ["*"]
    },
    {
      "Sid": "DataRead",
      "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
    },
    {
      "Sid": "AthenaResults",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:PutObject",
        "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"
      ],
      "Resource": ["arn:aws:s3:::my-bucket/athena-results/*", "arn:aws:s3:::my-bucket"]
    }
  ]
}
```

Athena needs `s3:PutObject` on the results prefix. Everything else is read-only.

## Run it

```bash
cd reporting/grafana
export $(aws configure export-credentials --format env-no-export)
export AWS_REGION=eu-west-2 SPRUCE_ATHENA_DATABASE=spruce SPRUCE_ATHENA_WORKGROUP=spruce
docker compose up
```

`AWS_REGION` has to be the region the data sits in, not wherever you usually
work: Athena queries the Parquet in place and the Glue catalogue is per region,
so pointing Grafana at another one reports the database as missing.

Set `GRAFANA_PORT` if something already holds 3000, which another Grafana on
the same machine will.

Then open <http://localhost:3000>. Anonymous access is on with the admin role,
since the data is read-only and on your own machine; drop the two
`GF_AUTH_ANONYMOUS_*` lines from `docker-compose.yml` to get a sign-in page.

`aws configure export-credentials` turns whatever your CLI is configured with,
including an SSO session, into the three environment variables the compose file
passes in. Grafana substitutes them into the data source as it provisions it, so
no credential is written into the compose file or the provisioning YAML, and
temporary ones still expire on their own.

They have to be passed explicitly, as `authType: keys`, rather than left to the
AWS SDK's default chain. Grafana hands its backend plugins a curated
environment: `AWS_ACCESS_KEY_ID` and friends exist in the Grafana container but
never reach the Athena plugin process, so `authType: default` finds nothing and
falls through the chain to the EC2 metadata service. To use a named profile
instead, set `authType: credentials` with `profile:` in
`provisioning/datasources/athena.yml` and mount `~/.aws` into the container.

## What you get

A **SPRUCE Overview** dashboard with a billing period selector and seven
panels: scope 2 and scope 3 emissions, energy, coverage of billed usage,
emissions by billing period, the top twenty emitting services, and a per-region
table.

The region table computes carbon intensity and PUE as energy-weighted averages
rather than plain ones:

```sql
round(sum(carbon_intensity * operational_energy_kwh)
      / nullif(sum(operational_energy_kwh), 0), 1) AS grid_g_per_kwh
```

Those columns are ratios, so averaging them across rows weights an hour of a
small instance the same as a month of a large one. On a typical CUR that is an
8% error, and it is worth preserving in anything built on top of this.

Coverage belongs on any page showing an absolute total. Without it the totals
imply a completeness they do not have, since SPRUCE cannot model every service.

## Editing

`allowUiUpdates` is on, so changes made in the browser stick rather than being
reverted on the next reload. To keep one, use the dashboard's share menu to
export the JSON and save it over `dashboards/spruce-overview.json`. The reverse
also works: edit the file and Grafana picks it up within seconds.

## Query notes

**Column names are lower case.** Glue lower-cases every column name in the
catalog while Parquet keeps them as written, so the FOCUS columns are
`billedcost`, `servicename`, `chargecategory` and `skumeter` in SQL. Athena
resolves identifiers case-insensitively, so this matters mostly when reading
the queries against the SPRUCE source.

**Marketplace rows are excluded**, matching `reporting/filter.py` and so the
report and the Streamlit dashboard:

```sql
AND (bill_billing_entity IS NULL OR bill_billing_entity = 'AWS')
```

Third-party AWS Marketplace subscriptions are billed through AWS but run on the
seller's own infrastructure, so nothing in the billing data describes their
energy use, and leaving them in only depresses the coverage figure. On this
data the clause moves coverage for a month from 78.4% to 83.1%. The column is
AWS-native, so remove the clause for an Azure table.

**Filtering is on `billing_period`, not the time picker.** Grafana's time range
wants a timestamp column and the partition is a `YYYY-MM` string, so the two do
not connect by themselves. The time picker is hidden on this dashboard to avoid
implying otherwise. Filtering on the partition column is also what keeps
queries cheap, since it is what Athena prunes on.

**Result reuse is on** at up to 60 minutes per query, so reloading the
dashboard re-reads cached results rather than rescanning S3. Turn it off per
query in the editor when you have just re-run enrichment and want to see the
new numbers immediately.

**Auto-refresh is off** and should stay off. A dashboard left refreshing every
30 seconds is the one realistic way to spend real money on Athena.

See [reporting/athena/README.md](../athena/README.md) for measured scan sizes
and what queries actually cost.

## Troubleshooting

A panel returning no data while the data source tests fine usually means the
partition projection range does not reach the month being asked for. Widen
`projection.billing_period.range` on the table.

A column of NULLs where numbers are expected is the Parquet SerDe failing to
match a name. Check it against `DESCRIBE spruce.enriched` in the Athena console.

If the data source fails to save with a credentials error, the exported
environment variables have probably expired. Re-run the
`aws configure export-credentials` line and restart the container.

`no EC2 IMDS role found` from a health check means the plugin got no
credentials at all and walked to the end of the SDK chain. Either the variables
were not exported before `docker compose up`, or the data source has been
switched to `authType: default`, which cannot work here for the reason given
under [Run it](#run-it).
