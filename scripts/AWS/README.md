# AWS SKU scripts

The scripts in this folder allow to get impacts for SKUs. We get the product descriptions for AWS, normalise into ndjson files.
These ndjson files can then be processed with Spark in order to generate mappings SKUs -> impacts.

Run the script FOCUS.sh in order to get ndjson files.

We can almost consider this enrichment process as a normal one, except it takes ndjson files as inputs and generates csvs. Instead of enriching usage reports, we enrich product descriptions.


