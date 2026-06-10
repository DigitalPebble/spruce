# AWS SKU scripts

The scripts in this folder allow to get impacts for SKUs. We get the product descriptions for AWS, normalise into ndjson files.
These ndjson files can then be processed with Spark in order to generate mappings SKUs -> impacts.

Run the script FOCUS.sh in order to get ndjson files.

We can almost consider this enrichment process as a normal one, except it takes ndjson files as inputs and generates csvs. Instead of enriching usage reports, we enrich product descriptions.

TODO

keep it simple and process the json directly 
like `cat AmazonEC2.ndjson | jq .sku,.attributes.instanceType`

and retrieve from src/main/resources/boavizta/instanceTypes.csv

substituting the type with the SKU (or just adding the SKU)

for AmazonS3 have different types of activities: network + storage

schema 
sku
productFamily
attributes with arbitrary key values

TODO check how this is loaded in Spark???

