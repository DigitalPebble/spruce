
# Quick start using Docker 🐳

## Prerequisites

You will need to have CUR reports as inputs. Those are generated via [Data Exports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html) and stored on S3 as Parquet files.
See instructions on [Generate Cost and Usage Reports](../howto/generate_cur.md).

For this tutorial, we will assume that you copied the S3 files to your local file system. You can do this with the AWS CLI

```
aws s3 cp s3://{bucket}/{prefix}/{data_export_name}/data/ curs --recursive
```

You will also need to have [Docker installed](https://docs.docker.com/engine/install/).

## With Docker

Pull the latest Docker image with

`docker pull ghcr.io/digitalpebble/spruce`

This retrieves a Docker image containing Apache Spark as well as the SPRUCE jar.

The command below processes the data locally by mounting the directories containing the CURs and output as volumes:
```
docker run -it -v ./curs:/curs -v ./output:/output --rm --name spruce --network host \
ghcr.io/digitalpebble/spruce \
/opt/spark/bin/spark-submit  \
--class com.digitalpebble.spruce.SparkJob \
--driver-memory 4g \
--master 'local[*]' \
/usr/local/lib/spruce.jar \
-i /curs -o /output/enriched
```

The `-i` parameter specifies the location of the directory containing the CUR reports in Parquet format.
The `-o` parameter specifies the location of enriched Parquet files generated in output.

The option `-c` allows to specify a JSON configuration file to [override the default settings](howto/config_modules.md).

The directory _output_ contains an enriched copy of the input CURs. See [Explore the results](results.md) to understand
what the output contains.
