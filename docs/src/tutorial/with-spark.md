# Quick start using Apache Spark⚡

Instead of using a container, you can run SPRUCE directly on [Apache Spark](https://spark.apache.org/) either locally or on a cluster.

## Prerequisites

You will need to have CUR reports as inputs. Those are generated via [Data Exports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html) and stored on S3 as Parquet files.
See instructions on [Generate Cost and Usage Reports](../howto/generate_cur.md).

For this tutorial, we will assume that you copied the S3 files to your local file system. You can do this with the AWS CLI

```shell
aws s3 cp s3://{bucket}/{prefix}/{data_export_name}/data/ curs --recursive
```

To run SPRUCE locally, you need [Apache Spark](https://spark.apache.org/) installed and added to the $PATH.

Finally, you need the JAR containing the code and resources for SPRUCE.  You can copy it from the [latest release](https://github.com/DigitalPebble/spruce/releases) or alternatively, build from source,
which requires Apache Maven and Java 17 or above.

```shell
mvn clean package
```

## Run on Apache Spark

If you downloaded a released jar, make sure the path matches its location.

```shell
spark-submit --class com.digitalpebble.spruce.SparkJob --driver-memory 8g ./target/spruce-*.jar -i ./curs -o ./output
```

The `-i` parameter specifies the location of the directory containing the CUR reports in Parquet format.
The `-o` parameter specifies the location of enriched Parquet files generated in output.

The option `-c` allows to specify a JSON configuration file to [override the default settings](../howto/config_modules.md).

The option `-p` selects the cloud provider (`AWS`, `AZURE`); it defaults to `AWS`. The option `-f`
selects the format of the input report: `NATIVE` (the provider's own export — the default) or
`FOCUS` for a [FOCUS 1.0](https://focus.finops.org/) export. FOCUS is currently supported for
AWS and Azure. Note that Azure reports are read as CSV files, unlike the AWS ones which are in Parquet:

```shell
spark-submit --class com.digitalpebble.spruce.SparkJob --driver-memory 8g ./target/spruce-*.jar -i ./focus-report -o ./output -p AZURE -f FOCUS
```

The directory _output_ contains an enriched copy of the input CURs. See [Explore the results](results.md) to understand
what the output contains.
