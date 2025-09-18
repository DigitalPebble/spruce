# Quick start using Apache Spark

Instead of using a container, you can run Spruce directly on [Apache Spark](https://spark.apache.org/) either locally or on a cluster.

## Prerequisites

You will need to have CUR reports as inputs. Those are generated via [DataExports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html) and stored on S3 as Parquet files.

For this tutorial, we will assume that you copied the S3 files to your local file system. You can do this with the AWS CLI

```
aws s3 cp s3://bucket/path_to_curs dipe-curs --recursive
```

To run Spruce locally, you need [Apache Spark](https://spark.apache.org/)  installed  and added to the $PATH.

Finally, you need the JAR containing the code and resources for Spruce.  You can copy it from the [latest release](https://github.com/DigitalPebble/spruce/releases) or alternatively, build from source,
which requires Apache Maven and Java 17 or above.s

```
mvn clean package
```

## Run on Apache Spark

If you downloaded a released jar, make sure the path matches its location.

```
spark-submit --class com.digitalpebble.spruce.SparkJob --driver-memory 8g ./target/spruce-*.jar -i ./curs -o ./output
```

The `-i` parameter specifies the location of the directory containing the CUR reports in Parquet format.
The `-o` parameter specifies the location of enriched Parquet files generated in output.

The option `-c` allows to specify a JSON configuration file to override the default settings.

The directory _output_ contains an enriched copy of the input CURs. See [Explore the results](results.md) to understand
what the output contains.
