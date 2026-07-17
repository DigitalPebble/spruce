
# Quick start using Docker 🐳

## Prerequisites

SPRUCE accepts AWS CUR reports (Parquet), Azure cost details (CSV), or [FOCUS](https://focus.finops.org/)
exports from either provider — see [Generate usage reports](../howto/generate_cur.md) for how to
produce them. This tutorial uses an AWS CUR, generated via
[Data Exports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html) and
stored on S3 as Parquet files; commands for the other inputs are shown below.

For this tutorial, we will assume that you copied the S3 files to your local file system. You can do this with the AWS CLI

```shell
aws s3 cp s3://{bucket}/{prefix}/{data_export_name}/data/ curs --recursive
```

You will also need to have [Docker installed](https://docs.docker.com/engine/install/).

## With Docker

Pull the latest Docker image with

`docker pull ghcr.io/digitalpebble/spruce`

This retrieves a Docker image containing Apache Spark as well as the SPRUCE jar.

The command below processes the data locally by mounting the working directory as a volume. The input reports are
assumed to be in a directory called _curs_.

=== "AWS CUR"

    ```shell
    docker run --rm -v $(pwd):/workspace -w /workspace \
    ghcr.io/digitalpebble/spruce \
    -i curs -o output
    ```

=== "AWS FOCUS"

    ```shell
    docker run --rm -v $(pwd):/workspace -w /workspace \
    ghcr.io/digitalpebble/spruce \
    -i focus -o output -f FOCUS
    ```

=== "Azure FOCUS"

    ```shell
    docker run --rm -v $(pwd):/workspace -w /workspace \
    ghcr.io/digitalpebble/spruce \
    -i focus -o output -p AZURE -f FOCUS
    ```

The `-i` parameter specifies the location of the directory containing the input reports.
The `-o` parameter specifies the location of enriched Parquet files generated in output.

The option `-c` allows to specify a JSON configuration file to [override the default settings](../howto/config_modules.md).
The options `-p` (cloud provider) and `-f` (report format) are described in [Quick start using Apache Spark](with-spark.md).

The directory _output_ contains an enriched copy of the input reports. See [Explore the results](results.md) to understand
what the output contains.

## Reporting with Docker

The same image contains the [reporting tools](https://github.com/DigitalPebble/spruce/tree/main/reporting),
so you can generate a report or explore the enriched output interactively without installing Python.

Generate a static report (format inferred from the suffix: `.md`, `.html` or `.pdf`):

```shell
docker run --rm -v $(pwd):/workspace -w /workspace \
ghcr.io/digitalpebble/spruce \
report -i output -o report.html
```

Launch the interactive dashboard, then open [http://localhost:8501](http://localhost:8501):

```shell
docker run --rm -p 8501:8501 -v $(pwd):/workspace -w /workspace \
ghcr.io/digitalpebble/spruce \
dashboard -i output
```

The image version determines both the SPRUCE jar and the reporting scripts, so when
enriching and reporting are done at different times, use the same explicit tag
(e.g. `ghcr.io/digitalpebble/spruce:1.1`) for both steps rather than relying on `latest`.
