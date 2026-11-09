---
title: "Run carbon estimates on AWS EMR Serverless"
description: "Process Cost and Usage Reports at scale on AWS EMR Serverless so your billing data never leaves your own AWS account."
---

# Run SPRUCE on AWS EMR Serverless

One of the benefits of using SPRUCE is that you can enrich usage reports within AWS without having to expose any of your data to an API or external service.

The easiest way to do this is by running it on [AWS EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide). EMR Serverless is a fully 
managed, on-demand service that lets you run [Apache Spark®](https://spark.apache.org/) without provisioning or managing clusters. You simply submit your jobs, and EMR Serverless automatically provisions, scales, and shuts down the required resources.
You pay only for the compute and memory used while your jobs run, making it ideal for episodic, variable, or exploratory data processing workloads.

Since AWS CUR reports are stored on S3, it makes perfect sense to enrich them straight from there without having to copy them.

EMR is also designed to scale, if your CUR reports are large, this is a good way of enriching them.

[emr-spark-8.1.0](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-version-emr-spark-8.1.0.html) ships with a version of Apache Spark compatible with what SPRUCE uses.

## Setup

Follow these [instructions](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html) to set up EMR Serverless on your AWS account.
You will also need to install the AWS CLI and have configured it.

We will assume that you have CUR reports on S3. You will also need to make the SPRUCE jar available to EMR by placing it in a S3 bucket.
EMR Serverless runs Java 17, whereas the jars on [the release page](https://github.com/DigitalPebble/spruce/releases) are compiled for Java 21 and fail on EMR with an `UnsupportedClassVersionError`.
You will need to compile the jar yourself with JDK 21 or above, targeting Java 17:

```shell
git checkout 1.5
mvn package -Dmaven.compiler.release=17
```

In the example below, we put it in `s3://spruce-jars/spruce-1.5.jar` but since that bucket name is now taken, you will have to choose a different one.

## Launch with the script

The repository contains [`scripts/launchEMR.sh`](https://github.com/DigitalPebble/spruce/blob/main/scripts/launchEMR.sh), which submits a job and follows its progress until it finishes. It requires the AWS CLI and `jq`.

```shell
./scripts/launchEMR.sh
```

The script lists your EMR Serverless applications, with the option of creating a new one, and the IAM roles that EMR Serverless can assume. It then prompts for the jar, the input and output locations, the provider and report format (`AWS` and `NATIVE` by default) and the Spark settings.

After a successful run, your answers are saved to `.launchEMR` in the current directory and offered as defaults the next time. You can also set any of them with an environment variable, the list is at the top of the script.

The next section shows how to do the same with the AWS CLI.

## Run on EMR

Have a look at the [user guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/gs-cli.html).

From the CLI

```shell
aws emr-serverless create-application \
    --type spark \
    --release-label emr-spark-8.1.0 \
    --name spark4-SPRUCE
```

Note the application ID returned in the output. You can always find it with `aws emr-serverless list-applications`.

Check that the application has been created with 

```shell
aws emr-serverless get-application \
    --application-id application-id
```

Next, you launch a job

```shell
aws emr-serverless start-job-run \
    --application-id application-id \
    --execution-role-arn job-role-arn \
    --name job-run-name \
    --job-driver '{
        "sparkSubmit": {
          "entryPoint": "s3://spruce-jars/spruce-1.5.jar",
          "entryPointArguments": [
             "-i", "s3://INPUT-curs/","-o","s3://spruce-output/"
          ],
          "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
        }
    }'
```

where `application-id` is the one you got when creating the application above.
`entryPoint` is the location of the SPRUCE jar. The _entryPointArguments_ are where you specify the inputs and outputs of SPRUCE, as explained in the [Apache Spark quick start](../tutorial/with-spark.md).

Note the _job run ID_ returned in the output.

## Check the results

Depending on the size of your CUR reports, the enrichment will take more or less time. You can check that it has completed with 

```shell
aws emr-serverless get-job-run \
    --application-id application-id \
    --job-run-id job-run-id
```

Look at the tutorial for examples of how to [query the enriched usage reports](../tutorial/results.md).

## Use the AWS Console

You can of course run EMR using the [AWS console](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/gs-console.html). This makes it easier to check the success of a job and access the logs for it.

![An EMR Serverless application in EMR Studio: the application details panel above a table of five batch job runs named job-run-spruce, with their status, run time and links to the logs](../images/emr_studio.png){ width="1543" height="992" }



