
## With Docker

Pull the latest Docker image with

`docker pull ghcr.io/digitalpebble/spruce`

This retrieves a Docker image containing Apache Spark as well as the Spruce jar.

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

The option `-c` allows to specify a JSON configuration file to override the default settings.
