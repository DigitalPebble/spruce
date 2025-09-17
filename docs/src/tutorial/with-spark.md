## With Spark installed

You can copy the Jar from the [latest release](https://github.com/DigitalPebble/spruce/releases) or alternatively, build from source,
which requires Apache Maven and Java 17 or above.

```
mvn clean package
```

To run Spruce locally, you need [Apache Spark](https://spark.apache.org/)  installed  and added to the $PATH:

```
spark-submit --class com.digitalpebble.spruce.SparkJob --driver-memory 8g ./target/spruce-*.jar -i ./curs -o ./output
```

If you downloaded a released jar, make sure the path matches the location of the file.

The `-i` parameter specifies the location of the directory containing the CUR reports in Parquet format.
The `-o` parameter specifies the location of enriched Parquet files generated in output.

The option `-c` allows to specify a JSON configuration file to override the default settings.
