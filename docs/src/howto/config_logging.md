# Configure logging

It can be useful to change the log levels when implementing a new enrichment module. The logging in Spark is handled with 
log4j.  You need to provide a configuration file and pass it to Spark, a good starting point is to copy the 
[template file](https://github.com/apache/spark/blob/master/conf/log4j2.properties.template) from Spark and save it as e.g. `log4j2.properties`.

The next step is to set the log level for specific resources, for instance adding the section below

```
# SPRUCE
logger.spruce.name = com.digitalpebble.spruce
logger.spruce.level = DEBUG
```

will set the log level to DEBUG for everything in the _com.digitalpebble.spruce_ package. In practice, you would be more specific.

Once the modification is saved, you have two options:

1. Rely on the current location of the file and launch Spark with `spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:///PATH/log4j2.properties" ... ` 
where PATH is where you saved the file. Please note that the path to the file has to be absolute.

2. With the SPRUCE code downloaded locally, have the file in `src/main/resources/log4j2.properties`, recompile the JAR with `mvn clean package` and launch Spark with 
`spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=./log4j2.properties" ... `. In this case, the path is relative.

Either way, the console will display the logs at the level specified. Once you have finished working on the code, don't forget to remove the log file or comment out the section you added.


