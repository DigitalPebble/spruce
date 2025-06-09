
With Spark installed locally
`mvn clean package`
`spark-submit --class com.digitalpebble.Carbonara  ./target/carbonara-1.0.jar ./curs ./output`

Build the Docker image with
`docker build -t carbonara:1.0 .`

The command below processes the data locally by mounting the directories containing the CURs and output as volumes:
```
docker run -it  -v ./curs:/curs -v ./output:/output  carbonara:1.0 \
/opt/spark/bin/spark-submit  \
--class com.digitalpebble.Carbonara \
--master 'local[*]' \
/usr/local/lib/carbonara-1.0.jar \
/curs /output
```
