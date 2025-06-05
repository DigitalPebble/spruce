mvn clean package
cp target/carbonara-1.0.jar output

docker run -it  -v ./curs:/curs -v ./output:/output  apache/spark:3.5.5-java17 \
/opt/spark/bin/spark-submit  \
--class com.digitalpebble.Carbonara \
--master 'local[*]' \
/output/carbonara-1.0.jar \
/curs /output/test


