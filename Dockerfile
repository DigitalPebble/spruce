FROM maven:3.9-eclipse-temurin-11 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -B -ntp -f /home/app/pom.xml clean package

FROM apache/spark:4.0.1-java21
COPY --from=build /home/app/target/spruce-*.jar /usr/local/lib/spruce.jar
