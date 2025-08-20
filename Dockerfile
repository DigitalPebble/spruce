FROM maven:3.9.9-eclipse-temurin-21 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

FROM apache/spark:4.0.0-java21
COPY --from=build /home/app/target/spruce-*.jar /usr/local/lib/spruce.jar
