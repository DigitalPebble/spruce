FROM maven:3.9.12-eclipse-temurin-17 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -B -ntp -Dmaven.test.skip=true -f /home/app/pom.xml clean package

FROM apache/spark:4.1.1-java17
COPY --from=build /home/app/target/spruce-*.jar /usr/local/lib/spruce.jar