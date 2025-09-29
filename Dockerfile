FROM maven:3.9.11-eclipse-temurin-11 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -B -ntp -f /home/app/pom.xml clean package -Dmaven.test.skip=true

FROM apache/spark:3.5.5
COPY --from=build /home/app/target/spruce-*.jar /usr/local/lib/spruce.jar
