FROM maven:3.9.12-eclipse-temurin-17 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -B -ntp -Dmaven.test.skip=true -f /home/app/pom.xml clean package

FROM apache/spark:4.2.0-java17

USER root
# python3-venv for the reporting tools; pango/cairo/gdk-pixbuf and fonts for
# weasyprint (PDF output of report.py)
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv \
        libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf-2.0-0 \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# requirements copied on their own so editing the scripts does not invalidate
# the pip layer
COPY reporting/requirements-report.txt reporting/requirements-dashboard.txt /opt/spruce/reporting/
RUN python3 -m venv /opt/spruce/venv \
    && /opt/spruce/venv/bin/pip install --no-cache-dir \
       -r /opt/spruce/reporting/requirements-report.txt \
       -r /opt/spruce/reporting/requirements-dashboard.txt
COPY reporting /opt/spruce/reporting

COPY --from=build /home/app/target/spruce-*.jar /usr/local/lib/spruce.jar
COPY entrypoint.sh /opt/spruce/entrypoint.sh

USER spark
EXPOSE 8501
ENTRYPOINT ["/opt/spruce/entrypoint.sh"]
