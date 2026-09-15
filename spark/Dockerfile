FROM spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

# Kafka source for Structured Streaming. Versions match the dependencies
# declared in spark-sql-kafka-0-10_2.13:4.1.3's POM. Baking the jars into the
# image (instead of `--packages`) means no Maven download at every start and
# the job works offline.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/
# JDBC driver: used to read the small lookup tables (patients, lab results, trends)
# for the stream-static joins. Writes go through psycopg upserts instead.
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar /opt/spark/jars/

COPY spark/requirements.txt /tmp/requirements.txt
COPY common /tmp/common
# Ubuntu 22.04's pip 22.0 mis-builds pyproject-only packages (installs them as
# "UNKNOWN"), so upgrade pip before installing our shared package.
RUN python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt /tmp/common \
    && rm -rf /tmp/common /tmp/requirements.txt

COPY spark/jobs /opt/app/jobs
RUN mkdir -p /opt/app/checkpoints && chown -R spark:spark /opt/app

USER spark
WORKDIR /opt/app
