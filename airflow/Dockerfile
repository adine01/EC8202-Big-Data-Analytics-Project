FROM apache/airflow:3.1.8-python3.12

# Pinning apache-airflow in the same install stops pip from upgrading or
# downgrading Airflow while resolving our packages. ward_common has no
# dependencies and talks to Postgres through the psycopg2 driver already
# shipped in this image. confluent-kafka is pinned to the version in
# Airflow 3.1.8's official constraints file (the tested combination).
COPY --chown=airflow:root common /tmp/common
# pytest runs the DAG integrity tests inside this image (make airflow-test).
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" confluent-kafka==2.13.2 pytest==9.1.1 /tmp/common \
    && rm -rf /tmp/common
