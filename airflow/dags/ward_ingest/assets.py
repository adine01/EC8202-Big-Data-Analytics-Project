"""Airflow assets shared between DAGs (defined once so both DAG files agree on the URI)."""

from airflow.sdk import Asset

# Updated by lab_ingest whenever it finishes processing the landing folder.
LABS_PUBLISHED = Asset("kafka://kafka:9092/labs.raw")

# Updated by daily_risk_report after it writes a report.
RISK_REPORT = Asset("file:///opt/airflow/reports/risk_report")
