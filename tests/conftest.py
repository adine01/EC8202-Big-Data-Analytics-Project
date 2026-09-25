import importlib.util
import os

# Some suites need a heavy runtime and run inside its Docker image instead of the local venv:
#   tests/spark    needs pyspark + Java      -> make spark-test
#   tests/airflow  needs Airflow             -> make airflow-test
# (The repo's own airflow/ folder would shadow the real package locally, so detect the
#  Airflow image by AIRFLOW_HOME rather than by trying to import it.)
collect_ignore = []
if importlib.util.find_spec("pyspark") is None:
    collect_ignore.append("spark")
if "AIRFLOW_HOME" not in os.environ:
    collect_ignore.append("airflow")
