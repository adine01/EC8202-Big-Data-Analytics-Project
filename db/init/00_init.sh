#!/bin/sh
# Creates roles/databases and applies db/init/sql/*.sql in order.
#
# Idempotent by design: it runs automatically on the first start of an empty
# Postgres volume (docker-entrypoint-initdb.d) and again on every
# `make migrate`, so each later step can add a new numbered SQL file.
set -eu

: "${POSTGRES_USER:?}"
: "${WARD_DB_PASSWORD:?WARD_DB_PASSWORD is not set}"
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD is not set}"
: "${GRAFANA_DB_PASSWORD:?GRAFANA_DB_PASSWORD is not set}"

SQL_DIR="$(dirname "$0")/sql"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
     -v ward_pw="$WARD_DB_PASSWORD" \
     -v airflow_pw="$AIRFLOW_DB_PASSWORD" \
     -v grafana_pw="$GRAFANA_DB_PASSWORD" <<'SQL'
-- Roles: one owner per database plus a read-only role for Grafana.
SELECT 'CREATE ROLE ward LOGIN'       WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ward')       \gexec
SELECT 'CREATE ROLE airflow LOGIN'    WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow')    \gexec
SELECT 'CREATE ROLE grafana_ro LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') \gexec
-- Re-applied every run so .env stays the single source of truth for passwords.
ALTER ROLE ward       WITH LOGIN PASSWORD :'ward_pw';
ALTER ROLE airflow    WITH LOGIN PASSWORD :'airflow_pw';
ALTER ROLE grafana_ro WITH LOGIN PASSWORD :'grafana_pw';

SELECT 'CREATE DATABASE ward OWNER ward'       WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ward')    \gexec
SELECT 'CREATE DATABASE airflow OWNER airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
ALTER DATABASE ward SET timezone TO 'UTC';
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname ward <<'SQL'
REVOKE ALL ON DATABASE ward FROM PUBLIC;
GRANT CONNECT ON DATABASE ward TO ward, grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
-- Tables created later by `ward` are automatically readable by Grafana.
ALTER DEFAULT PRIVILEGES FOR ROLE ward IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
SQL

# Schema objects are created as `ward` so the application role owns them.
for file in "$SQL_DIR"/*.sql; do
    [ -e "$file" ] || continue
    echo "applying $(basename "$file")"
    psql -v ON_ERROR_STOP=1 --username ward --dbname ward -f "$file"
done

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname ward \
     -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;"
