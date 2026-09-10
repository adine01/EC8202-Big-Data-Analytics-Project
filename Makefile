SHELL := /bin/bash
COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U ward -d ward
# Kafka CLI tools start a JVM inside the broker container; cap its heap so they
# cannot push the broker over its memory limit.
KAFKA_CLI := $(COMPOSE) exec -e KAFKA_HEAP_OPTS=-Xmx128m kafka /opt/kafka/bin
VENV := .venv

.DEFAULT_GOAL := help
.PHONY: help env check-env build up down restart ps logs clean migrate psql \
        topics offsets consume clock-show clock-pause sim-dry \
        landing lab-day lab-dry stream-status stream-reset spark-test dag-trigger lab-loads dashboard test-alerts airflow-test test-all venv test

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- setup
env: ## Create .env from .env.example with generated secrets
	python3 scripts/init_env.py

check-env:
	@test -f .env || { echo "No .env found - run 'make env' first."; exit 1; }

build: check-env ## Build the project images
	$(COMPOSE) build

# ------------------------------------------------------------- lifecycle
up: check-env ## Start the whole stack (resumes the simulated clock)
	$(COMPOSE) up -d --build
	@$(COMPOSE) ps

down: ## Pause the simulated clock, then stop the stack (data is kept)
	-@$(PSQL) -c "UPDATE sim_clock SET paused_at = now(), updated_at = now() WHERE paused_at IS NULL;" >/dev/null 2>&1 \
		&& echo "sim clock paused"
	$(COMPOSE) down

restart: down up ## Stop and start again

ps: ## Show service status
	$(COMPOSE) ps -a

logs: ## Follow logs (optionally: make logs s=api)
	$(COMPOSE) logs -f --tail=100 $(s)

clean: ## Stop and DELETE all data (volumes, checkpoints, landing files, reports)
	@read -p "This deletes all pipeline data. Continue? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v --remove-orphans
	find data reports -type f ! -name .gitkeep -delete
	find data -mindepth 2 -type d -empty -delete

# ------------------------------------------------------------- utilities
migrate: ## Re-apply db/init (idempotent) after adding new SQL files
	$(COMPOSE) exec -T postgres sh /docker-entrypoint-initdb.d/00_init.sh

psql: ## Open a psql shell on the ward database
	$(COMPOSE) exec postgres psql -U ward -d ward

topics: ## Describe Kafka topics
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server kafka:9092 --describe

offsets: ## Latest offset per partition (optionally: make offsets t=vitals.raw)
	$(KAFKA_CLI)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $(or $(t),vitals.raw)

consume: ## Print the newest messages (optionally: make consume t=deadletter n=10)
	$(KAFKA_CLI)/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic $(or $(t),vitals.raw) --max-messages $(or $(n),5) \
		--property print.key=true --property print.partition=true

clock-show: ## Print the shared simulated clock
	$(COMPOSE) run --rm --no-deps clock-init python -m ward_common.sim_clock show

clock-pause: ## Freeze the simulated clock (resumed by the next `make up`)
	$(COMPOSE) run --rm --no-deps clock-init python -m ward_common.sim_clock pause

sim-dry: ## Print 5 s of simulated vitals to the terminal (no Kafka, no DB)
	$(COMPOSE) run --rm --no-deps vitals-simulator python -m ward_sim.vitals_producer --dry-run --duration 5

landing: ## List lab files in landing/, archive/ and quarantine/
	@for d in landing archive quarantine; do echo "data/$$d:"; ls -1 data/$$d | grep -v '^\.' | sed 's/^/  /'; done

lab-day: ## Drop the lab file for one simulated day now (make lab-day d=2026-01-05 [f=--force])
	@test -n "$(d)" || { echo "usage: make lab-day d=YYYY-MM-DD"; exit 1; }
	$(COMPOSE) run --rm --no-deps lab-simulator python -m ward_sim.lab_generator --day $(d) $(f)

lab-dry: ## Print the lab CSV for one simulated day (make lab-dry d=2026-01-05)
	@test -n "$(d)" || { echo "usage: make lab-dry d=YYYY-MM-DD"; exit 1; }
	@$(COMPOSE) run --rm --no-deps lab-simulator python -m ward_sim.lab_generator --day $(d) --dry-run

# ---------------------------------------------------------------- airflow
dag-trigger: ## Trigger a DAG run now (make dag-trigger d=lab_ingest)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(or $(d),lab_ingest)

lab-loads: ## Lab file ledger: arrival, DQ counts, idempotency counters per day
	@$(PSQL) -c "SELECT file_day, status, arrival, rows_total AS total, rows_valid AS valid, rows_rejected AS rejected, reject_reasons, deliveries, duplicate_deliveries AS dup FROM lab_file_loads ORDER BY file_day;"

# ----------------------------------------------------------- observability
dashboard: ## Regenerate the Grafana dashboard JSON from build_dashboard.py
	python3 observability/grafana/build_dashboard.py

# -------------------------------------------------------- stream processing
stream-status: ## Row counts of the tables the stream job maintains
	@$(PSQL) -c "SELECT 'vitals_window_1h' AS table_name, count(*) FROM vitals_window_1h UNION ALL SELECT 'vitals_trend_4h', count(*) FROM vitals_trend_4h UNION ALL SELECT 'patient_live_status', count(*) FROM patient_live_status UNION ALL SELECT 'alerts', count(*) FROM alerts UNION ALL SELECT 'lab_results', count(*) FROM lab_results UNION ALL SELECT 'dead_letter', count(*) FROM dead_letter;"

stream-reset: ## Kappa replay: wipe derived tables + checkpoints, rebuild everything from Kafka
	@read -p "Delete stream checkpoints and derived tables, then replay from Kafka? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) stop spark
	$(PSQL) -c "TRUNCATE vitals_window_1h, vitals_trend_4h, patient_live_status, alerts, lab_results, dead_letter;"
	find data/checkpoints -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} +
	$(COMPOSE) start spark

# Mount the working tree so tests run against current source, not what the image was built with.
SPARK_TEST_PATH := /src/common:/src/spark/jobs:/src/simulators:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
spark-test: ## Run the Spark tests inside the Spark image (needs Java, so not in the local venv)
	$(COMPOSE) run --rm --no-deps -v .:/src:ro -w /src -e PYTHONPATH=$(SPARK_TEST_PATH) \
		spark python3 -m pytest -q -p no:cacheprovider -c /dev/null --rootdir /src tests/spark

# ----------------------------------------------------------------- tests
venv: ## Create a local virtualenv for tests
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements-dev.txt

test: ## Run the local unit tests (simulators, contracts, scoring, ingestion, report, API, dashboard)
	$(VENV)/bin/pytest -q

test-alerts: ## Validate Prometheus config and unit-test the alert rules with promtool
	docker run --rm --entrypoint sh -v .:/src:ro -w /src prom/prometheus:v3.5.1 -c \
		"promtool check config observability/prometheus/prometheus.yml && promtool test rules tests/prometheus/alert_rules_test.yml"

airflow-test: ## DAG integrity tests inside the Airflow image
	$(COMPOSE) run --rm --no-deps -v ./tests:/opt/airflow/tests:ro -e PYTHONPATH=/opt/airflow/dags airflow-scheduler \
		python -m pytest -q -p no:cacheprovider /opt/airflow/tests/airflow

test-all: test spark-test airflow-test test-alerts ## Every test suite (local, Spark, Airflow, alert rules)
