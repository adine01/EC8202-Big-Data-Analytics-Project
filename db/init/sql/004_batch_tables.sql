-- Tables written by Airflow (Step 5+).

-- One row per simulated day: did the lab file arrive, and what happened to it.
-- Doubles as the idempotency ledger: a re-delivered file with the same
-- checksum is recognised and not published again.
CREATE TABLE IF NOT EXISTS lab_file_loads (
    file_day              date        PRIMARY KEY,
    file_name             text        NOT NULL,
    status                text        NOT NULL CHECK (status IN ('missing', 'loaded', 'quarantined')),
    arrival               text        CHECK (arrival IN ('on_time', 'late', 'missing')),
    expected_at           timestamptz NOT NULL,   -- simulated time the file is due
    arrived_at            timestamptz,            -- simulated time it appeared in landing/
    checksum              text,                   -- sha256 of the file bytes
    rows_total            integer,
    rows_valid            integer,
    rows_rejected         integer,
    reject_rate           real,
    reject_reasons        jsonb       NOT NULL DEFAULT '{}',  -- {"non_numeric_result": 2, ...}
    deliveries            integer     NOT NULL DEFAULT 0,     -- distinct versions published
    duplicate_deliveries  integer     NOT NULL DEFAULT 0,     -- identical re-deliveries skipped
    dag_run_id            text,
    loaded_at             timestamptz,            -- real time
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- Task outcomes, recorded by Airflow callbacks; the API turns these into
-- Prometheus metrics (task failures, durations) without a Pushgateway.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id           bigserial   PRIMARY KEY,
    dag_id       text        NOT NULL,
    run_id       text        NOT NULL,
    task_id      text        NOT NULL,
    try_number   integer     NOT NULL,
    state        text        NOT NULL,
    started_at   timestamptz,
    ended_at     timestamptz,
    duration_s   real,
    error        text,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dag_id, run_id, task_id, try_number)
);
CREATE INDEX IF NOT EXISTS pipeline_runs_recent_idx ON pipeline_runs (dag_id, task_id, recorded_at DESC);
