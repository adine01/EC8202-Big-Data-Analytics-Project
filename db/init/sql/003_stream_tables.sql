-- Tables maintained by the Spark streaming job (Step 4).
-- Every write is an upsert on a natural key, so a replayed micro-batch
-- (after a crash or restart from checkpoint) cannot create duplicates.

-- 1 h tumbling windows: aggregates + live score at that point in time.
CREATE TABLE IF NOT EXISTS vitals_window_1h (
    patient_id        text        NOT NULL,
    window_start      timestamptz NOT NULL,
    window_end        timestamptz NOT NULL,
    n_readings        integer     NOT NULL,
    hr_avg real, hr_min real, hr_max real,
    spo2_avg real, spo2_min real, spo2_max real,
    sbp_avg real, sbp_min real, sbp_max real,
    dbp_avg real, dbp_min real, dbp_max real,
    temp_avg real, temp_min real, temp_max real,
    last_event_time   timestamptz NOT NULL,
    ews_score         smallint    NOT NULL,   -- NEWS2-style points from window averages
    ews_red_flag      boolean     NOT NULL,   -- any single parameter scored 3
    lab_adjustment    smallint    NOT NULL,   -- from labs in the 48 h before window_end
    lab_flags         text[]      NOT NULL DEFAULT '{}',
    trend             text        NOT NULL DEFAULT 'unknown',
    trend_adjustment  smallint    NOT NULL DEFAULT 0,
    total_score       smallint    NOT NULL,
    risk_tier         text        NOT NULL CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH')),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, window_start)
);
CREATE INDEX IF NOT EXISTS vitals_window_1h_start_idx ON vitals_window_1h (window_start);

-- 4 h windows sliding every 1 h: per-vital slope (units per simulated hour).
CREATE TABLE IF NOT EXISTS vitals_trend_4h (
    patient_id          text        NOT NULL,
    window_start        timestamptz NOT NULL,
    window_end          timestamptz NOT NULL,
    n_readings          integer     NOT NULL,
    hr_slope real, spo2_slope real, sbp_slope real, temp_slope real,
    deterioration_index real        NOT NULL,
    improvement_index   real        NOT NULL,
    trend               text        NOT NULL CHECK (trend IN ('deteriorating', 'improving', 'stable', 'insufficient_data')),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, window_start)
);
CREATE INDEX IF NOT EXISTS vitals_trend_4h_end_idx ON vitals_trend_4h (patient_id, window_end DESC);

-- One row per patient: the ward "right now" view behind the API.
CREATE TABLE IF NOT EXISTS patient_live_status (
    patient_id        text        PRIMARY KEY,
    window_start      timestamptz NOT NULL,
    window_end        timestamptz NOT NULL,
    last_event_time   timestamptz NOT NULL,
    hr_avg real, spo2_avg real, sbp_avg real, dbp_avg real, temp_avg real,
    ews_score         smallint    NOT NULL,
    ews_red_flag      boolean     NOT NULL,
    lab_adjustment    smallint    NOT NULL,
    lab_flags         text[]      NOT NULL DEFAULT '{}',
    trend             text        NOT NULL,
    trend_adjustment  smallint    NOT NULL,
    total_score       smallint    NOT NULL,
    risk_tier         text        NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- Alerts: deterministic id = hash(patient, type, window), so re-processing
-- the same window updates the alert instead of raising a second one.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id        text        PRIMARY KEY,
    patient_id      text        NOT NULL,
    alert_type      text        NOT NULL,
    severity        text        NOT NULL CHECK (severity IN ('medium', 'high', 'critical')),
    window_start    timestamptz NOT NULL,
    window_end      timestamptz NOT NULL,
    observed_value  real,
    threshold       real,
    message         text        NOT NULL,
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at    timestamptz NOT NULL DEFAULT now(),
    acknowledged    boolean     NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS alerts_patient_idx ON alerts (patient_id, window_start DESC);
CREATE INDEX IF NOT EXISTS alerts_window_idx ON alerts (window_start DESC);

-- Lab results (loaded from labs.raw by the stream job; Kappa: the file never
-- goes straight into Postgres).
CREATE TABLE IF NOT EXISTS lab_results (
    sample_id        text        NOT NULL,
    test_type        text        NOT NULL,
    patient_id       text        NOT NULL,
    result_value     real        NOT NULL,
    unit             text        NOT NULL,
    reference_range  text        NOT NULL,
    ref_low          real        NOT NULL,
    ref_high         real        NOT NULL,
    abnormal_flag    char(1)     NOT NULL CHECK (abnormal_flag IN ('L', 'N', 'H')),
    collected_at     timestamptz NOT NULL,
    file_day         date        NOT NULL,
    source_file      text        NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (sample_id, test_type)
);
CREATE INDEX IF NOT EXISTS lab_results_patient_idx ON lab_results (patient_id, test_type, collected_at DESC);

CREATE OR REPLACE VIEW lab_latest AS
SELECT DISTINCT ON (patient_id, test_type) *
FROM lab_results
ORDER BY patient_id, test_type, collected_at DESC;

-- Queryable copy of everything on the dead-letter topic.
CREATE TABLE IF NOT EXISTS dead_letter (
    id             bigserial   PRIMARY KEY,
    source         text        NOT NULL,
    origin         text        NOT NULL,
    error_reasons  text[]      NOT NULL,
    raw_payload    text,
    detected_by    text        NOT NULL,
    detected_at    timestamptz NOT NULL,
    stored_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, origin)
);
CREATE INDEX IF NOT EXISTS dead_letter_detected_idx ON dead_letter (detected_at DESC);
