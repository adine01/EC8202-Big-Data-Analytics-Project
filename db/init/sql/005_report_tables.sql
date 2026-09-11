-- Daily consolidated risk report (Step 6), one row per patient per simulated day.
-- ILLUSTRATIVE ONLY: NEWS2-style scoring on synthetic data, not a clinical tool.
CREATE TABLE IF NOT EXISTS daily_risk_report (
    report_date              date        NOT NULL,
    patient_id               text        NOT NULL,
    bed                      text,
    -- vitals over the day (from vitals_window_1h)
    windows                  integer     NOT NULL,
    readings                 integer     NOT NULL,
    ews_end                  smallint,               -- EWS from the day's last 4 h averages
    ews_peak                 smallint,
    hours_high               integer     NOT NULL,
    hours_medium             integer     NOT NULL,
    hr_max real, spo2_min real, sbp_min real, temp_max real,
    -- trends over the day (from vitals_trend_4h)
    trend_last               text        NOT NULL,
    deteriorating_windows    integer     NOT NULL,
    max_deterioration_index  real,
    -- alerts raised during the day
    alerts_critical          integer     NOT NULL,
    alerts_high              integer     NOT NULL,
    alerts_medium            integer     NOT NULL,
    -- labs: latest result per test collected in the 48 h up to the end of the day
    labs                     jsonb       NOT NULL DEFAULT '{}',
    lab_adjustment           smallint    NOT NULL,
    lab_flags                text[]      NOT NULL DEFAULT '{}',
    labs_status              text        NOT NULL,  -- lab file ledger status for the day
    -- scores
    red_flag                 boolean     NOT NULL,
    trend_adjustment         smallint    NOT NULL,
    vitals_score             smallint,
    total_score              smallint,
    vitals_only_tier         text        NOT NULL,
    risk_tier                text        NOT NULL CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH', 'NO_DATA')),
    tier_change              text        NOT NULL CHECK (tier_change IN ('escalated', 'unchanged', 'no_data')),
    generated_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, patient_id)
);
CREATE INDEX IF NOT EXISTS daily_risk_report_tier_idx ON daily_risk_report (report_date, risk_tier);
