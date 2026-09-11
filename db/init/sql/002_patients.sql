-- Patient roster (dimension table), written by the vitals simulator at start-up.
-- `scenario` is simulation ground truth: it is never sent in the event stream,
-- and is only used afterwards to evaluate whether alerts caught the right patients.
CREATE TABLE IF NOT EXISTS patients (
    patient_id          text        PRIMARY KEY CHECK (patient_id ~ '^P[0-9]{3}$'),
    bed                 text        NOT NULL,
    age                 smallint    NOT NULL CHECK (age BETWEEN 0 AND 120),
    sex                 char(1)     NOT NULL CHECK (sex IN ('F', 'M')),
    baseline_heart_rate real        NOT NULL,
    baseline_spo2       real        NOT NULL,
    baseline_systolic   real        NOT NULL,
    baseline_diastolic  real        NOT NULL,
    baseline_temp       real        NOT NULL,
    scenario            text        NOT NULL,
    active              boolean     NOT NULL DEFAULT true,
    updated_at          timestamptz NOT NULL DEFAULT now()
);
