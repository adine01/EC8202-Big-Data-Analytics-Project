# Report notes - Ward Vital Signs Monitoring Pipeline

Material for the 8-15 page report. Each section can be lifted into the report and trimmed.
All measurements were taken on the reference machine (MacBook Pro M5, 16 GB RAM, 8 GB for
Docker) with the default configuration (20 patients, 20 readings/s, 1 simulated day = 5 real
minutes). Numbers marked *sim* are simulated time; everything else is real time.

> All data is synthetic. The risk score is an illustrative, partial NEWS2-style score and is
> not a clinical tool.

Suggested report structure (≈ page budget): 1 Introduction & requirements (1) · 2 Architecture
decision (2) · 3 Design & implementation (3-4) · 4 Observability (1.5-2) · 5 Results (2-3) ·
6 Limitations & production improvements (1-1.5) · 7 Conclusion (0.5).

---

## 1. Problem and requirements

A hospital ward wants near-real-time monitoring of bedside vitals (heart rate, SpO₂, blood
pressure, temperature every few seconds) correlated with lab results that pathology uploads
once a day. Business question: *which patients show concerning vital-sign trends right now,
and how do yesterday's lab results change the risk picture for those patients going forward?*

Required outputs, and where they are delivered:

| Requirement | Delivered by |
|---|---|
| Real-time ward figures via API | `GET /ward/summary`, `/patients`, `/patients/{id}/vitals|trends` |
| Threshold-based per-patient alerts | Spark `vitals_windows` / `vitals_trends` → `alerts` table, `alerts.patient` topic, `GET /alerts` |
| Daily consolidated risk report (vitals trends + latest labs) | Airflow `daily_risk_report` → table + HTML/CSV, `GET /reports/latest` |
| Simulated clock (1 day = 5 min) used consistently | `sim_clock` table + `ward_common.sim_clock` + SQL `sim_now()` |
| Robustness (malformed/late events, DLQ, idempotent loads, checkpoints, watermarks) | §3, §5.3 |
| Observability across ingestion, processing, storage + 3 named alerts | §4 |

## 2. Architecture decision: Kappa (Lambda rejected)

Full record: `docs/architecture_decision.md`. Key argument for the report:

- **The workload.** Vitals are an unbounded stream (~20 events/s); labs are ~110 rows per
  simulated day. The only "batch" input is tiny, so a Lambda *batch layer* would be
  infrastructure without a workload.
- **Consistency (decisive).** Lambda needs the scoring and trend logic twice (batch + speed).
  If they drift, the API and the daily report disagree about the same patient - on a ward,
  contradictory numbers are worse than slightly late ones. In Kappa there is one
  implementation, and the project enforces it further: every rule lives in
  `scoring_rules.py`, Spark generates its column expressions from it, and a parity test proves
  Spark and Python score identically.
- **Latency.** Labs enter the same stream, so they change the live score as soon as Spark
  consumes them - "going forward" is immediate rather than after a nightly batch.
- **Replay.** Kafka keeps vitals for 7 real days (≈ 2,000 sim days) and labs forever. A full
  reprocess is `make stream-reset`: measured **≈ 45 s to rebuild 22 simulated days**
  (≈ 270,000 backlog records across the queries).
- **Cost.** One Spark application (local mode) instead of streaming + batch jobs; the whole
  stack fits in 8 GB.
- **Honest trade-offs.** Replay time grows with history; Kafka becomes critical state; during a
  replay the independent vitals and labs queries can interleave differently from live.

**Rejected alternative - Lambda:** duplicate logic (consistency risk), a second failure mode to
monitor, merge semantics in serving, and roughly double the Spark footprint, all to recompute
~110 lab rows a day.

## 3. Design and implementation

### 3.1 Data generation (synthetic but realistic)

- 20 patients, deterministic from a seed: baselines by age/sex; hidden storylines
  (10 stable, 3 sepsis, 3 respiratory failure, 2 haemorrhage, 2 recovering).
- Vitals = baseline + circadian rhythm + Ornstein-Uhlenbeck drift (autocorrelated, like real
  physiology; tested lag-1 correlation > 0.5) + storyline offset (S-curve onset, plateau,
  recovery, recurring) + short spikes + measurement noise.
- Faults: 2 % malformed (7 kinds), 2 % late (5-120 sim-min, via a delay buffer), 1 % duplicates.
- Labs (7 tests, sex-specific and one-sided reference ranges) are driven by the *same* hidden
  severity with clinically plausible lags (lactate immediate, CRP +18 h), 3 % bad rows,
  10 % late files, 5 % never delivered; files are written atomically and are byte-for-byte
  reproducible (enables the idempotency proof).
- Ground truth (storyline) is stored in `patients` but never sent in the stream: it is used
  only to evaluate the pipeline.

### 3.2 Ingestion (Kafka)

| Topic | Key | Partitions | Retention | Why |
|---|---|---|---|---|
| `vitals.raw` | patient_id | 6 | 7 days | per-patient ordering for trend detection |
| `labs.raw` | patient_id | 3 | forever | Kappa replay source for lab history |
| `alerts.patient` | patient_id | 3 | 7 days | alerts as an event stream for downstream consumers |
| `deadletter` | source | 1 | 7 days | every rejected record with its reasons |

Producers use `acks=all` + idempotence; topic auto-creation is disabled.
Observed partition skew (38-201 messages per partition early on) is the known cost of keying
20 patients into 6 partitions.

### 3.3 Stream processing (Spark Structured Streaming, 5 queries)

Transformations (none are pass-through):

1. **Validation & cleaning** - payloads parsed into Spark 4 `VARIANT`, preserving JSON types,
   so Spark reproduces the Python contract exactly: missing field, wrong type, physiological
   plausibility ranges, diastolic < systolic, timezone-qualified timestamps, unknown patient
   (stream-static join with `patients`). Invalid → `deadletter` with stable error codes.
2. **De-duplication** - `dropDuplicatesWithinWatermark(event_id)`.
3. **Watermark** - 30 sim-minutes; later rows are dropped and counted.
4. **Windowed aggregation** - 1 h tumbling (counts, mean/min/max, breach counts) and 4 h / 1 h
   sliding windows (least-squares slope per vital).
5. **Trend detection** - slopes scaled by stable-patient noise, summed in the worsening
   direction (deterioration index). Calibrated on the simulator: index ≥ 5 catches ≈ 76 % of
   worsening windows at ≈ 1 % false positives; per-vital thresholds could not separate slow
   sepsis (median HR slope 2.4 bpm/h) from noise (2.5 % of stable windows > 3 bpm/h).
6. **Early-warning score** - NEWS2-style points from window averages; "red flag" rule.
7. **Stream/batch join** - each window joined in Spark with the labs collected in the 48 h
   before it ended (batch side read per micro-batch, bounded to the batch's time range so a
   replay joins historically correct labs); rules add up to +4.
8. **Risk tier and alerts** - threshold alerts need ≥ 2 breaching readings in the window
   (single artefacts do not page), HIGH-tier alerts, trend alerts; deterministic `alert_id`.

Sinks: `foreachBatch` → Postgres upserts on natural keys (psycopg, because Spark's JDBC writer
cannot upsert); new alerts also to Kafka (at-least-once with a stable id). Checkpoint per query.

### 3.4 Batch ingestion and daily report (Airflow)

- `lab_ingest` runs once per simulated day. Custom `@task.sensor` with a *simulated-time*
  deadline (06:00 + 4 h), reschedule mode; on expiry the day is recorded `missing`
  and the task fails without retries. The loader (`all_done`) processes every waiting file:
  sha256 → header check → row DQ (shared validator + in-file duplicates) → publish valid rows to
  `labs.raw` and rejected rows to `deadletter`, or quarantine the file if > 20 % of rows fail.
  Ledger `lab_file_loads` makes identical re-deliveries a no-op. Airflow never writes lab
  results to Postgres (Kappa).
- `daily_risk_report` is asset-triggered by `lab_ingest`. It waits (bounded, `soft_fail`) for
  Spark to finish loading the day's labs, then joins the day's windows, trends and alerts with
  the latest labs and scores each patient twice - vitals only, and with labs - using the shared
  rules. Output: table + HTML/CSV report.

### 3.5 Serving (FastAPI)

Endpoints listed in the README. Repository pattern + dependency injection: all SQL sits in one
class that tests replace with an in-memory fake (19 API tests without a database). Input
validation (patient-id pattern, typed dates → no path traversal on report files, bounded
`limit`/`hours`). Timestamps are simulated time and documented as such in OpenAPI.

## 4. Observability design

Principle: every stage answers "is it flowing, is it correct, is it fast?".

| Stage | Signals (metric) | Question answered |
|---|---|---|
| Ingestion | `vitals_events_sent_total{kind}`, `vitals_delivery_total{result}`, delivery latency histogram, `lab_files_written_total`, `lab_files_withheld_total` | Is the source producing? Are brokers acknowledging? |
| Processing | `spark_query_input_rows_total`, `spark_query_kafka_lag_records`, `spark_query_batch_duration_seconds`, `spark_query_rows_dropped_by_watermark_total`, `spark_vitals_records_total{status}`, `spark_vitals_invalid_total{reason}`, `spark_vitals_end_to_end_latency_seconds`, `spark_query_active` | Keeping up? Data quality? Latency? |
| Batch | `ward_lab_files{status}`, `ward_lab_file_overdue`, `ward_lab_file_hours_late`, `ward_lab_rows{result}`, `ward_pipeline_task_runs_total{state}`, task duration | Did today's file arrive, on time, clean? Are DAGs succeeding? |
| Storage | `ward_table_rows{table}`, `ward_db_size_bytes`, `spark_rows_upserted_total{table}`, `ward_db_up` | Are writes landing? How big is it? |
| Serving | `ward_api_requests_total`, request latency histogram, `/health` | Is the API up and fast? |

Design decisions worth defending:

- **Kafka lag from Spark progress**, not a consumer-group exporter: Spark keeps offsets in its
  checkpoint and never commits them to Kafka, so broker-side lag would read zero.
- **No Pushgateway**: Airflow tasks are short-lived; callbacks write outcomes to
  `pipeline_runs`, and the API exposes them (and the lab ledger) as metrics *at scrape time*.
  If the API is down these disappear - caught by `TargetDown`.
- **Alerts as ratios, not counts** where the base rate matters (invalid share, late share):
  a count threshold on late drops fired constantly on normal data and was replaced (§6).
- **Alert rules are tested code** (`promtool test rules`): each required alert has a
  should-fire and a must-not-fire case.
- **Dashboard as code** (`build_dashboard.py`): 38 panels; KPI row first; one measure per panel
  (no dual axes); a validated colour-blind-safe categorical palette assigned in a fixed order
  per entity; status colours reserved for states and always paired with text. A test checks the
  committed JSON equals the generator output.
- **Structured logs**: one JSON object per line from every Python component (`event`,
  `service`, fields), e.g. `alert_raised`, `lab_file_processed`, `lab_file_missing`.

Required alerts:

| Alert | Rule | Measured time to fire |
|---|---|---|
| No vitals received | `time() - spark_vitals_last_processed_timestamp_seconds > 120` for 30 s | ≈ 3 min after stopping the producer (source-side `VitalsProducerSilent` fired first, at ≈ 75 s) |
| Invalid-record rate | invalid share over 5 min > 5 % for 1 min | ≈ 2.5 min after raising malformed input to 15 % |
| Lab file missing or late | `ward_lab_file_hours_late > 2` (late) / `ward_lab_file_overdue == 1` (missing, SLA 4 sim-h) | Late → Missing escalation as the simulated SLA passed; `AirflowTaskFailed` followed from the sensor |

## 5. Results

### 5.1 Performance and resources

| Measure | Result |
|---|---|
| Input rate (default) | ≈ 20-21 readings/s |
| Kafka lag, steady state | 0 records on every query |
| Micro-batch duration | 0.6-2.8 s (trigger 5 s) |
| End-to-end latency (reading produced → Postgres) | ≈ 2.7-3.1 s |
| Full Kappa replay | ≈ 45 s for 22 sim days (≈ 270 k backlog records) |
| Spark memory | 1.95 GB of 2 GB before tuning → ≈ 1.55 GB steady after tuning |
| Whole stack | ≈ 1.1 GB idle before data; ≈ 3.5 GB steady under load (8 GB Docker VM) |
| Database after 22 sim days | ≈ 16 MB |

### 5.2 Detection quality (against hidden ground truth, 22 simulated days)

Alerts per patient per simulated day:

| Storyline | Trend alerts | Threshold alerts | HIGH-risk alerts (total) |
|---|---|---|---|
| sepsis | 2.51 | 5.97 | 315 |
| haemorrhage | 2.03 | 2.84 | 53 |
| respiratory failure | 2.03 | 5.42 | 3 |
| stable | **0.33** | **0.67** | 0 |
| recovering | 0.30 | 0.32 | 0 |

- Deteriorating storylines draw 6-8× more trend alerts than stable patients; stable patients'
  0.33/day is close to the calibrated ≈ 0.26/day (≈ 1 % of 24 windows).
- Threshold alerts on stable patients come from the injected short spikes - intended: a
  genuine spike should page even without a trend.
- Respiratory failure rarely reaches HIGH (low SpO₂ scores 3 points but HR/BP stay near
  normal), which is consistent with a partial NEWS2 that lacks respiratory rate - a limitation
  worth stating.

Daily report (first 5 report days, 100 patient-days): all 50 stable patient-days LOW; every
HIGH was a sepsis patient. **Every lab escalation so far (4 patient-days: P014, P006, P018 ×2)
was a sepsis patient** - e.g. P014: EWS 1 at day end, "LOW" on vitals alone, +4 from
CRP/lactate/creatinine/troponin/WBC → MEDIUM. This is the business question answered: labs
flag a patient whose vitals have settled but whose biochemistry has not, and they did not
escalate a single stable patient.

### 5.3 Robustness and data quality

| Experiment | Result |
|---|---|
| Validation parity Spark vs Python (300+ corrupted payloads) | identical error codes after fixing 3 real discrepancies (§6) |
| Dead-letter coverage | all 7 injected vitals fault types and 9 lab fault types reach `dead_letter` with correct reasons |
| Lab load reconciliation (13 days) | ledger valid rows = `lab_results` rows and ledger rejected = dead letters, for every day |
| Idempotent lab load | identical re-delivery skipped by checksum (`duplicate_deliveries` = 1, no republish) |
| Mostly-bad file | 86/154 bad rows → quarantined, nothing published |
| Missing file | detected at the simulated SLA; later delivery flipped the day to `loaded/late`; a genuinely withheld day (simulator's 5 %) detected independently |
| Spark restart | resumed from checkpoints; 0 duplicate alert keys; no reprocessing |
| Kappa replay | derived tables rebuilt from Kafka; lag back to 0 |
| Clock pause/resume | resumed at the exact simulated time it paused (no jump after downtime) |

Tests: 154 local + 15 Spark + 6 Airflow + promtool rule tests, all green (`make test-all`).

## 6. Problems found and fixed (engineering narrative)

1. **Validation drift caught by parity tests** - JSON `null` is `VOID` in Spark's variant type;
   empty CSV cells mean "missing"; Python's `True` counts as `int`. Each would have made Spark
   and the Airflow/Python validator disagree.
2. **Memory** - Spark sat at 1.95/2 GB. Measured (not guessed): heap 857 MB, metaspace 187 MB,
   537 threads. Heap 768 MB, 512 KB stacks, capped RocksDB state memory and
   `MALLOC_ARENA_MAX=2` brought it to ≈ 1.55 GB steady.
3. **Trend-alert inflation** - stable patients drew ≈ 3 false trend alerts/day, 10× the
   calibration. Cause: update mode re-scored each open 4 h window on every micro-batch, giving
   noisy partial slopes ~10 chances to cross the threshold. Fix: append mode (each window
   evaluated once, complete) → 0.33/day. Re-derived for the whole history with one Kappa replay.
4. **Alert calibration** - a count-based late-drop alert fired on normal data; replaced by a
   share-based rule with a must-not-fire unit test.
5. **Stream trusted its producer** - lab rows for unknown patients passed Spark because the
   roster check lived only in Airflow; Spark now validates too (defence in depth).
6. **Ledger downgrade** - a bad re-delivery of an already-loaded day would have marked the day
   quarantined although good data was loaded; now it is quarantined without touching the ledger.

## 7. Limitations

- Synthetic data; partial NEWS2 (no respiratory rate, consciousness, O₂ therapy); lab weights
  and trend thresholds are illustrative, calibrated only against the simulator.
- Single node: one broker (RF = 1), Spark local mode, single Postgres - no HA.
- Replay ordering between independent queries (labs vs vitals) can differ from live.
- Trend alerts arrive when a 4 h window completes (≈ 1 real min at default speed).
- Local-only security: plaintext, no API authentication, secrets in `.env`.
- Report waits a bounded time for Spark; if Spark is down it is produced with labs flagged.
- Kafka container memory includes page cache from log segments (reclaimable), so its reported
  usage approaches its limit under load without being at risk.

## 8. Production-scale improvements

| Area | Improvement |
|---|---|
| Kafka | ≥ 3 brokers, RF 3, `min.insync.replicas=2`, rack awareness; Schema Registry with Avro/Protobuf data contracts; tiered storage for long replay windows; more partitions sized for ward/hospital count |
| Spark | Cluster mode on Kubernetes; checkpoints and RocksDB state on object storage (S3/GCS); autoscaling on lag; transactional or MERGE-based sinks |
| Storage | Raw archive to a lakehouse table format (Iceberg/Delta on object storage) for multi-year audit and ML; HA Postgres (Patroni) or a time-series/columnar store for long history |
| Airflow | Celery/Kubernetes executor, HA scheduler, deferrable sensors with the triggerer, SLA callbacks |
| Observability | Alertmanager routing to on-call with silences and escalation; OpenTelemetry tracing across producer → Kafka → Spark → API; log aggregation (Loki/ELK); SLOs on end-to-end latency |
| Security & compliance | TLS + SASL on Kafka, TLS to Postgres, OAuth2/OIDC on the API, RBAC, audit logging, PHI handling (encryption at rest, access reviews), HL7 FHIR integration with real devices and the LIS |
| Clinical safety | Full NEWS2 inputs; clinically validated thresholds; human-in-the-loop acknowledgement workflows; model governance before any clinical use |
| Delivery | CI running `make test-all` + image scanning; infrastructure as code; blue/green for the stream job with versioned output tables (Kappa reprocessing pattern) |
