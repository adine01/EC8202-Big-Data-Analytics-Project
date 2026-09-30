# Demo script (≈ 8 minutes)

A timed walkthrough for the live demo. Commands are run from the project root; say the
**bold** lines, show the rest.

## Before the demo (not timed)

```bash
make up && make ps                 # all healthy; let it run ≥ 10 min so there are reports
make test-all                      # green, so you can mention it
```

Open in browser tabs: Grafana dashboard (http://localhost:3000 → Ward Monitoring → Ward vitals
pipeline), API docs (http://localhost:8000/docs), Airflow (http://localhost:8080), Prometheus
alerts (http://localhost:9090/alerts), and the latest `reports/risk_report_*.html`.
Keep a terminal ready. Know one sepsis patient id (`make psql` →
`SELECT patient_id FROM patients WHERE scenario='sepsis';`).

---

## 0:00 - The problem and the decision (1 min)

**"A ward needs to know which patients are deteriorating now, and how yesterday's labs change
that picture. Vitals stream in every second; labs arrive once a day."**

Show the ADR diagram (`docs/architecture_decision.md` §4).

**"We chose Kappa: vitals and labs both go through Kafka, and one Spark job computes
everything. The deciding reason is consistency - there is one scoring implementation, so the
live API and the daily report can never disagree. Lambda was rejected because its batch layer
would duplicate that logic to process about 110 lab rows a day."**

## 1:00 - Data flowing in (1 min)

```bash
make consume n=3                   # keyed JSON readings on vitals.raw
make logs s=vitals-simulator | grep deterioration | tail -3
```

**"Synthetic patients follow hidden storylines - sepsis, respiratory failure, haemorrhage -
with gradual deterioration, spikes, and deliberately broken, late and duplicate messages."**
Point out the simulated clock: 1 day = 5 real minutes (`make clock-show`).

## 2:00 - Real-time monitoring (1.5 min)

Grafana: KPI row (patients HIGH/MEDIUM, alerts firing, lab file status, invalid share,
end-to-end latency ≈ 3 s). Scroll to *Processing*: input rate, lag 0, batch duration under
the 5 s trigger.

API (`/docs` or terminal):

```bash
curl -s localhost:8000/ward/summary | jq '.tiers, .active_alerts, .vitals.data_lag_sim_minutes'
curl -s "localhost:8000/alerts?severity=critical&limit=3" | jq '.[].message'
curl -s localhost:8000/patients/P014 | jq '{risk_tier, ews_score, lab_adjustment, lab_flags}'
```

**"Spark validates every message, de-duplicates, applies a 30-minute watermark, computes
1-hour windows and 4-hour trend slopes, scores a NEWS2-style early-warning score and joins the
latest labs in the stream."**

## 3:30 - Yesterday's labs change the picture (1.5 min)

Open the latest HTML report (or `curl -s localhost:8000/reports/latest | jq`).

**"This is the business question. For each patient the report shows the tier from vitals alone
and the tier after adding yesterday's labs. Here a sepsis patient's vitals have settled -
LOW on vitals - but CRP, lactate and troponin are still high, so the labs raise them to
MEDIUM."** Point to the arrow column and to the appendix: every HIGH is a sepsis patient; all
stable patients are LOW.

Airflow UI: show `lab_ingest` (sensor → load) and `daily_risk_report` triggered by the asset.

```bash
make lab-loads                     # per-day ledger: on_time / late / missing / quarantined
```

## 5:00 - Robustness (1.5 min)

Pick two (each takes seconds to start):

```bash
make lab-day d=<a loaded day> f=--force     # identical re-delivery
make dag-trigger d=lab_ingest && sleep 30 && make lab-loads   # duplicate_deliveries +1, nothing republished

echo y | make stream-reset                  # Kappa replay
make stream-status                          # tables rebuilt from Kafka in under a minute
```

**"Every write is an upsert on a natural key and every query has a checkpoint, so a restart
or a replay never duplicates anything. Fixing a bug means fixing the code and replaying -
we actually did that to fix over-sensitive trend alerts."**

## 6:30 - Observability and alerts (1.5 min)

```bash
docker compose stop vitals-simulator        # start this at 6:30
```

While waiting: Prometheus → Alerts. **"Fifteen rules, unit-tested with promtool. The three
required ones: no vitals received, invalid-record rate above 5 %, and the daily lab file
missing or late - measured against the simulated SLA."** Show `VitalsProducerSilent` going
pending → firing (~75 s): **"the source-side alert fires first, so an operator knows it's the
monitors, not Spark."** (`VitalsNotReceived` follows at ~3 min - mention rather than wait.)

```bash
docker compose start vitals-simulator       # restore
```

## 8:00 - Close (30 s)

**"Kappa with one scoring implementation; strict validation with dead-lettering; idempotent,
replayable processing; and observability on every stage with tested alerts. All synthetic -
the score is illustrative, not clinical."**

---

### If something goes wrong

- Spark restarting: `make logs s=spark`; it resumes from checkpoints automatically.
- No report yet: `make dag-trigger d=lab_ingest` (the report DAG follows via the asset).
- Clock looks wrong after a plain `docker compose down`: use `make down`/`make up`, which pause
  and resume the simulated clock.
