"""Generate the provisioned Grafana dashboard (observability/grafana/dashboards/ward_pipeline.json).

    python3 observability/grafana/build_dashboard.py      # or: make dashboard

The dashboard is code so it can be reviewed and regenerated, instead of a
hand-edited 2,000-line JSON export. Design rules applied:
  * KPI row of stat tiles first: the headline numbers a ward/ops user needs.
  * One measure per panel, one y-axis (never dual-axis).
  * Series colours from a validated categorical palette (dark-theme steps,
    Grafana's default theme), assigned in a FIXED order per entity, so a
    series keeps its colour whatever else is on screen.
  * Status colours (good / warning / critical) are reserved for states and
    always paired with text (value mappings), never used as series colours.
  * Thin 2 px lines, no fill; legend shown whenever a panel has >= 2 series;
    shared crosshair tooltip.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards" / "ward_pipeline.json"

PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "ward-postgres"}

# Validated categorical palette (dark steps), fixed slot order.
SLOTS = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
# Status palette: reserved for state, never for series identity.
GOOD, WARNING, SERIOUS, CRITICAL = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"

# Fixed entity -> slot assignments (colour follows the entity, not its rank).
QUERY_COLOURS = {"vitals_quality": 0, "vitals_windows": 1, "vitals_trends": 2, "labs": 3, "deadletter_sink": 4}
KIND_COLOURS = {"valid": 0, "malformed": 1, "late": 2, "duplicate": 3}
SEVERITY_COLOURS = {"medium": 0, "high": 1, "critical": 4}
TABLE_COLOURS = {"vitals_window_1h": 0, "vitals_trend_4h": 1, "patient_live_status": 2, "alerts": 3,
                 "lab_results": 4, "dead_letter": 5, "daily_risk_report": 6}

_panel_id = 0


def _next_id() -> int:
    global _panel_id
    _panel_id += 1
    return _panel_id


def prom(expr: str, legend: str = "", ref: str = "A", instant: bool = False) -> dict:
    target = {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend or "__auto", "range": not instant}
    if instant:
        target["instant"] = True
    return target


def pg(sql: str, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": PG, "rawSql": sql, "format": "table", "editorMode": "code", "rawQuery": True}


def colour_overrides(mapping: dict[str, int]) -> list[dict]:
    return [
        {"matcher": {"id": "byName", "options": name},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": SLOTS[slot]}}]}
        for name, slot in mapping.items()
    ]


def thresholds(*steps: tuple[float | None, str]) -> dict:
    return {"mode": "absolute", "steps": [{"value": v, "color": c} for v, c in steps]}


def row(title: str, y: int) -> dict:
    return {"type": "row", "title": title, "id": _next_id(), "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}


def stat(title: str, target: dict, x: int, y: int, w: int = 4, h: int = 4, unit: str = "short",
         steps=((None, GOOD),), mappings=None, description: str = "", decimals: int | None = None) -> dict:
    defaults = {"unit": unit, "thresholds": thresholds(*steps), "color": {"mode": "thresholds"},
                "mappings": mappings or []}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "type": "stat", "title": title, "id": _next_id(), "description": description,
        "datasource": target["datasource"], "targets": [target],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "justifyMode": "auto",
                    "textMode": "value", "orientation": "auto"},
    }


def timeseries(title: str, targets: list[dict], x: int, y: int, w: int = 12, h: int = 8, unit: str = "short",
               colours: dict[str, int] | None = None, single_colour: int | None = 0, description: str = "",
               threshold_line: float | None = None, soft_max: float | None = None) -> dict:
    multi = colours is not None
    custom = {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never", "spanNulls": True,
              "axisSoftMin": 0, "drawStyle": "line", "lineInterpolation": "linear"}
    if soft_max is not None:
        custom["axisSoftMax"] = soft_max
    defaults = {"unit": unit, "custom": custom,
                "color": {"mode": "fixed", "fixedColor": SLOTS[single_colour or 0]} if not multi
                else {"mode": "palette-classic"}}
    if threshold_line is not None:
        custom["thresholdsStyle"] = {"mode": "line"}
        defaults["thresholds"] = thresholds((None, "transparent"), (threshold_line, CRITICAL))
    return {
        "type": "timeseries", "title": title, "id": _next_id(), "description": description,
        "datasource": PROM, "targets": targets, "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": colour_overrides(colours) if multi else []},
        "options": {"legend": {"showLegend": multi, "displayMode": "list", "placement": "bottom", "calcs": []},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def bars(title: str, target: dict, x: int, y: int, w: int = 8, h: int = 8, unit: str = "short",
         colours: dict[str, int] | None = None, description: str = "", steps=None) -> dict:
    """Horizontal bar gauge for comparing magnitudes across categories."""
    defaults = {"unit": unit, "min": 0}
    if steps:
        defaults.update(thresholds=thresholds(*steps), color={"mode": "thresholds"})
    else:
        defaults.update(color={"mode": "fixed", "fixedColor": SLOTS[0]})
    return {
        "type": "bargauge", "title": title, "id": _next_id(), "description": description,
        "datasource": target["datasource"], "targets": [target], "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": colour_overrides(colours) if colours else []},
        "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                    "valueMode": "text", "namePlacement": "left", "sizing": "auto",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
    }


def tier_mappings() -> list[dict]:
    return [{"type": "value", "options": {
        "HIGH": {"text": "HIGH", "color": CRITICAL, "index": 0},
        "MEDIUM": {"text": "MEDIUM", "color": WARNING, "index": 1},
        "LOW": {"text": "LOW", "color": GOOD, "index": 2},
    }}]


def table(title: str, target: dict, x: int, y: int, w: int = 24, h: int = 9, description: str = "",
          overrides: list[dict] | None = None, no_value: str = "No rows") -> dict:
    return {
        "type": "table", "title": title, "id": _next_id(), "description": description,
        "datasource": target["datasource"], "targets": [target], "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}},
                                     "noValue": no_value},
                        "overrides": overrides or []},
        "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}},
    }


def coloured_text_column(name: str, mappings: list[dict]) -> dict:
    return {"matcher": {"id": "byName", "options": name},
            "properties": [{"id": "mappings", "value": mappings},
                           {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}


def build() -> dict:
    panels: list[dict] = []
    y = 0

    # ------------------------------------------------------------ KPI row
    panels.append(row("Ward right now", y)); y += 1
    panels += [
        stat("Patients HIGH risk", prom('ward_patients_by_tier{tier="HIGH"}', instant=True), 0, y,
             steps=((None, GOOD), (1, CRITICAL)),
             description="Patients in the HIGH live tier (illustrative NEWS2-style score, synthetic data)."),
        stat("Patients MEDIUM risk", prom('ward_patients_by_tier{tier="MEDIUM"}', instant=True), 4, y,
             steps=((None, GOOD), (1, WARNING))),
        stat("Alerts firing", prom('count(ALERTS{alertstate="firing", severity=~"critical|warning"}) or vector(0)',
                                   instant=True), 8, y,
             steps=((None, GOOD), (1, CRITICAL)), description="Pipeline alert rules currently firing (Prometheus)."),
        stat("Lab file", prom("ward_lab_file_overdue", instant=True), 12, y,
             steps=((None, GOOD), (1, CRITICAL)),
             mappings=[{"type": "value", "options": {"0": {"text": "✓ on track", "index": 0},
                                                      "1": {"text": "✗ OVERDUE", "index": 1}}}],
             description="Yesterday's lab file against its simulated SLA (06:00 + 4 h)."),
        stat("Invalid vitals (5 m)", prom('sum(rate(spark_vitals_records_total{status="invalid"}[5m])) '
                                          '/ sum(rate(spark_vitals_records_total[5m]))', instant=True), 16, y,
             unit="percentunit", decimals=1, steps=((None, GOOD), (0.05, CRITICAL)),
             description="Share of vitals messages dead-lettered. Alert threshold 5 %."),
        stat("Reading → Postgres", prom("spark_vitals_end_to_end_latency_seconds", instant=True), 20, y,
             unit="s", decimals=1, steps=((None, GOOD), (15, WARNING), (60, CRITICAL)),
             description="Real seconds from the newest reading being produced to it being written to Postgres."),
    ]
    y += 4

    # ------------------------------------------------------------ ingestion
    panels.append(row("Ingestion - bedside monitors → Kafka", y)); y += 1
    panels += [
        timeseries("Vitals messages sent / s by kind",
                   [prom("sum by (kind) (rate(vitals_events_sent_total[1m]))", "{{kind}}")], 0, y,
                   unit="reqps", colours=KIND_COLOURS,
                   description="valid = clean readings; malformed/late/duplicate = injected faults."),
        bars("Malformed messages by damage type (range)",
             prom("sum by (reason) (increase(vitals_malformed_sent_total[$__range]))", "{{reason}}", instant=True),
             12, y, w=6),
        timeseries("Kafka produce → ack latency p95",
                   [prom("histogram_quantile(0.95, sum by (le) (rate(vitals_delivery_latency_seconds_bucket[5m])))",
                         "p95")], 18, y, w=6, unit="s"),
    ]
    y += 8

    # ----------------------------------------------------------- processing
    panels.append(row("Processing - Spark Structured Streaming", y)); y += 1
    panels += [
        timeseries("Input rows / s per streaming query",
                   [prom("sum by (query) (rate(spark_query_input_rows_total[1m]))", "{{query}}")], 0, y, w=8,
                   unit="rowsps", colours=QUERY_COLOURS),
        timeseries("Kafka lag (records not yet processed)",
                   [prom("spark_query_kafka_lag_records", "{{query}}")], 8, y, w=8, colours=QUERY_COLOURS,
                   description="latestOffset − endOffset from Spark progress (Spark does not commit to a consumer group)."),
        timeseries("Micro-batch duration (trigger = 5 s)",
                   [prom("spark_query_batch_duration_seconds", "{{query}}")], 16, y, w=8, unit="s",
                   colours=QUERY_COLOURS, threshold_line=5),
    ]
    y += 8
    panels += [
        timeseries("Invalid vitals share (5 m)",
                   [prom('sum(rate(spark_vitals_records_total{status="invalid"}[5m])) '
                         '/ sum(rate(spark_vitals_records_total[5m]))', "invalid share")], 0, y, w=8,
                   unit="percentunit", threshold_line=0.05, soft_max=0.08,
                   description="Red line = HighInvalidRecordRate alert threshold (5 %)."),
        bars("Dead-lettered vitals by first error (range)",
             prom("topk(8, sum by (reason) (increase(spark_vitals_invalid_total[$__range])))", "{{reason}}",
                  instant=True), 8, y),
        timeseries("Late readings dropped by the watermark",
                   [prom("sum by (query) (increase(spark_query_rows_dropped_by_watermark_total[5m]))", "{{query}}")],
                   16, y, w=8, colours=QUERY_COLOURS),
    ]
    y += 8
    panels += [
        timeseries("New alerts raised by severity",
                   [prom("sum by (severity) (increase(spark_alerts_raised_total[5m]))", "{{severity}}")], 0, y,
                   colours=SEVERITY_COLOURS),
        timeseries("End-to-end latency (reading produced → Postgres)",
                   [prom("spark_vitals_end_to_end_latency_seconds", "latency")], 12, y, unit="s"),
    ]
    y += 8

    # ------------------------------------------------------------------ batch
    panels.append(row("Batch - daily lab files (Airflow)", y)); y += 1
    panels += [
        bars("Lab ledger: days by status", prom("ward_lab_files", "{{status}}", instant=True), 0, y, w=6,
             steps=((None, SLOTS[0]),),
             description="loaded / quarantined (> 20 % bad rows) / missing (not received by the SLA)."),
        timeseries("Yesterday's lab file: simulated hours late",
                   [prom("ward_lab_file_hours_late", "hours late")], 6, y, w=9, unit="h", threshold_line=4,
                   soft_max=6, description="Red line = SLA (4 simulated hours after the 06:00 upload)."),
        bars("Airflow task failures (range)",
             prom('sum by (dag_id, task_id) (increase(ward_pipeline_task_runs_total{state="failed"}[$__range]))',
                  "{{dag_id}}.{{task_id}}", instant=True), 15, y, w=9),
    ]
    y += 8
    panels += [
        bars("Last task duration", prom("ward_pipeline_task_duration_seconds", "{{dag_id}}.{{task_id}}",
                                        instant=True), 0, y, w=12, unit="s"),
        bars("Lab rows across loaded files", prom("ward_lab_rows", "{{result}}", instant=True), 12, y, w=12,
             colours={"valid": 0, "rejected": 1}),
    ]
    y += 8

    # -------------------------------------------------------- storage & serving
    panels.append(row("Storage & serving - Postgres and API", y)); y += 1
    panels += [
        bars("Rows per table", prom("ward_table_rows", "{{table}}", instant=True), 0, y, w=8),
        timeseries("Rows written / s by table",
                   [prom("sum by (table) (rate(spark_rows_upserted_total[1m]))", "{{table}}")], 8, y, w=8,
                   unit="rowsps", colours=TABLE_COLOURS),
        # One overall p95: per-route series would exceed the fixed palette (colour must follow a known entity).
        timeseries("API p95 latency (all routes)",
                   [prom("histogram_quantile(0.95, sum by (le) (rate(ward_api_request_duration_seconds_bucket[5m])))",
                         "p95")], 16, y, w=8, unit="s"),
    ]
    y += 8
    up_map = [{"type": "value", "options": {"1": {"text": "UP", "index": 0}, "0": {"text": "DOWN", "index": 1}}}]
    panels += [
        stat("Scrape targets", prom("up", "{{job}}", instant=True), 0, y, w=18, h=4,
             steps=((None, CRITICAL), (1, GOOD)), mappings=up_map),
        stat("Database size", prom("ward_db_size_bytes", instant=True), 18, y, w=6, h=4, unit="bytes",
             steps=((None, SLOTS[0]),)),
    ]
    panels[-2]["options"]["textMode"] = "value_and_name"
    y += 4

    # ------------------------------------------------------- ward (Postgres)
    panels.append(row("Ward - live status, alerts and daily report (Postgres)", y)); y += 1
    panels.append(table(
        "Patients by live risk (illustrative score)",
        pg("""SELECT s.patient_id AS "Patient", p.bed AS "Bed", s.risk_tier AS "Tier", s.total_score AS "Score",
       s.ews_score AS "EWS", s.lab_adjustment AS "Labs +", s.trend AS "Trend",
       round(s.hr_avg::numeric) AS "HR", round(s.spo2_avg::numeric) AS "SpO2",
       round(s.sbp_avg::numeric) AS "SBP", round(s.temp_avg::numeric, 1) AS "Temp",
       array_to_string(s.lab_flags, ', ') AS "Lab flags"
FROM patient_live_status s JOIN patients p USING (patient_id)
WHERE p.active ORDER BY s.total_score DESC, s.patient_id"""),
        0, y, h=10, overrides=[coloured_text_column("Tier", tier_mappings())]))
    y += 10
    panels.append(table(
        "Active alerts (unacknowledged, last 2 simulated hours)",
        pg("""SELECT to_char(window_end, 'MM-DD HH24:MI') AS "Sim time", patient_id AS "Patient",
       alert_type AS "Alert", severity AS "Severity", message AS "Detail"
FROM alerts
WHERE NOT acknowledged AND window_end >= sim_now() - interval '2 hours'
ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END, window_end DESC
LIMIT 50"""),
        0, y, w=14, h=9, overrides=[coloured_text_column("Severity", [{"type": "value", "options": {
            "critical": {"text": "critical", "color": CRITICAL, "index": 0},
            "high": {"text": "high", "color": SERIOUS, "index": 1},
            "medium": {"text": "medium", "color": WARNING, "index": 2}}}])]))
    panels.append(table(
        "Latest daily report: how labs changed the tier",
        pg("""SELECT patient_id AS "Patient", vitals_only_tier AS "Vitals only", risk_tier AS "With labs",
       CASE tier_change WHEN 'escalated' THEN '↑ raised by labs' ELSE '' END AS "Change",
       total_score AS "Score", array_to_string(lab_flags, ', ') AS "Lab flags"
FROM daily_risk_report
WHERE report_date = (SELECT max(report_date) FROM daily_risk_report)
ORDER BY CASE risk_tier WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 ELSE 2 END, total_score DESC NULLS LAST"""),
        14, y, w=10, h=9, overrides=[coloured_text_column("Vitals only", tier_mappings()),
                                     coloured_text_column("With labs", tier_mappings())]))
    y += 9

    # --------------------------------------------------------- alert rules
    panels.append(row("Pipeline alert rules", y)); y += 1
    panels.append(table(
        "Alert rules firing / pending",
        {**prom('ALERTS', instant=True), "format": "table"}, 0, y, h=7, no_value="✓ No pipeline alerts firing",
        description="Evaluated by Prometheus from alert_rules.yml. Full view: http://localhost:9090/alerts"))

    return {
        "uid": "ward-pipeline",
        "title": "Ward vitals pipeline",
        "description": "Ingestion, processing, batch, storage and serving health of the ward pipeline. "
                       "Synthetic data; risk scores are illustrative, not clinical.",
        "tags": ["ward", "pipeline"],
        "timezone": "utc",
        "editable": False,
        "graphTooltip": 1,  # shared crosshair across panels
        "refresh": "10s",
        "time": {"from": "now-30m", "to": "now"},
        "schemaVersion": 41,
        "version": 1,
        "panels": panels,
        "templating": {"list": []},
        "annotations": {"list": []},
    }


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n")
    print(f"wrote {OUT} ({len(build()['panels'])} panels)")
