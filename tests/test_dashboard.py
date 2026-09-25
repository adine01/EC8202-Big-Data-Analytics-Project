"""The provisioned Grafana dashboard: up to date with its generator and following the chart rules."""

import importlib.util
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "ward_pipeline.json"
DATASOURCES = (ROOT / "observability" / "grafana" / "provisioning" / "datasources" / "datasources.yml").read_text()


def load_builder():
    spec = importlib.util.spec_from_file_location("build_dashboard", ROOT / "observability/grafana/build_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def panels():
    return [p for p in json.loads(DASHBOARD.read_text())["panels"] if p["type"] != "row"]


def test_committed_json_matches_generator():
    # Guards against editing the JSON by hand (or forgetting `make dashboard`).
    assert json.loads(DASHBOARD.read_text()) == load_builder().build()


def test_every_panel_uses_a_provisioned_datasource():
    uids = set(re.findall(r"uid:\s*(\S+)", DATASOURCES))
    for panel in panels():
        assert panel["datasource"]["uid"] in uids, panel["title"]
        assert panel["targets"], panel["title"]


def test_layout_fits_the_24_column_grid():
    for panel in panels():
        pos = panel["gridPos"]
        assert 0 <= pos["x"] and pos["x"] + pos["w"] <= 24, panel["title"]


def test_timeseries_follow_the_chart_rules():
    builder = load_builder()
    allowed = set(builder.SLOTS)
    for panel in (p for p in panels() if p["type"] == "timeseries"):
        custom = panel["fieldConfig"]["defaults"]["custom"]
        assert custom["lineWidth"] == 2 and custom["fillOpacity"] == 0, panel["title"]
        # One y-axis only: no override moves a series to a second (right) axis.
        for override in panel["fieldConfig"]["overrides"]:
            for prop in override["properties"]:
                assert prop["id"] != "custom.axisPlacement", panel["title"]
                if prop["id"] == "color":
                    assert prop["value"]["fixedColor"] in allowed, panel["title"]
        multi = panel["fieldConfig"]["defaults"]["color"]["mode"] != "fixed"
        # Legend whenever there can be several series; none for a single series.
        assert panel["options"]["legend"]["showLegend"] == multi, panel["title"]


def test_status_colours_are_never_used_as_series_colours():
    builder = load_builder()
    status = {builder.GOOD, builder.WARNING, builder.SERIOUS, builder.CRITICAL}
    assert not status & set(builder.SLOTS)


def test_required_alert_signals_are_on_the_dashboard():
    exprs = " ".join(t.get("expr", "") for p in panels() for t in p["targets"])
    for metric in ("spark_vitals_records_total", "ward_lab_file_overdue", "ward_lab_file_hours_late",
                   "spark_vitals_end_to_end_latency_seconds", "ALERTS"):
        assert metric in exprs, metric
