import json
import random
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import pytest

from ward_common.schemas import validate_vitals_event
from ward_sim.faults import MALFORMED_KINDS, FaultInjector
from ward_sim.patients import SCENARIOS, EpisodeSchedule, build_roster
from ward_sim.vitals import VitalsGenerator
from ward_sim.vitals_producer import Metrics, Simulator, build_event, encode, parse_args

UTC = timezone.utc
START = date(2026, 1, 1)
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
STEP = timedelta(minutes=5)  # ~ one reading per patient at the default rate


def patient_with(scenario, seed=7):
    for p in build_roster(40, seed, START):
        if p.scenario == scenario:
            return p
    raise AssertionError(f"no {scenario} patient in roster")


def mean_between(gen, start, end, field):
    values, t = [], start
    while t < end:
        values.append(gen.reading(t).values[field])
        t += STEP
    return sum(values) / len(values)


# --- roster -------------------------------------------------------------------


def test_roster_is_deterministic_for_a_seed():
    assert build_roster(20, 42, START) == build_roster(20, 42, START)
    assert build_roster(20, 42, START) != build_roster(20, 43, START)


def test_roster_mix_contains_every_storyline():
    counts = Counter(p.scenario for p in build_roster(20, 42, START))
    assert counts["stable"] == 10
    for name in ("sepsis", "respiratory_failure", "haemorrhage", "recovering"):
        assert counts[name] >= 2


def test_patient_ids_are_sequential():
    ids = [p.patient_id for p in build_roster(12, 1, START)]
    assert ids == [f"P{i:03d}" for i in range(1, 13)]


# --- episodes -----------------------------------------------------------------


def test_stable_patient_never_deteriorates():
    schedule = EpisodeSchedule(patient_with("stable"))
    assert all(schedule.severity(EPOCH + timedelta(hours=h)) == 0 for h in range(0, 24 * 20, 3))


def test_recovering_patient_starts_unwell_and_improves():
    schedule = EpisodeSchedule(patient_with("recovering"))
    assert schedule.severity(EPOCH) > 0.7
    assert schedule.severity(EPOCH + timedelta(days=4)) == 0
    assert schedule.severity(EPOCH + timedelta(days=30)) == 0  # never relapses


def test_recurring_scenario_has_repeated_episodes():
    schedule = EpisodeSchedule(patient_with("sepsis"))
    hours = [h for h in range(0, 24 * 30) if schedule.severity(EPOCH + timedelta(hours=h)) > 0]
    onsets = [h for h in hours if h - 1 not in hours]
    assert len(onsets) >= 3


def test_severity_ramps_gradually():
    ep = EpisodeSchedule(patient_with("sepsis")).episodes_until(EPOCH + timedelta(days=10))[0]
    quarter = ep.onset + ep.ramp / 4
    assert 0 < ep.severity(quarter) < ep.severity(ep.onset + ep.ramp / 2) < ep.severity(ep.peak_start)


# --- vitals -------------------------------------------------------------------


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_generated_readings_always_pass_validation(scenario):
    patient = patient_with(scenario)
    gen = VitalsGenerator(patient, spike_rate=0.05)
    t = EPOCH
    for _ in range(3000):
        event = build_event(patient.patient_id, gen.reading(t).values, t, random.Random(1))
        assert validate_vitals_event(event) == [], event
        t += STEP


@pytest.mark.parametrize(
    "scenario,field,direction",
    [
        ("sepsis", "heart_rate", +1),
        ("sepsis", "temperature", +1),
        ("sepsis", "systolic_bp", -1),
        ("respiratory_failure", "spo2", -1),
        ("haemorrhage", "systolic_bp", -1),
        ("haemorrhage", "heart_rate", +1),
    ],
)
def test_scenarios_move_the_right_vitals(scenario, field, direction):
    patient = patient_with(scenario)
    gen = VitalsGenerator(patient, spike_rate=0)
    ep = EpisodeSchedule(patient).episodes_until(EPOCH + timedelta(days=10))[0]
    baseline = mean_between(gen, ep.onset - timedelta(hours=12), ep.onset, field)
    peak = mean_between(gen, ep.peak_start, ep.recovery_start, field)
    assert (peak - baseline) * direction > 0


def test_readings_are_autocorrelated_not_white_noise():
    gen = VitalsGenerator(patient_with("stable"), spike_rate=0)
    t, hr = EPOCH, []
    for _ in range(2000):
        hr.append(gen.reading(t).values["heart_rate"])
        t += STEP
    mean = sum(hr) / len(hr)
    var = sum((x - mean) ** 2 for x in hr)
    lag1 = sum((hr[i] - mean) * (hr[i + 1] - mean) for i in range(len(hr) - 1)) / var
    assert lag1 > 0.5


def test_spikes_are_rare_and_short():
    gen = VitalsGenerator(patient_with("stable"), spike_rate=0.01)
    t, spiking, runs, run = EPOCH, 0, [], 0
    for _ in range(20_000):
        if gen.reading(t).spike:
            spiking += 1
            run += 1
        elif run:
            runs.append(run)
            run = 0
        t += STEP
    # ~1% start rate x ~2 readings each -> roughly 2% of readings affected.
    assert 0.01 < spiking / 20_000 < 0.04
    assert sum(r <= 3 for r in runs) / len(runs) > 0.95


# --- faults -------------------------------------------------------------------


def test_fault_rates_are_respected():
    inj = FaultInjector(random.Random(3), malformed_rate=0.1, late_rate=0.05, duplicate_rate=0.02)
    counts = Counter(inj.choose() for _ in range(50_000))
    assert counts["malformed"] / 50_000 == pytest.approx(0.10, abs=0.01)
    assert counts["late"] / 50_000 == pytest.approx(0.05, abs=0.01)
    assert counts["duplicate"] / 50_000 == pytest.approx(0.02, abs=0.005)


def test_every_malformed_event_fails_validation():
    patient = patient_with("stable")
    event = build_event(patient.patient_id, VitalsGenerator(patient).reading(EPOCH).values, EPOCH, random.Random(1))
    inj = FaultInjector(random.Random(5))
    seen = set()
    for _ in range(500):
        payload, kind = inj.corrupt(event)
        seen.add(kind)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            assert kind == "invalid_json"
            continue
        assert validate_vitals_event(decoded, known_patients={patient.patient_id}), kind
    assert seen == set(MALFORMED_KINDS)


def test_invalid_fault_configuration_rejected():
    with pytest.raises(ValueError):
        FaultInjector(random.Random(), malformed_rate=0.6, late_rate=0.6)


# --- end-to-end dry run ---------------------------------------------------------


class ListSink:
    def __init__(self):
        self.messages = []

    def send(self, key, value):
        self.messages.append((key, value))

    def poll(self):
        pass

    def flush(self):
        return 0


def test_simulator_dry_run_produces_keyed_events(monkeypatch):
    from ward_common.sim_clock import SimClock, utcnow

    monkeypatch.setenv("SIM_START_DATE", "2026-01-01")
    args = parse_args(["--patients", "5", "--rate", "50", "--duration", "1.5", "--dry-run",
                       "--malformed-rate", "0", "--late-rate", "0", "--duplicate-rate", "0"])
    clock = SimClock.starting(START, 300, utcnow())
    sink = ListSink()
    Simulator(args, lambda: clock, sink, Metrics(enabled=False, port=0)).run()

    assert 50 <= len(sink.messages) <= 90  # ~75 expected at 50/s for 1.5 s
    keys = {key for key, _ in sink.messages}
    assert keys == {f"P00{i}" for i in range(1, 6)}
    for key, value in sink.messages:
        event = json.loads(value)
        assert event["patient_id"] == key
        assert validate_vitals_event(event) == []


def test_encode_is_compact_json():
    assert encode({"a": 1, "b": [1, 2]}) == b'{"a":1,"b":[1,2]}'
