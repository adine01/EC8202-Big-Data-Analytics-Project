"""Synthetic patient roster and deterioration episodes.

Everything here is a pure function of (seed, patient_id, simulated time).
That makes runs reproducible, and lets the lab simulator (Step 3) recompute
exactly the same hidden patient state as the vitals simulator without the two
processes talking to each other.

Model:
    observed value = baseline + circadian + slow noise + scenario offset
                     + transient spike + measurement noise
This module owns the *baseline* and the *scenario offset*; vitals.py owns the rest.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from ward_common.schemas import patient_id as make_patient_id

VITALS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")


@dataclass(frozen=True)
class Scenario:
    """A clinical storyline: how far each vital moves from baseline at full severity."""

    name: str
    deltas: dict[str, float]
    ramp_hours: float = 0.0       # onset -> full severity (gradual deterioration)
    plateau_hours: float = 0.0    # time spent at full severity
    recovery_hours: float = 0.0   # full severity -> back to baseline
    recurring: bool = True        # another episode after a quiet period?


SCENARIOS: dict[str, Scenario] = {
    "stable": Scenario("stable", {}, recurring=False),
    # Infection: fever, tachycardia, falling blood pressure, mild desaturation.
    "sepsis": Scenario(
        "sepsis",
        {"heart_rate": 35, "spo2": -4, "systolic_bp": -30, "diastolic_bp": -15, "temperature": 2.2},
        ramp_hours=18, plateau_hours=8, recovery_hours=24,
    ),
    # Oxygenation failure: marked desaturation with compensatory tachycardia.
    "respiratory_failure": Scenario(
        "respiratory_failure",
        {"heart_rate": 25, "spo2": -12, "systolic_bp": 10, "diastolic_bp": 5, "temperature": 0.3},
        ramp_hours=10, plateau_hours=6, recovery_hours=18,
    ),
    # Bleeding: fast drop in blood pressure, strong tachycardia.
    "haemorrhage": Scenario(
        "haemorrhage",
        {"heart_rate": 45, "spo2": -2, "systolic_bp": -45, "diastolic_bp": -25, "temperature": -0.5},
        ramp_hours=5, plateau_hours=4, recovery_hours=12,
    ),
    # Admitted unwell and improving: starts at full severity, recovers once.
    "recovering": Scenario(
        "recovering",
        {"heart_rate": 25, "spo2": -3, "systolic_bp": -15, "diastolic_bp": -8, "temperature": 1.5},
        ramp_hours=0, plateau_hours=6, recovery_hours=36, recurring=False,
    ),
}

DETERIORATING = ("sepsis", "respiratory_failure", "haemorrhage", "recovering")


def _smoothstep(x: float) -> float:
    """S-shaped 0->1 curve: slow start, faster middle, slow finish (no sudden corners)."""
    x = min(max(x, 0.0), 1.0)
    return x * x * (3 - 2 * x)


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low), high)


@dataclass(frozen=True)
class Episode:
    onset: datetime
    ramp: timedelta
    plateau: timedelta
    recovery: timedelta
    intensity: float  # scales the scenario deltas (episodes differ in severity)

    @property
    def peak_start(self) -> datetime:
        return self.onset + self.ramp

    @property
    def recovery_start(self) -> datetime:
        return self.peak_start + self.plateau

    @property
    def end(self) -> datetime:
        return self.recovery_start + self.recovery

    def severity(self, t: datetime) -> float:
        """0 outside the episode, rising to `intensity` at the plateau."""
        if t < self.onset or t >= self.end:
            return 0.0
        if t < self.peak_start:
            shape = _smoothstep((t - self.onset) / self.ramp)
        elif t < self.recovery_start:
            shape = 1.0
        else:
            shape = 1.0 - _smoothstep((t - self.recovery_start) / self.recovery)
        return shape * self.intensity


@dataclass(frozen=True)
class Patient:
    patient_id: str
    bed: str
    age: int
    sex: str
    baseline: dict[str, float]
    scenario: str
    seed: int
    sim_epoch: datetime  # SIM_START_DATE 00:00 - episode schedules are anchored here

    @property
    def scenario_def(self) -> Scenario:
        return SCENARIOS[self.scenario]


class EpisodeSchedule:
    """Lazily generated, deterministic sequence of deterioration episodes for one patient."""

    def __init__(self, patient: Patient) -> None:
        self.patient = patient
        self.scenario = patient.scenario_def
        self._rng = random.Random(f"{patient.seed}:{patient.patient_id}:episodes")
        self._episodes: list[Episode] = []
        self._exhausted = self.scenario.name == "stable"

    def _jitter(self, hours: float) -> timedelta:
        return timedelta(hours=hours * self._rng.uniform(0.8, 1.2))

    def _next_episode(self) -> Episode | None:
        rng, sc = self._rng, self.scenario
        if not self._episodes:
            if sc.ramp_hours == 0:
                # Already at full severity when the simulation starts.
                onset = self.patient.sim_epoch
            else:
                onset = self.patient.sim_epoch + timedelta(hours=rng.uniform(4, 36))
        elif sc.recurring:
            onset = self._episodes[-1].end + timedelta(hours=rng.uniform(24, 72))
        else:
            return None
        return Episode(
            onset=onset,
            ramp=self._jitter(sc.ramp_hours),
            plateau=self._jitter(sc.plateau_hours),
            recovery=self._jitter(sc.recovery_hours),
            intensity=rng.uniform(0.8, 1.15),
        )

    def _extend_to(self, t: datetime) -> None:
        while not self._exhausted and (not self._episodes or self._episodes[-1].onset <= t):
            nxt = self._next_episode()
            if nxt is None:
                self._exhausted = True
            else:
                self._episodes.append(nxt)

    def episodes_until(self, t: datetime) -> list[Episode]:
        """All episodes whose onset is at or before t."""
        self._extend_to(t)
        return [e for e in self._episodes if e.onset <= t]

    def episode_at(self, t: datetime) -> Episode | None:
        self._extend_to(t)
        for episode in reversed(self._episodes):
            if episode.onset <= t:
                return episode if t < episode.end else None
        return None

    def severity(self, t: datetime) -> float:
        episode = self.episode_at(t)
        return episode.severity(t) if episode else 0.0

    def offsets(self, t: datetime) -> dict[str, float]:
        """Scenario-driven offset from baseline for each vital at simulated time t."""
        severity = self.severity(t)
        return {name: self.scenario.deltas.get(name, 0.0) * severity for name in VITALS}


def build_roster(
    n_patients: int,
    seed: int,
    sim_start: date,
    deteriorating_fraction: float = 0.5,
) -> list[Patient]:
    """Create a reproducible ward of `n_patients` with plausible adult baselines."""
    if n_patients < 1:
        raise ValueError("n_patients must be >= 1")
    rng = random.Random(f"{seed}:roster")
    epoch = datetime(sim_start.year, sim_start.month, sim_start.day, tzinfo=timezone.utc)

    n_deteriorating = round(n_patients * deteriorating_fraction)
    # Cycle through the scenarios so every storyline appears once the ward is big enough.
    scenarios = [DETERIORATING[i % len(DETERIORATING)] for i in range(n_deteriorating)]
    scenarios += ["stable"] * (n_patients - n_deteriorating)
    rng.shuffle(scenarios)

    roster = []
    for index, scenario in enumerate(scenarios, start=1):
        age = rng.randint(18, 92)
        systolic = _clamp(rng.gauss(112 + 0.25 * age, 10), 98, 155)
        baseline = {
            "heart_rate": _clamp(rng.gauss(76, 8), 56, 96),
            "spo2": _clamp(rng.gauss(97.5 - 0.02 * max(age - 60, 0), 0.8), 94.5, 99.5),
            "systolic_bp": systolic,
            "diastolic_bp": _clamp(systolic * rng.gauss(0.63, 0.03), 58, 95),
            "temperature": _clamp(rng.gauss(36.8, 0.2), 36.2, 37.3),
        }
        roster.append(
            Patient(
                patient_id=make_patient_id(index),
                bed=f"B{index:02d}",
                age=age,
                sex=rng.choice("FM"),
                baseline={k: round(v, 2) for k, v in baseline.items()},
                scenario=scenario,
                seed=seed,
                sim_epoch=epoch,
            )
        )
    return roster


# Convenience for logging / tests.
def describe_patient(p: Patient) -> dict:
    return {"patient_id": p.patient_id, "bed": p.bed, "age": p.age, "scenario": p.scenario}

