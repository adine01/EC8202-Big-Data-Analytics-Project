"""Per-patient vital-sign generator.

observed = baseline + circadian + slow noise + scenario offset + spike + measurement noise

* Slow noise is an Ornstein-Uhlenbeck process: it wanders but is pulled back
  to zero, so consecutive readings are correlated like a real patient's,
  instead of independent random jumps.
* Circadian rhythm uses the *simulated* time of day (HR and temperature are
  lowest in the early morning, highest in the late afternoon).
* Spikes are short transient events (1-3 readings), independent of the
  gradual scenario deterioration - they are what a threshold alert sees
  first, while trend detection should see the gradual scenarios.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime

from ward_common.schemas import VITAL_RANGES

from ward_sim.patients import VITALS, EpisodeSchedule, Patient

# name: (stationary sd of slow noise, sd of measurement noise)
NOISE: dict[str, tuple[float, float]] = {
    "heart_rate": (3.0, 1.0),
    "spo2": (0.6, 0.4),
    "systolic_bp": (4.0, 2.0),
    "diastolic_bp": (3.0, 1.5),
    "temperature": (0.08, 0.03),
}

# Peak-to-baseline amplitude of the 24 h rhythm (peak ~17:00, trough ~05:00).
CIRCADIAN: dict[str, float] = {
    "heart_rate": 4.0,
    "systolic_bp": 5.0,
    "diastolic_bp": 3.0,
    "temperature": 0.25,
}
CIRCADIAN_PEAK_HOUR = 17.0

# Mean-reversion rate of the slow noise, per simulated hour.
REVERSION_PER_HOUR = 1.0

# name: (offset ranges applied while active, (min, max) readings it lasts)
SPIKES: dict[str, tuple[dict[str, tuple[float, float]], tuple[int, int]]] = {
    "tachycardia": ({"heart_rate": (30, 60)}, (1, 3)),
    "desaturation": ({"spo2": (-12, -6), "heart_rate": (5, 15)}, (1, 3)),
    "hypertensive": ({"systolic_bp": (30, 50), "diastolic_bp": (10, 20)}, (1, 2)),
    "fever_spike": ({"temperature": (0.8, 1.4), "heart_rate": (8, 18)}, (2, 3)),
}

MIN_PULSE_PRESSURE = 15  # keep systolic - diastolic realistic in valid events


@dataclass
class _ActiveSpike:
    name: str
    offsets: dict[str, float]
    remaining: int


@dataclass(frozen=True)
class Reading:
    values: dict[str, float]
    severity: float          # hidden ground truth, never sent on the wire
    spike: str | None        # hidden ground truth, never sent on the wire


def _round(name: str, value: float) -> float:
    return round(value, 1) if name == "temperature" else int(round(value))


class VitalsGenerator:
    def __init__(self, patient: Patient, spike_rate: float = 0.01) -> None:
        self.patient = patient
        self.schedule = EpisodeSchedule(patient)
        self.spike_rate = spike_rate
        self._rng = random.Random(f"{patient.seed}:{patient.patient_id}:vitals")
        self._slow = {name: self._rng.gauss(0, NOISE[name][0]) for name in VITALS}
        self._last_t: datetime | None = None
        self._spike: _ActiveSpike | None = None

    def _advance_slow_noise(self, t: datetime) -> None:
        if self._last_t is None or t <= self._last_t:
            return
        dt_hours = (t - self._last_t).total_seconds() / 3600
        decay = math.exp(-REVERSION_PER_HOUR * dt_hours)
        spread = math.sqrt(1 - decay * decay)  # exact OU step keeps the stationary sd
        for name in VITALS:
            self._slow[name] = self._slow[name] * decay + NOISE[name][0] * spread * self._rng.gauss(0, 1)

    def _spike_offsets(self) -> tuple[dict[str, float], str | None]:
        if self._spike is None and self._rng.random() < self.spike_rate:
            name = self._rng.choice(sorted(SPIKES))
            ranges, (min_len, max_len) = SPIKES[name]
            self._spike = _ActiveSpike(
                name=name,
                offsets={k: self._rng.uniform(*r) for k, r in ranges.items()},
                remaining=self._rng.randint(min_len, max_len),
            )
        if self._spike is None:
            return {}, None
        spike = self._spike
        spike.remaining -= 1
        if spike.remaining <= 0:
            self._spike = None
        return spike.offsets, spike.name

    def reading(self, t: datetime) -> Reading:
        """Generate the reading a bedside monitor would report at simulated time t."""
        self._advance_slow_noise(t)
        self._last_t = t

        hour = t.hour + t.minute / 60
        phase = math.cos(2 * math.pi * (hour - CIRCADIAN_PEAK_HOUR) / 24)
        scenario = self.schedule.offsets(t)
        spike, spike_name = self._spike_offsets()

        values: dict[str, float] = {}
        for name in VITALS:
            value = (
                self.patient.baseline[name]
                + CIRCADIAN.get(name, 0.0) * phase
                + self._slow[name]
                + scenario[name]
                + spike.get(name, 0.0)
                + self._rng.gauss(0, NOISE[name][1])
            )
            low, high = VITAL_RANGES[name]
            values[name] = min(max(value, low), high)

        values["spo2"] = min(values["spo2"], 100.0)
        values["diastolic_bp"] = min(values["diastolic_bp"], values["systolic_bp"] - MIN_PULSE_PRESSURE)
        values = {name: _round(name, v) for name, v in values.items()}
        return Reading(values=values, severity=self.schedule.severity(t), spike=spike_name)
