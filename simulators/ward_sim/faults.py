"""Fault injection for the vitals stream.

Real bedside networks produce three kinds of bad data, and the stream job
must handle each one differently:

* malformed  - broken or impossible payloads      -> dead-letter (Step 4)
* late       - valid readings delivered after a   -> accepted if within the
               monitor/network outage                watermark, dropped otherwise
* duplicate  - the same event sent twice (retry   -> de-duplicated by event_id
               after a lost acknowledgement)
"""

from __future__ import annotations

import json
import random
from datetime import timedelta

MALFORMED_KINDS = (
    "invalid_json",
    "missing_field",
    "wrong_type",
    "out_of_range",
    "bp_inverted",
    "bad_timestamp",
    "unknown_patient",
)

# Deliberately outside ward_common.schemas.VITAL_RANGES.
_IMPOSSIBLE_VALUES = {
    "heart_rate": (0, -12, 400),
    "spo2": (0, 120, 150),
    "systolic_bp": (0, 20, 400),
    "diastolic_bp": (0, 5, 250),
    "temperature": (0.0, 20.5, 55.0),
}
_BAD_TIMESTAMPS = ("31/02/2026 25:61", "yesterday", "", "2026-13-45T99:00:00")
_WRONG_TYPES = ("N/A", "eighty", "--", "ERR")


class FaultInjector:
    NORMAL, MALFORMED, LATE, DUPLICATE = "normal", "malformed", "late", "duplicate"

    def __init__(
        self,
        rng: random.Random,
        malformed_rate: float = 0.02,
        late_rate: float = 0.02,
        duplicate_rate: float = 0.01,
        late_min: timedelta = timedelta(minutes=5),
        late_max: timedelta = timedelta(minutes=120),
    ) -> None:
        total = malformed_rate + late_rate + duplicate_rate
        if min(malformed_rate, late_rate, duplicate_rate) < 0 or total > 1:
            raise ValueError("fault rates must be >= 0 and sum to <= 1")
        if late_min > late_max:
            raise ValueError("late_min must be <= late_max")
        self._rng = rng
        self._thresholds = (
            (malformed_rate, self.MALFORMED),
            (malformed_rate + late_rate, self.LATE),
            (total, self.DUPLICATE),
        )
        self.late_min = late_min
        self.late_max = late_max

    def choose(self) -> str:
        """One draw decides the fate of a reading, so the rates are exact and exclusive."""
        draw = self._rng.random()
        for threshold, kind in self._thresholds:
            if draw < threshold:
                return kind
        return self.NORMAL

    def late_delay(self) -> timedelta:
        """How far behind simulated 'now' a late event arrives."""
        low, high = self.late_min.total_seconds(), self.late_max.total_seconds()
        return timedelta(seconds=self._rng.uniform(low, high))

    def corrupt(self, event: dict) -> tuple[bytes, str]:
        """Return a broken serialisation of `event` and the kind of damage done."""
        kind = self._rng.choice(MALFORMED_KINDS)
        bad = dict(event)
        vital = self._rng.choice(sorted(_IMPOSSIBLE_VALUES))

        if kind == "invalid_json":
            text = json.dumps(event)
            return text[: len(text) // 2].encode(), kind
        if kind == "missing_field":
            del bad[self._rng.choice(["patient_id", "timestamp", vital])]
        elif kind == "wrong_type":
            bad[vital] = self._rng.choice(_WRONG_TYPES)
        elif kind == "out_of_range":
            bad[vital] = self._rng.choice(_IMPOSSIBLE_VALUES[vital])
        elif kind == "bp_inverted":
            bad["systolic_bp"], bad["diastolic_bp"] = event["diastolic_bp"], event["systolic_bp"]
        elif kind == "bad_timestamp":
            bad["timestamp"] = self._rng.choice(_BAD_TIMESTAMPS)
        elif kind == "unknown_patient":
            bad["patient_id"] = f"P{self._rng.randint(900, 999)}"
        return json.dumps(bad).encode(), kind
