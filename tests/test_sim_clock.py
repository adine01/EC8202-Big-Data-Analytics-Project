from datetime import date, datetime, timedelta, timezone

import pytest

from ward_common.sim_clock import ClockReader, SimClock

UTC = timezone.utc
REAL_T0 = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)


def default_clock() -> SimClock:
    return SimClock.starting(date(2026, 1, 1), sim_day_seconds=300, real_now=REAL_T0)


def test_default_speedup_is_288():
    assert default_clock().speedup == 288


def test_five_real_minutes_is_one_sim_day():
    clock = default_clock()
    assert clock.to_sim(REAL_T0) == datetime(2026, 1, 1, tzinfo=UTC)
    assert clock.to_sim(REAL_T0 + timedelta(minutes=5)) == datetime(2026, 1, 2, tzinfo=UTC)


def test_one_sim_hour_is_12_5_real_seconds():
    assert default_clock().real_seconds(timedelta(hours=1)) == pytest.approx(12.5)
    assert default_clock().sim_delta(12.5) == timedelta(hours=1)


def test_to_real_inverts_to_sim():
    clock = default_clock()
    real = REAL_T0 + timedelta(seconds=137)
    assert clock.to_real(clock.to_sim(real)) == real


def test_pause_freezes_sim_time():
    paused = default_clock().paused(REAL_T0 + timedelta(minutes=5))
    later = REAL_T0 + timedelta(hours=3)
    assert paused.to_sim(later) == datetime(2026, 1, 2, tzinfo=UTC)


def test_pausing_twice_keeps_first_pause():
    first = default_clock().paused(REAL_T0 + timedelta(minutes=1))
    assert first.paused(REAL_T0 + timedelta(minutes=9)) is first


def test_reanchor_resumes_without_a_jump_after_downtime():
    paused = default_clock().paused(REAL_T0 + timedelta(minutes=5))  # sim 2026-01-02 00:00
    resume_at = REAL_T0 + timedelta(hours=10)  # stack was down for 10 real hours
    resumed = paused.reanchored(resume_at)
    assert not resumed.is_paused
    assert resumed.to_sim(resume_at) == datetime(2026, 1, 2, tzinfo=UTC)
    assert resumed.to_sim(resume_at + timedelta(minutes=5)) == datetime(2026, 1, 3, tzinfo=UTC)


def test_reanchor_with_new_speed_is_continuous():
    clock = default_clock()
    switch_at = REAL_T0 + timedelta(minutes=5)
    faster = clock.reanchored(switch_at, sim_day_seconds=60)
    assert faster.to_sim(switch_at) == clock.to_sim(switch_at)
    assert faster.to_sim(switch_at + timedelta(minutes=1)) == datetime(2026, 1, 3, tzinfo=UTC)


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError):
        SimClock(anchor_real=datetime(2026, 1, 1), anchor_sim=REAL_T0, sim_day_seconds=300)
    with pytest.raises(ValueError):
        default_clock().to_sim(datetime(2026, 1, 1))


def test_non_positive_day_length_is_rejected():
    with pytest.raises(ValueError):
        SimClock(anchor_real=REAL_T0, anchor_sim=REAL_T0, sim_day_seconds=0)


# --- ClockReader --------------------------------------------------------------


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return self.row


class FakeConn:
    def __init__(self, row):
        self.row = row

    def cursor(self):
        return FakeCursor(self.row)

    def close(self):
        pass


def make_row(day_seconds=300):
    return (REAL_T0, datetime(2026, 1, 1, tzinfo=UTC), day_seconds, None)


def test_reader_caches_until_refresh_interval():
    calls = []
    now = [0.0]

    def connect():
        calls.append(1)
        return FakeConn(make_row())

    reader = ClockReader(connect, refresh_seconds=30, monotonic=lambda: now[0])
    reader.clock()
    now[0] = 10
    reader.clock()
    assert len(calls) == 1
    now[0] = 31
    reader.clock()
    assert len(calls) == 2


def test_reader_keeps_last_clock_when_db_fails():
    now = [0.0]
    healthy = [True]

    def connect():
        if not healthy[0]:
            raise ConnectionError("db down")
        return FakeConn(make_row())

    reader = ClockReader(connect, refresh_seconds=30, monotonic=lambda: now[0])
    first = reader.clock()
    healthy[0] = False
    now[0] = 60
    assert reader.clock() == first


def test_reader_raises_when_clock_never_initialised():
    reader = ClockReader(lambda: FakeConn(None), refresh_seconds=30)
    with pytest.raises(RuntimeError):
        reader.clock()
