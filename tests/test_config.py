from datetime import date

import pytest

from ward_common.config import ClockSettings, ConfigError, KafkaSettings, PostgresSettings


def test_postgres_password_is_required(monkeypatch):
    monkeypatch.delenv("WARD_DB_PASSWORD", raising=False)
    with pytest.raises(ConfigError, match="WARD_DB_PASSWORD"):
        PostgresSettings.from_env()


def test_postgres_password_never_appears_in_repr(monkeypatch):
    monkeypatch.setenv("WARD_DB_PASSWORD", "s3cret-value")
    settings = PostgresSettings.from_env()
    assert "s3cret-value" not in repr(settings)
    assert settings.connect_kwargs()["password"] == "s3cret-value"


def test_clock_defaults(monkeypatch):
    for name in ("SIM_DAY_SECONDS", "SIM_START_DATE", "SIM_CLOCK_REFRESH_SECONDS"):
        monkeypatch.delenv(name, raising=False)
    settings = ClockSettings.from_env()
    assert settings.sim_day_seconds == 300
    assert settings.sim_start_date == date(2026, 1, 1)


@pytest.mark.parametrize(
    "name,value",
    [("SIM_DAY_SECONDS", "0"), ("SIM_DAY_SECONDS", "abc"), ("SIM_START_DATE", "01/01/2026")],
)
def test_invalid_clock_settings_fail_fast(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError):
        ClockSettings.from_env()


def test_blank_values_fall_back_to_defaults(monkeypatch):
    monkeypatch.setenv("TOPIC_VITALS", "   ")
    assert KafkaSettings.from_env().topic_vitals == "vitals.raw"
