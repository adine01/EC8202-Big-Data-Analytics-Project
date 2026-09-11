-- Shared simulated clock (see common/ward_common/sim_clock.py).
-- A single row enforced by the CHECK constraint.
CREATE TABLE IF NOT EXISTS sim_clock (
    id              smallint         PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    anchor_real     timestamptz      NOT NULL,
    anchor_sim      timestamptz      NOT NULL,
    sim_day_seconds double precision NOT NULL CHECK (sim_day_seconds > 0),
    paused_at       timestamptz,
    updated_at      timestamptz      NOT NULL DEFAULT now()
);

COMMENT ON TABLE sim_clock IS
    'sim_time = anchor_sim + (real_time - anchor_real) * 86400 / sim_day_seconds; frozen at paused_at when set';

-- Same formula as SimClock.to_sim(), for SQL consumers such as Grafana.
-- LEAST ignores NULL, so an un-paused clock simply uses now().
CREATE OR REPLACE FUNCTION sim_now() RETURNS timestamptz
LANGUAGE sql STABLE AS $$
    SELECT anchor_sim + (LEAST(paused_at, now()) - anchor_real) * (86400.0 / sim_day_seconds)
    FROM sim_clock
    WHERE id = 1
$$;
