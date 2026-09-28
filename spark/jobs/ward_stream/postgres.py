"""Idempotent writes from foreachBatch into Postgres.

Why psycopg on the driver rather than Spark's JDBC writer: the JDBC writer
can only INSERT (or overwrite whole tables); it cannot express
INSERT ... ON CONFLICT DO UPDATE. Upserts on natural keys are what make a
re-executed micro-batch harmless. Each micro-batch output is small and
bounded (patients x open windows), so collecting it to the driver is safe.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Sequence

import psycopg
from psycopg import sql

from ward_common.config import PostgresSettings


def connect(settings: PostgresSettings) -> psycopg.Connection:
    return psycopg.connect(**settings.connect_kwargs(), connect_timeout=10)


def _utc(value):
    # PySpark returns naive datetimes in the driver's local zone (UTC in the container).
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def upsert(
    conn: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    key: Sequence[str],
    rows: Iterable[dict],
    update_where: str | None = None,
    do_nothing: bool = False,
    touch: str | None = "updated_at",
) -> int:
    """INSERT ... ON CONFLICT (key) DO UPDATE SET <non-key columns>.

    `update_where` is an optional SQL guard (e.g. only move a pointer forward
    in time); `touch` names a timestamp column set to now() on update.
    Table/column names are code constants, quoted via psycopg.sql.
    """
    rows = list(rows)
    if not rows:
        return 0
    non_key = [c for c in columns if c not in key]
    if do_nothing or not non_key:
        conflict = sql.SQL("DO NOTHING")
    else:
        assignments = sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in non_key
        )
        if touch and touch not in columns:
            assignments = sql.SQL("{}, {} = now()").format(assignments, sql.Identifier(touch))
        conflict = sql.SQL("DO UPDATE SET {}").format(assignments)
        if update_where:
            conflict = sql.SQL("{} WHERE {}").format(conflict, sql.SQL(update_where))
    statement = sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) ON CONFLICT ({keys}) {conflict}").format(
        t=sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
        vals=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        keys=sql.SQL(", ").join(map(sql.Identifier, key)),
        conflict=conflict,
    )
    with conn.cursor() as cur:
        cur.executemany(statement, [tuple(_utc(r[c]) for c in columns) for r in rows])
    return len(rows)


def existing_ids(conn: psycopg.Connection, table: str, id_column: str, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    query = sql.SQL("SELECT {c} FROM {t} WHERE {c} = ANY(%s)").format(c=sql.Identifier(id_column), t=sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(query, (ids,))
        return {row[0] for row in cur.fetchall()}
