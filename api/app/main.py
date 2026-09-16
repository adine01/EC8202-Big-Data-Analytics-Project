"""Ward monitoring API.

    /ward/summary, /patients[...], /alerts, /reports[...]  (see /docs)
    /health   liveness + dependency checks (503 if Postgres or the sim clock is unavailable)
    /metrics  Prometheus: HTTP metrics + pipeline/batch/storage state read at scrape time

ILLUSTRATIVE ONLY - synthetic data and a NEWS2-style score, not a clinical tool.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ward_common.log import configure_logging

from app import models
from app.metrics import ApiMetrics
from app.repository import Repository
from app.routers import router

configure_logging("api")
log = logging.getLogger("app")


def _production_dependencies():
    """Connection pool + shared simulated clock (only when not injected by tests)."""
    import psycopg
    from psycopg_pool import ConnectionPool

    from ward_common.config import ClockSettings, PostgresSettings
    from ward_common.sim_clock import ClockReader

    pg = PostgresSettings.from_env()
    # A small pool: the API is read-mostly and each request runs one or two short queries.
    pool = ConnectionPool(kwargs={**pg.connect_kwargs(), "connect_timeout": 3}, min_size=1, max_size=5,
                          timeout=5, open=False)
    pool.open(wait=False)
    reader = ClockReader(lambda: psycopg.connect(**pg.connect_kwargs(), connect_timeout=3),
                         ClockSettings.from_env().refresh_seconds)
    return Repository(pool), reader.now, pool.close


def create_app(repository: Repository | None = None, sim_now: Callable[[], datetime] | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        close = None
        if repository is None:
            app.state.repository, app.state.sim_now, close = _production_dependencies()
        else:
            app.state.repository, app.state.sim_now = repository, sim_now
        app.state.metrics = ApiMetrics(app.state.repository, app.state.sim_now)
        log.info("api_started")
        yield
        if close:
            close()

    app = FastAPI(
        title="Ward Vitals Monitoring API",
        version="1.0.0",
        description=models.DISCLAIMER + " All timestamps are simulated time unless stated otherwise.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def http_metrics(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        # Route template (e.g. /patients/{patient_id}), not the raw path, keeps label cardinality bounded.
        path = getattr(route, "path", "unmatched")
        metrics = getattr(request.app.state, "metrics", None)
        if metrics is not None and path != "/metrics":
            metrics.requests.labels(request.method, path, str(response.status_code)).inc()
            metrics.latency.labels(path).observe(time.perf_counter() - started)
        return response

    @app.get("/health", response_model=models.Health, tags=["system"])
    def health(request: Request, response: Response):
        """Liveness plus dependency checks; 503 if any dependency is down."""
        checks: dict[str, str] = {}
        sim_now = None
        try:
            request.app.state.repository.ping()
            checks["database"] = "ok"
        except Exception as exc:
            checks["database"] = f"error: {type(exc).__name__}"
        try:
            sim_now = request.app.state.sim_now()
            checks["sim_clock"] = "ok"
        except Exception as exc:
            checks["sim_clock"] = f"error: {type(exc).__name__}"
        healthy = all(v == "ok" for v in checks.values())
        if not healthy:
            response.status_code = 503
        return {"status": "ok" if healthy else "degraded", "checks": checks, "sim_now": sim_now}

    @app.get("/metrics", include_in_schema=False)
    def metrics(request: Request) -> Response:
        return Response(generate_latest(request.app.state.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    app.include_router(router)
    return app


app = create_app()
