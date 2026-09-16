"""Dependencies injected into routers (overridden in tests)."""

from __future__ import annotations

from datetime import datetime

from fastapi import Request

from app.repository import Repository


def get_repository(request: Request) -> Repository:
    return request.app.state.repository


def get_sim_now(request: Request) -> datetime:
    return request.app.state.sim_now()
