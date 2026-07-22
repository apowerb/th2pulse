"""FastAPI app: OTLP receiver endpoints + query API for the frontend."""
from __future__ import annotations

import gzip
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request

from th2pulse.ingest.parsing import min_severity_number, parse_logs, parse_traces
from th2pulse.ingest.store import Store

logger = logging.getLogger("th2pulse.ingest")

# OTLP/HTTP success response: empty partialSuccess = everything accepted.
_OTLP_OK: dict[str, Any] = {"partialSuccess": {}}


def create_app(store: Store | None = None) -> FastAPI:
    """Build the ingest app.

    Without an explicit ``store``, configuration comes from the environment:
    ``TH2PULSE_DB_DSN`` (required) and ``TH2PULSE_DB_SCHEMA`` (optional).
    """
    if store is None:
        dsn = os.environ.get("TH2PULSE_DB_DSN")
        if not dsn:
            raise RuntimeError("TH2PULSE_DB_DSN is required to run the ingest service")
        store = Store(dsn, schema=os.environ.get("TH2PULSE_DB_SCHEMA"))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await store.connect()
        try:
            yield
        finally:
            await store.close()

    app = FastAPI(title="th2pulse ingest", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/logs")
    async def ingest_logs(request: Request) -> dict[str, Any]:
        payload = await _json_body(request)
        rows, links = parse_logs(payload)
        await store.insert_logs(rows)
        await store.upsert_links(links)
        return _OTLP_OK

    @app.post("/v1/traces")
    async def ingest_traces(request: Request) -> dict[str, Any]:
        payload = await _json_body(request)
        links = parse_traces(payload)
        await store.upsert_links(links)
        return _OTLP_OK

    @app.post("/v1/metrics")
    async def ingest_metrics(request: Request) -> dict[str, Any]:
        # Accepted and dropped: metrics stay on the collector side for now.
        await request.body()
        return _OTLP_OK

    @app.get("/logs")
    async def get_logs(
        conversation_id: str | None = None,
        service: str | None = None,
        level: str | None = None,
        since: datetime | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        min_severity = None
        if level is not None:
            min_severity = min_severity_number(level)
            if min_severity is None:
                raise HTTPException(422, detail=f"unknown level: {level!r}")
        rows = await store.query_logs(
            conversation_id=conversation_id,
            service=service,
            min_severity=min_severity,
            since=since,
            limit=limit,
        )
        return {"count": len(rows), "logs": rows}

    @app.get("/conversations")
    async def get_conversations(
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        rows = await store.query_conversations(limit=limit)
        return {"count": len(rows), "conversations": rows}

    return app


async def _json_body(request: Request) -> dict[str, Any]:
    content_type = request.headers.get("content-type", "")
    if "json" not in content_type:
        # The collector's otlphttp exporter must be configured with
        # `encoding: json`; the default protobuf encoding is not supported.
        raise HTTPException(
            415,
            detail="expected application/json — set `encoding: json` "
                   "on the collector's otlphttp exporter",
        )
    body = await request.body()
    # The otlphttp exporter gzips payloads by default; Starlette does not
    # transparently decompress request bodies.
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except OSError as exc:
            raise HTTPException(400, detail=f"invalid gzip body: {exc}") from exc
    try:
        return json.loads(body)
    except ValueError as exc:
        raise HTTPException(400, detail=f"invalid JSON body: {exc}") from exc
