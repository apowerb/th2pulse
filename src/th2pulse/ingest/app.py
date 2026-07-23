"""FastAPI app: OTLP receiver endpoints + query API for the frontend."""
from __future__ import annotations

import asyncio
import gzip
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from datetime import timedelta, timezone

from fastapi import FastAPI, HTTPException, Query, Request

from th2pulse.ingest.parsing import min_severity_number, parse_logs, parse_traces
from th2pulse.ingest.store import Store

logger = logging.getLogger("th2pulse.ingest")

# OTLP/HTTP success response: empty partialSuccess = everything accepted.
_OTLP_OK: dict[str, Any] = {"partialSuccess": {}}

# Payload ceilings: the service buffers bodies in memory, and spans carry
# tool arguments/responses — an unbounded body is an OOM waiting to happen.
MAX_BODY_BYTES = 10 * 1024 * 1024
MAX_DECOMPRESSED_BYTES = 32 * 1024 * 1024

INGEST_TOKEN_HEADER = "x-th2pulse-token"


def create_app(store: Store | None = None) -> FastAPI:
    """Build the ingest app.

    Without an explicit ``store``, configuration comes from the environment:
    ``TH2PULSE_DB_DSN`` (required), ``TH2PULSE_DB_SCHEMA`` (optional) and
    ``TH2PULSE_INGEST_TOKEN`` (optional shared secret: when set, POST
    endpoints require a matching ``X-Th2Pulse-Token`` header — a cheap
    defense-in-depth on top of the localhost-only bind).
    """
    if store is None:
        dsn = os.environ.get("TH2PULSE_DB_DSN")
        if not dsn:
            raise RuntimeError("TH2PULSE_DB_DSN is required to run the ingest service")
        store = Store(dsn, schema=os.environ.get("TH2PULSE_DB_SCHEMA"))

    ingest_token = os.environ.get("TH2PULSE_INGEST_TOKEN") or None

    alert_interval = int(os.environ.get("TH2PULSE_ALERT_INTERVAL_S", "60"))
    alert_p95_ms = float(os.environ.get("TH2PULSE_ALERT_P95_MS", "10000"))
    alert_tokens_24h = int(os.environ.get("TH2PULSE_ALERT_TOKENS_24H", "2000000"))

    async def _alert_loop() -> None:
        # Sleep first: never race service startup, never tick in fast tests.
        while True:
            await asyncio.sleep(alert_interval)
            try:
                outcome = await store.evaluate_alerts(
                    p95_threshold_ms=alert_p95_ms,
                    tokens_24h_threshold=alert_tokens_24h,
                )
                if outcome.get("opened") or outcome.get("resolved"):
                    logger.info("alert evaluation: %s", outcome)
            except Exception:  # noqa: BLE001 - the evaluator must never die
                logger.exception("alert evaluation failed")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await store.connect()
        task = (
            asyncio.create_task(_alert_loop()) if alert_interval > 0 else None
        )
        try:
            yield
        finally:
            if task:
                task.cancel()
            await store.close()

    app = FastAPI(title="th2pulse ingest", lifespan=lifespan)

    def _check_ingest_token(request: Request) -> None:
        if ingest_token is None:
            return
        provided = request.headers.get(INGEST_TOKEN_HEADER, "")
        if not hmac.compare_digest(provided, ingest_token):
            raise HTTPException(401, detail="missing or invalid ingest token")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/logs")
    async def ingest_logs(request: Request) -> dict[str, Any]:
        _check_ingest_token(request)
        payload = await _json_body(request)
        rows, links = parse_logs(payload)
        await store.ingest_logs(rows, links)
        return _OTLP_OK

    @app.post("/v1/traces")
    async def ingest_traces(request: Request) -> dict[str, Any]:
        _check_ingest_token(request)
        payload = await _json_body(request)
        links, spans = parse_traces(payload)
        await store.ingest_traces(links, spans)
        return _OTLP_OK

    @app.post("/v1/metrics")
    async def ingest_metrics(request: Request) -> dict[str, Any]:
        # Accepted and dropped: metrics stay on the collector side for now.
        _check_ingest_token(request)
        await request.body()
        return _OTLP_OK

    @app.get("/logs")
    async def get_logs(
        conversation_id: str | None = None,
        service: str | None = None,
        level: str | None = None,
        since: datetime | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
        user_id: str | None = None,
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
            user_id=user_id,
        )
        return {"count": len(rows), "logs": rows}

    @app.get("/spans")
    async def get_spans(
        conversation_id: str | None = None,
        limit: int = Query(default=500, ge=1, le=2000),
        user_id: str | None = None,
    ) -> dict[str, Any]:
        rows = await store.query_spans(
            conversation_id=conversation_id, limit=limit, user_id=user_id,
        )
        return {"count": len(rows), "spans": rows}

    @app.get("/annotations")
    async def get_annotations(
        conversation_id: str,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        rows = await store.query_annotations(
            conversation_id=conversation_id, user_id=user_id,
        )
        return {"count": len(rows), "annotations": rows}

    @app.post("/annotations")
    async def post_annotation(
        request: Request,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        payload = await _json_body(request)
        conversation_id = payload.get("conversation_id")
        note = (payload.get("note") or "").strip()
        author = (payload.get("author") or "").strip()
        if not conversation_id or not author:
            raise HTTPException(422, detail="conversation_id and author are required")
        if not note or len(note) > 2000:
            raise HTTPException(422, detail="note must be 1..2000 characters")
        annotation_id = await store.insert_annotation(
            conversation_id=conversation_id,
            trace_id=payload.get("trace_id"),
            author=author,
            note=note,
            user_id=user_id,
        )
        if annotation_id is None:
            raise HTTPException(404, detail="conversation not found in caller scope")
        return {"id": annotation_id}

    @app.get("/alerts")
    async def get_alerts(
        active: bool = True,
        user_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, Any]:
        rows = await store.query_alerts(active=active, user_id=user_id, limit=limit)
        return {"count": len(rows), "alerts": rows}

    @app.get("/stats")
    async def get_stats(
        hours: int = Query(default=24, ge=1, le=720),
        user_id: str | None = None,
    ) -> dict[str, Any]:
        since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
        stats = await store.query_stats(since=since, user_id=user_id)
        return {"hours": hours, **stats}

    @app.get("/conversations")
    async def get_conversations(
        limit: int = Query(default=50, ge=1, le=500),
        user_id: str | None = None,
    ) -> dict[str, Any]:
        rows = await store.query_conversations(limit=limit, user_id=user_id)
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
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise HTTPException(413, detail="payload too large")
    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        raise HTTPException(413, detail="payload too large")
    # The otlphttp exporter gzips payloads by default; Starlette does not
    # transparently decompress request bodies.
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except OSError as exc:
            raise HTTPException(400, detail=f"invalid gzip body: {exc}") from exc
        if len(body) > MAX_DECOMPRESSED_BYTES:
            raise HTTPException(413, detail="decompressed payload too large")
    try:
        return json.loads(body)
    except ValueError as exc:
        raise HTTPException(400, detail=f"invalid JSON body: {exc}") from exc
