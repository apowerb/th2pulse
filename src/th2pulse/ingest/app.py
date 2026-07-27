"""FastAPI app: OTLP receiver endpoints + query API for the frontend."""
from __future__ import annotations

import asyncio
import gzip
import hmac
import io
import json
import logging
import os
import zlib
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from datetime import timedelta, timezone

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from th2pulse.ingest.parsing import min_severity_number, parse_logs, parse_traces
from th2pulse.ingest.store import Store
from th2pulse.masking import mask_log_rows, mask_span_rows, masking_enabled

logger = logging.getLogger("th2pulse.ingest")

# OTLP/HTTP success response: empty partialSuccess = everything accepted.
_OTLP_OK: dict[str, Any] = {"partialSuccess": {}}

# Payload ceilings: the service buffers bodies in memory, and spans carry
# tool arguments/responses — an unbounded body is an OOM waiting to happen.
MAX_BODY_BYTES = 10 * 1024 * 1024
MAX_DECOMPRESSED_BYTES = 32 * 1024 * 1024
# Slice size for incremental gunzip: caps how much a single decompress call
# can produce, so the ceiling above is enforced while inflating, not after.
_GUNZIP_CHUNK = 1024 * 1024

INGEST_TOKEN_HEADER = "x-th2pulse-token"
QUERY_TOKEN_HEADER = "x-th2pulse-query-token"


def create_app(store: Store | None = None) -> FastAPI:
    """Build the ingest app.

    Without an explicit ``store``, configuration comes from the environment:
    ``TH2PULSE_DB_DSN`` (required) and ``TH2PULSE_DB_SCHEMA`` (optional).

    Two independent shared secrets guard the two roles, so the collector and
    the frontend can be rolled out separately:

    ``TH2PULSE_INGEST_TOKEN``
        write side — the OTLP ``POST /v1/*`` endpoints require a matching
        ``X-Th2Pulse-Token`` header.
    ``TH2PULSE_QUERY_TOKEN``
        read side — every query endpoint (and ``POST /annotations``) requires
        a matching ``X-Th2Pulse-Query-Token`` header.

    **Trust model.** ``user_id`` is *authorization*, not authentication: this
    service trusts its single caller (the frontend proxy) to derive it from a
    verified identity, and an absent ``user_id`` deliberately means "no
    scoping" so an admin view can span every user. That is only sound while
    the caller is authenticated — hence the query token. Leaving either token
    unset keeps the endpoints open to anything that can reach the socket, so
    the service logs a warning at startup and relies solely on the
    localhost-only bind.
    """
    if store is None:
        dsn = os.environ.get("TH2PULSE_DB_DSN")
        if not dsn:
            raise RuntimeError("TH2PULSE_DB_DSN is required to run the ingest service")
        store = Store(dsn, schema=os.environ.get("TH2PULSE_DB_SCHEMA"))

    ingest_token = os.environ.get("TH2PULSE_INGEST_TOKEN") or None
    query_token = os.environ.get("TH2PULSE_QUERY_TOKEN") or None
    for name, value in (("TH2PULSE_INGEST_TOKEN", ingest_token),
                        ("TH2PULSE_QUERY_TOKEN", query_token)):
        if value is None:
            logger.warning(
                "%s is not set: the matching endpoints accept any caller that "
                "can reach the socket — the localhost-only bind is the sole "
                "protection", name,
            )

    alert_interval = int(os.environ.get("TH2PULSE_ALERT_INTERVAL_S", "60"))
    alert_p95_ms = float(os.environ.get("TH2PULSE_ALERT_P95_MS", "10000"))
    alert_tokens_24h = int(os.environ.get("TH2PULSE_ALERT_TOKENS_24H", "2000000"))

    retention_interval = int(os.environ.get("TH2PULSE_RETENTION_INTERVAL_S", "3600"))
    retention_days = int(os.environ.get("TH2PULSE_RETENTION_DAYS", "15"))

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

    async def _retention_loop() -> None:
        # Sleep first: never race service startup, never tick in fast tests.
        while True:
            await asyncio.sleep(retention_interval)
            try:
                deleted = await store.prune_old(retention_days)
                total = sum(deleted.values())
                if total:
                    logger.info(
                        "retention pruned %d row(s) older than %d days: %s",
                        total, retention_days, deleted,
                    )
            except Exception:  # noqa: BLE001 - retention must never die
                logger.exception("retention prune failed")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await store.connect()
        tasks: list[asyncio.Task] = []
        if alert_interval > 0:
            tasks.append(asyncio.create_task(_alert_loop()))
        if retention_interval > 0:
            tasks.append(asyncio.create_task(_retention_loop()))
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await store.close()

    app = FastAPI(title="th2pulse ingest", lifespan=lifespan)

    def _token_matches(provided: str, expected: str) -> bool:
        """Constant-time compare over the bytes that were actually sent.

        ``hmac.compare_digest`` raises TypeError on str holding non-ASCII,
        and a raw 0xE9 byte is legal in an HTTP header — a bogus token
        answered 500 instead of 401. Round-tripping through **latin-1** is
        what recovers the wire bytes, because that is how Starlette decodes
        header values; encoding as UTF-8 instead produced different bytes
        and rejected a correct non-ASCII secret every single time.
        """
        try:
            wanted = expected.encode("utf-8")
        except UnicodeEncodeError:
            # A secret carrying bytes os.environ could not decode: refuse
            # rather than answer 500 on every authenticated request.
            return False
        return hmac.compare_digest(provided.encode("latin-1", "replace"), wanted)

    def _check_ingest_token(request: Request) -> None:
        if ingest_token is None:
            return
        provided = request.headers.get(INGEST_TOKEN_HEADER, "")
        if not _token_matches(provided, ingest_token):
            raise HTTPException(401, detail="missing or invalid ingest token")

    def _check_query_token(request: Request) -> None:
        """Authenticate the read side.

        Without this, ``user_id`` alone decides what a caller sees — and
        omitting it returns every user's telemetry. The token is what makes
        "the caller is the trusted proxy" an enforced claim rather than an
        assumption.
        """
        if query_token is None:
            return
        provided = request.headers.get(QUERY_TOKEN_HEADER, "")
        if not _token_matches(provided, query_token):
            raise HTTPException(401, detail="missing or invalid query token")

    # Applied as a route dependency rather than a parameter on each handler:
    # a new endpoint added later cannot silently skip the check by forgetting
    # to call it, it can only skip it by explicitly omitting the guard.
    query_guard = [Depends(_check_query_token)]

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/logs")
    async def ingest_logs(request: Request) -> dict[str, Any]:
        _check_ingest_token(request)
        payload = await _json_body(request)
        rows, links = parse_logs(payload)
        if masking_enabled():
            rows = mask_log_rows(rows)
        await store.ingest_logs(rows, links)
        return _OTLP_OK

    @app.post("/v1/traces")
    async def ingest_traces(request: Request) -> dict[str, Any]:
        _check_ingest_token(request)
        payload = await _json_body(request)
        links, spans = parse_traces(payload)
        if masking_enabled():
            spans = mask_span_rows(spans)
        await store.ingest_traces(links, spans)
        return _OTLP_OK

    @app.post("/v1/metrics")
    async def ingest_metrics(request: Request) -> dict[str, Any]:
        # Accepted and dropped: metrics stay on the collector side for now.
        _check_ingest_token(request)
        await request.body()
        return _OTLP_OK

    @app.get("/logs", dependencies=query_guard)
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

    @app.get("/spans", dependencies=query_guard)
    async def get_spans(
        conversation_id: str | None = None,
        limit: int = Query(default=500, ge=1, le=2000),
        user_id: str | None = None,
    ) -> dict[str, Any]:
        rows = await store.query_spans(
            conversation_id=conversation_id, limit=limit, user_id=user_id,
        )
        return {"count": len(rows), "spans": rows}

    @app.get("/annotations", dependencies=query_guard)
    async def get_annotations(
        conversation_id: str,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        rows = await store.query_annotations(
            conversation_id=conversation_id, user_id=user_id,
        )
        return {"count": len(rows), "annotations": rows}

    @app.post("/annotations", dependencies=query_guard)
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

    @app.get("/alerts", dependencies=query_guard)
    async def get_alerts(
        active: bool = True,
        user_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, Any]:
        rows = await store.query_alerts(active=active, user_id=user_id, limit=limit)
        return {"count": len(rows), "alerts": rows}

    @app.get("/stats", dependencies=query_guard)
    async def get_stats(
        hours: int = Query(default=24, ge=1, le=720),
        user_id: str | None = None,
    ) -> dict[str, Any]:
        since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
        stats = await store.query_stats(since=since, user_id=user_id)
        return {"hours": hours, **stats}

    @app.get("/conversations", dependencies=query_guard)
    async def get_conversations(
        limit: int = Query(default=50, ge=1, le=500),
        user_id: str | None = None,
    ) -> dict[str, Any]:
        rows = await store.query_conversations(limit=limit, user_id=user_id)
        return {"count": len(rows), "conversations": rows}

    return app


def _gunzip_bounded(body: bytes) -> bytes:
    """Decompress gzip incrementally, aborting past MAX_DECOMPRESSED_BYTES.

    ``gzip.decompress`` would materialise the whole stream before anything
    could check its size: a few hundred KB of repetitive input expands to
    gigabytes (ratios above 1000:1 are trivial to craft), which is an OOM
    waiting to happen. Feeding a decompressobj in slices lets us stop the
    moment the running total crosses the ceiling, so peak memory stays
    bounded by the ceiling itself rather than by the attacker's payload.
    """
    # Hand-rolling this over zlib.decompressobj went wrong three times in a
    # row — silent truncation, then an infinite loop on multi-member streams,
    # then quadratic cost in the number of members (300k empty members in a
    # 6 MB body froze the event loop for 15s). GzipFile already handles
    # member boundaries, headers and framing, and reading from a BytesIO
    # streams through the buffer instead of recopying the remainder each
    # time. Bounding is then just a capped read: ask for one byte more than
    # the ceiling and reject if we get it.
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            out = stream.read(MAX_DECOMPRESSED_BYTES + 1)
    except (OSError, EOFError, zlib.error) as exc:
        # OSError covers gzip's own "Not a gzipped file" / bad CRC.
        raise HTTPException(400, detail=f"invalid gzip body: {exc}") from exc
    if len(out) > MAX_DECOMPRESSED_BYTES:
        raise HTTPException(413, detail="decompressed payload too large")
    return out


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
    #
    # Both the inflate and the JSON parse are CPU-bound and run on a single
    # worker: called inline they block the event loop, so one large body
    # stalls every other request — health checks included — for its whole
    # duration. Off to a worker thread they go.
    if request.headers.get("content-encoding", "").lower() == "gzip":
        body = await run_in_threadpool(_gunzip_bounded, body)
    try:
        return await run_in_threadpool(json.loads, body)
    except ValueError as exc:
        raise HTTPException(400, detail=f"invalid JSON body: {exc}") from exc
