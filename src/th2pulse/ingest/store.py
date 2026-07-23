"""PostgreSQL persistence for ingested telemetry (asyncpg)."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

from th2pulse.ingest.parsing import ConversationLink, LogRow, SpanRow

_DDL = """
CREATE TABLE IF NOT EXISTS pulse_logs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    severity_num SMALLINT,
    severity TEXT,
    service TEXT,
    trace_id TEXT,
    span_id TEXT,
    body TEXT NOT NULL DEFAULT '',
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    resource JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_pulse_logs_trace ON pulse_logs (trace_id);
CREATE INDEX IF NOT EXISTS idx_pulse_logs_ts ON pulse_logs (ts DESC);
CREATE TABLE IF NOT EXISTS pulse_conversation_map (
    conversation_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    user_id TEXT,
    service TEXT,
    first_seen TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (conversation_id, trace_id)
);
CREATE INDEX IF NOT EXISTS idx_pulse_conv_trace ON pulse_conversation_map (trace_id);
ALTER TABLE pulse_logs ADD COLUMN IF NOT EXISTS event_name TEXT;
CREATE TABLE IF NOT EXISTS pulse_spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    ts TIMESTAMPTZ NOT NULL,
    duration_ms DOUBLE PRECISION,
    name TEXT NOT NULL DEFAULT '',
    service TEXT,
    status_code TEXT,
    status_message TEXT,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (trace_id, span_id)
);
CREATE INDEX IF NOT EXISTS idx_pulse_spans_ts ON pulse_spans (ts DESC);
CREATE TABLE IF NOT EXISTS pulse_annotations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    trace_id TEXT,
    author TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pulse_annotations_conv
    ON pulse_annotations (conversation_id);
"""

_INSERT_ANNOTATION = """
INSERT INTO pulse_annotations (conversation_id, trace_id, author, note)
SELECT $1, $2, $3, $4
WHERE $5::text IS NULL OR EXISTS (
    SELECT 1 FROM pulse_conversation_map
    WHERE conversation_id = $1 AND user_id = $5)
RETURNING id
"""

_QUERY_ANNOTATIONS = """
SELECT id, conversation_id, trace_id, author, note, created_at
FROM pulse_annotations
WHERE conversation_id = $1
  AND ($2::text IS NULL OR EXISTS (
      SELECT 1 FROM pulse_conversation_map
      WHERE conversation_id = $1 AND user_id = $2))
ORDER BY created_at
"""

_INSERT_LOG = """
INSERT INTO pulse_logs (ts, severity_num, severity, service, trace_id, span_id,
                        body, attributes, resource, event_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)
"""

_INSERT_SPAN = """
INSERT INTO pulse_spans (trace_id, span_id, parent_span_id, ts, duration_ms,
                         name, service, status_code, status_message, attributes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
ON CONFLICT (trace_id, span_id) DO NOTHING
"""

_QUERY_SPANS = """
SELECT ts, duration_ms, name, service, trace_id, span_id, parent_span_id,
       status_code, status_message, attributes
FROM pulse_spans
WHERE ($1::text IS NULL OR trace_id IN (
          SELECT trace_id FROM pulse_conversation_map WHERE conversation_id = $1))
  AND ($3::text IS NULL OR trace_id IN (
          SELECT trace_id FROM pulse_conversation_map WHERE user_id = $3))
ORDER BY ts DESC
LIMIT $2
"""

_UPSERT_LINK = """
INSERT INTO pulse_conversation_map (conversation_id, trace_id, user_id, service, first_seen)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (conversation_id, trace_id) DO UPDATE
SET user_id = COALESCE(pulse_conversation_map.user_id, EXCLUDED.user_id),
    first_seen = LEAST(pulse_conversation_map.first_seen, EXCLUDED.first_seen)
"""

_QUERY_LOGS = """
SELECT ts, severity, severity_num, service, trace_id, span_id, body, attributes,
       event_name
FROM pulse_logs
WHERE ($1::text IS NULL OR trace_id IN (
          SELECT trace_id FROM pulse_conversation_map WHERE conversation_id = $1))
  AND ($2::text IS NULL OR service = $2)
  AND ($3::smallint IS NULL OR severity_num >= $3)
  AND ($4::timestamptz IS NULL OR ts >= $4)
  AND ($6::text IS NULL OR trace_id IN (
          SELECT trace_id FROM pulse_conversation_map WHERE user_id = $6))
ORDER BY ts DESC
LIMIT $5
"""

_QUERY_STATS = """
WITH scoped AS (
    SELECT m.trace_id, m.conversation_id
    FROM pulse_conversation_map m
    WHERE ($1::text IS NULL OR m.user_id = $1)
      AND m.first_seen >= $2
)
SELECT
    count(DISTINCT sc.conversation_id) AS conversations,
    count(DISTINCT sc.trace_id) AS turns,
    avg(s.duration_ms) FILTER (WHERE s.name = 'invocation') AS avg_turn_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY s.duration_ms)
        FILTER (WHERE s.name = 'invocation') AS p95_turn_ms,
    count(s.span_id) FILTER (WHERE s.name LIKE 'execute_tool%') AS tool_calls,
    coalesce(sum((s.attributes ->> 'gen_ai.usage.input_tokens')::numeric), 0)
        AS input_tokens,
    coalesce(sum((s.attributes ->> 'gen_ai.usage.output_tokens')::numeric), 0)
        AS output_tokens
FROM scoped sc
LEFT JOIN pulse_spans s ON s.trace_id = sc.trace_id
"""

_QUERY_APP_ERRORS = """
SELECT count(*) AS app_errors
FROM pulse_logs
WHERE severity_num >= 17
  AND ts >= $2
  AND ($1::text IS NULL OR trace_id IN (
          SELECT trace_id FROM pulse_conversation_map WHERE user_id = $1))
"""

_QUERY_CONVERSATIONS = """
SELECT conversation_id,
       min(first_seen) AS first_seen,
       max(user_id) AS user_id,
       count(*) AS trace_count,
       array_agg(trace_id ORDER BY first_seen) AS trace_ids
FROM pulse_conversation_map
WHERE ($2::text IS NULL OR user_id = $2)
GROUP BY conversation_id
ORDER BY min(first_seen) DESC
LIMIT $1
"""


class Store:
    """Thin asyncpg wrapper. ``schema`` scopes every table via search_path."""

    def __init__(self, dsn: str, schema: str | None = None) -> None:
        self._dsn = dsn
        self._schema = schema
        self._pool: asyncpg.Pool | None = None

    # Arbitrary but stable key so concurrent instances (rolling restart)
    # serialize DDL execution — CREATE TABLE IF NOT EXISTS is not atomic
    # across concurrent transactions.
    _DDL_LOCK_KEY = 0x7482_5055_4C53_4531

    async def connect(self) -> None:
        server_settings = {"search_path": self._schema} if self._schema else None
        self._pool = await asyncpg.create_pool(
            self._dsn, min_size=1, max_size=4, server_settings=server_settings,
        )
        async with self._pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock($1)", self._DDL_LOCK_KEY)
            try:
                await conn.execute(_DDL)
            finally:
                await conn.execute("SELECT pg_advisory_unlock($1)", self._DDL_LOCK_KEY)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @staticmethod
    def _log_args(rows: list[LogRow]) -> list[tuple]:
        return [
            (r.ts, r.severity_num, r.severity, r.service, r.trace_id, r.span_id,
             r.body, json.dumps(r.attributes, default=str),
             json.dumps(r.resource, default=str), r.event_name)
            for r in rows
        ]

    @staticmethod
    def _span_args(spans: list[SpanRow]) -> list[tuple]:
        return [
            (s.trace_id, s.span_id, s.parent_span_id, s.ts, s.duration_ms,
             s.name, s.service, s.status_code, s.status_message,
             json.dumps(s.attributes, default=str))
            for s in spans
        ]

    @staticmethod
    def _link_args(links: list[ConversationLink]) -> list[tuple]:
        return [
            (link.conversation_id, link.trace_id, link.user_id, link.service,
             link.first_seen)
            for link in links
        ]

    async def ingest_logs(
        self, rows: list[LogRow], links: list[ConversationLink],
    ) -> int:
        """Persist a logs payload atomically.

        One transaction for rows + links: a failure rolls everything back,
        so the collector's retry (otlphttp retries 5xx) cannot duplicate
        already-committed rows.
        """
        if not rows and not links:
            return 0
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if rows:
                    await conn.executemany(_INSERT_LOG, self._log_args(rows))
                if links:
                    await conn.executemany(_UPSERT_LINK, self._link_args(links))
        return len(rows)

    async def ingest_traces(
        self, links: list[ConversationLink], spans: list[SpanRow],
    ) -> int:
        """Persist a traces payload atomically (same rationale as ingest_logs)."""
        if not links and not spans:
            return 0
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if links:
                    await conn.executemany(_UPSERT_LINK, self._link_args(links))
                if spans:
                    await conn.executemany(_INSERT_SPAN, self._span_args(spans))
        return len(spans)

    async def query_logs(
        self,
        conversation_id: str | None = None,
        service: str | None = None,
        min_severity: int | None = None,
        since: datetime | None = None,
        limit: int = 100,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(
                _QUERY_LOGS, conversation_id, service, min_severity, since,
                limit, user_id,
            )
        return [
            {**dict(r), "attributes": json.loads(r["attributes"])}
            for r in records
        ]

    async def query_spans(
        self,
        conversation_id: str | None = None,
        limit: int = 500,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(_QUERY_SPANS, conversation_id, limit, user_id)
        return [
            {**dict(r), "attributes": json.loads(r["attributes"])}
            for r in records
        ]

    async def query_conversations(
        self, limit: int = 50, user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(_QUERY_CONVERSATIONS, limit, user_id)
        return [dict(r) for r in records]

    async def insert_annotation(
        self,
        conversation_id: str,
        trace_id: str | None,
        author: str,
        note: str,
        user_id: str | None = None,
    ) -> int | None:
        """Insert a human note; scoped writers may only annotate their own
        conversations (returns None when the scope check rejects)."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                _INSERT_ANNOTATION, conversation_id, trace_id, author, note, user_id,
            )
        return row["id"] if row else None

    async def query_annotations(
        self, conversation_id: str, user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(_QUERY_ANNOTATIONS, conversation_id, user_id)
        return [dict(r) for r in records]

    async def query_stats(
        self, since: datetime, user_id: str | None = None,
    ) -> dict[str, Any]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            stats = await conn.fetchrow(_QUERY_STATS, user_id, since)
            errors = await conn.fetchrow(_QUERY_APP_ERRORS, user_id, since)
        out = dict(stats)
        out["app_errors"] = errors["app_errors"]
        for key in ("avg_turn_ms", "p95_turn_ms"):
            out[key] = float(out[key]) if out[key] is not None else None
        for key in ("input_tokens", "output_tokens"):
            out[key] = int(out[key])
        return out
