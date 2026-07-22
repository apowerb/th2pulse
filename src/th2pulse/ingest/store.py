"""PostgreSQL persistence for ingested telemetry (asyncpg)."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

from th2pulse.ingest.parsing import ConversationLink, LogRow

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
"""

_INSERT_LOG = """
INSERT INTO pulse_logs (ts, severity_num, severity, service, trace_id, span_id,
                        body, attributes, resource)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb)
"""

_UPSERT_LINK = """
INSERT INTO pulse_conversation_map (conversation_id, trace_id, user_id, service, first_seen)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (conversation_id, trace_id) DO UPDATE
SET user_id = COALESCE(pulse_conversation_map.user_id, EXCLUDED.user_id),
    first_seen = LEAST(pulse_conversation_map.first_seen, EXCLUDED.first_seen)
"""

_QUERY_LOGS = """
SELECT ts, severity, severity_num, service, trace_id, span_id, body, attributes
FROM pulse_logs
WHERE ($1::text IS NULL OR trace_id IN (
          SELECT trace_id FROM pulse_conversation_map WHERE conversation_id = $1))
  AND ($2::text IS NULL OR service = $2)
  AND ($3::smallint IS NULL OR severity_num >= $3)
  AND ($4::timestamptz IS NULL OR ts >= $4)
ORDER BY ts DESC
LIMIT $5
"""

_QUERY_CONVERSATIONS = """
SELECT conversation_id,
       min(first_seen) AS first_seen,
       max(user_id) AS user_id,
       count(*) AS trace_count,
       array_agg(trace_id ORDER BY first_seen) AS trace_ids
FROM pulse_conversation_map
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

    async def connect(self) -> None:
        server_settings = {"search_path": self._schema} if self._schema else None
        self._pool = await asyncpg.create_pool(
            self._dsn, min_size=1, max_size=4, server_settings=server_settings,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(_DDL)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def insert_logs(self, rows: list[LogRow]) -> int:
        if not rows:
            return 0
        assert self._pool is not None
        args = [
            (r.ts, r.severity_num, r.severity, r.service, r.trace_id, r.span_id,
             r.body, json.dumps(r.attributes, default=str),
             json.dumps(r.resource, default=str))
            for r in rows
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(_INSERT_LOG, args)
        return len(rows)

    async def upsert_links(self, links: list[ConversationLink]) -> int:
        if not links:
            return 0
        assert self._pool is not None
        args = [
            (l.conversation_id, l.trace_id, l.user_id, l.service, l.first_seen)
            for l in links
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(_UPSERT_LINK, args)
        return len(links)

    async def query_logs(
        self,
        conversation_id: str | None = None,
        service: str | None = None,
        min_severity: int | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(
                _QUERY_LOGS, conversation_id, service, min_severity, since, limit,
            )
        return [
            {**dict(r), "attributes": json.loads(r["attributes"])}
            for r in records
        ]

    async def query_conversations(self, limit: int = 50) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(_QUERY_CONVERSATIONS, limit)
        return [dict(r) for r in records]
