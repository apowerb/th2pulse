"""PostgreSQL persistence for ingested telemetry (asyncpg)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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
-- Retention prunes on last activity, not on creation: a conversation opened
-- weeks ago but still in use must keep its mapping, otherwise today's rows
-- survive in pulse_logs while becoming invisible to any user-scoped query.
-- Backfilled from first_seen so existing rows migrate without a data pass.
ALTER TABLE pulse_conversation_map
    ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ;
UPDATE pulse_conversation_map SET last_seen = first_seen WHERE last_seen IS NULL;
-- Five query paths filter or join on user_id; without this it is a seq scan
-- that degrades as telemetry accumulates.
CREATE INDEX IF NOT EXISTS idx_pulse_conv_user ON pulse_conversation_map (user_id);
CREATE INDEX IF NOT EXISTS idx_pulse_conv_last_seen
    ON pulse_conversation_map (last_seen);
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
ALTER TABLE pulse_spans ADD COLUMN IF NOT EXISTS
    business_error BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS pulse_alerts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule_key TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT 'global',
    severity TEXT NOT NULL DEFAULT 'warning',
    message TEXT NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pulse_alerts_open
    ON pulse_alerts (rule_key, target) WHERE resolved_at IS NULL;
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
                         name, service, status_code, status_message, attributes,
                         business_error)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
ON CONFLICT (trace_id, span_id) DO NOTHING
"""

# ── Alerting: each rule yields (target, message) rows for conditions that
# are CURRENTLY true over the sliding window. Stateless evaluation:
# condition true + no open alert → open ; condition false + open → resolve.

_RULE_TOOL_FAILURE = """
SELECT COALESCE(m.conversation_id, 'global') AS target,
       count(*) AS n
FROM pulse_spans s
LEFT JOIN pulse_conversation_map m ON m.trace_id = s.trace_id
WHERE s.business_error AND s.ts >= $1
GROUP BY 1
"""

_RULE_APP_ERROR = """
SELECT COALESCE(m.conversation_id, 'global') AS target,
       count(*) AS n
FROM pulse_logs l
LEFT JOIN pulse_conversation_map m ON m.trace_id = l.trace_id
WHERE l.severity_num >= 17 AND l.ts >= $1
GROUP BY 1
"""

# An OTLP error status (LLM API/auth failure, tool crash) propagates up the
# whole span tree, so we count only the leaf spans that originate a failure
# (generate_content / execute_tool) — otherwise one error inflates to four.
_RULE_SPAN_ERROR = """
SELECT COALESCE(m.conversation_id, 'global') AS target,
       count(*) AS n
FROM pulse_spans s
LEFT JOIN pulse_conversation_map m ON m.trace_id = s.trace_id
WHERE s.status_code IN ('2', 'STATUS_CODE_ERROR') AND s.ts >= $1
  AND (s.name LIKE 'generate_content%' OR s.name LIKE 'execute_tool%')
GROUP BY 1
"""

_RULE_P95 = """
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95
FROM pulse_spans
WHERE name = 'invocation' AND ts >= $1
"""

_RULE_TOKENS = """
SELECT coalesce(sum((attributes ->> 'gen_ai.usage.input_tokens')::numeric), 0)
       AS tokens
FROM pulse_spans
WHERE ts >= $1
"""

_OPEN_ALERT = """
INSERT INTO pulse_alerts (rule_key, target, severity, message)
VALUES ($1, $2, $3, $4)
ON CONFLICT (rule_key, target) WHERE resolved_at IS NULL DO NOTHING
"""

_RESOLVE_STALE = """
UPDATE pulse_alerts
SET resolved_at = now()
WHERE rule_key = $1 AND resolved_at IS NULL AND NOT (target = ANY($2::text[]))
"""

_QUERY_ALERTS = """
SELECT id, rule_key, target, severity, message, triggered_at, resolved_at
FROM pulse_alerts
WHERE ($1::bool IS DISTINCT FROM TRUE OR resolved_at IS NULL)
  AND ($2::text IS NULL OR (target <> 'global' AND EXISTS (
      SELECT 1 FROM pulse_conversation_map
      WHERE conversation_id = target AND user_id = $2)))
ORDER BY triggered_at DESC
LIMIT $3
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
INSERT INTO pulse_conversation_map
    (conversation_id, trace_id, user_id, service, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $5, $5)
ON CONFLICT (conversation_id, trace_id) DO UPDATE
SET user_id = COALESCE(pulse_conversation_map.user_id, EXCLUDED.user_id),
    first_seen = LEAST(pulse_conversation_map.first_seen, EXCLUDED.first_seen),
    last_seen = GREATEST(
        COALESCE(pulse_conversation_map.last_seen, pulse_conversation_map.first_seen),
        EXCLUDED.last_seen)
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
    -- ADK emits usage on BOTH the call_llm span and its generate_content
    -- child, so summing across all spans double-counts. Count each model
    -- call once via call_llm — where the cached-token mirror also lands,
    -- keeping input and cached on the same rows.
    coalesce(sum((s.attributes ->> 'gen_ai.usage.input_tokens')::numeric)
        FILTER (WHERE s.name LIKE 'call_llm%'), 0) AS input_tokens,
    coalesce(sum((s.attributes ->> 'gen_ai.usage.output_tokens')::numeric)
        FILTER (WHERE s.name LIKE 'call_llm%'), 0) AS output_tokens,
    coalesce(sum((s.attributes ->> 'gen_ai.usage.cached_input_tokens')::numeric)
        FILTER (WHERE s.name LIKE 'call_llm%'), 0) AS cached_tokens
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

# ── Retention: prune telemetry older than N days by its event timestamp.
# Alerts are the exception — only *resolved* ones are pruned; an open alert
# is kept regardless of age so a standing incident never silently vanishes.
_PRUNE_LOGS = "DELETE FROM pulse_logs WHERE ts < now() - make_interval(days => $1)"
_PRUNE_SPANS = "DELETE FROM pulse_spans WHERE ts < now() - make_interval(days => $1)"
# Prune on last activity: pruning on first_seen orphaned long-running
# conversations — their recent logs stayed in the table but no user-scoped
# query could reach them any more. COALESCE covers rows written before
# last_seen existed.
_PRUNE_CONVERSATION_MAP = (
    "DELETE FROM pulse_conversation_map "
    "WHERE COALESCE(last_seen, first_seen) < now() - make_interval(days => $1)"
)
_PRUNE_ANNOTATIONS = (
    "DELETE FROM pulse_annotations "
    "WHERE created_at < now() - make_interval(days => $1)"
)
_PRUNE_ALERTS = (
    "DELETE FROM pulse_alerts "
    "WHERE resolved_at IS NOT NULL AND triggered_at < now() - make_interval(days => $1)"
)


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
             json.dumps(s.attributes, default=str), s.business_error)
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

    async def evaluate_alerts(
        self,
        window_minutes: int = 15,
        p95_threshold_ms: float = 10_000,
        tokens_24h_threshold: int = 2_000_000,
    ) -> dict[str, int]:
        """One stateless evaluation pass: open alerts for currently-true
        conditions, resolve open alerts whose condition cleared."""
        assert self._pool is not None
        since = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
        since_24h = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        opened = resolved = 0
        async with self._pool.acquire() as conn:
            active: dict[str, list[tuple[str, str, str]]] = {}

            rows = await conn.fetch(_RULE_TOOL_FAILURE, since)
            active["tool_failure"] = [
                (r["target"], "error",
                 f"{r['n']} failed tool call(s) in the last {window_minutes} min")
                for r in rows
            ]
            rows = await conn.fetch(_RULE_APP_ERROR, since)
            active["app_error"] = [
                (r["target"], "error",
                 f"{r['n']} application error log(s) in the last {window_minutes} min")
                for r in rows
            ]
            rows = await conn.fetch(_RULE_SPAN_ERROR, since)
            active["span_error"] = [
                (r["target"], "error",
                 f"{r['n']} failed LLM/tool call(s) in the last {window_minutes} min")
                for r in rows
            ]
            p95 = (await conn.fetchrow(_RULE_P95, since))["p95"]
            active["latency_p95"] = (
                [("global", "warning",
                  f"p95 turn latency {p95 / 1000:.1f} s over the last "
                  f"{window_minutes} min (threshold {p95_threshold_ms / 1000:.0f} s)")]
                if p95 is not None and float(p95) > p95_threshold_ms else []
            )
            tokens = int((await conn.fetchrow(_RULE_TOKENS, since_24h))["tokens"])
            active["token_budget"] = (
                [("global", "warning",
                  f"{tokens:,} input tokens over 24 h "
                  f"(budget {tokens_24h_threshold:,})")]
                if tokens > tokens_24h_threshold else []
            )

            for rule_key, entries in active.items():
                for target, severity, message in entries:
                    result = await conn.execute(
                        _OPEN_ALERT, rule_key, target, severity, message,
                    )
                    if result.endswith("1"):
                        opened += 1
                result = await conn.execute(
                    _RESOLVE_STALE, rule_key, [t for t, _, _ in entries],
                )
                resolved += int(result.split()[-1])
        return {"opened": opened, "resolved": resolved}

    async def query_alerts(
        self,
        active: bool = True,
        user_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            records = await conn.fetch(_QUERY_ALERTS, active, user_id, limit)
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
        for key in ("input_tokens", "output_tokens", "cached_tokens"):
            out[key] = int(out[key])
        return out

    async def prune_old(self, days: int) -> dict[str, int]:
        """Delete telemetry older than ``days`` days; return {table: rows}.

        Logs, spans, conversation links and annotations are pruned by their
        event timestamp. Alerts are the exception: only *resolved* alerts are
        pruned, so a still-open incident is retained no matter how old it is.
        """
        assert self._pool is not None
        deleted: dict[str, int] = {}
        async with self._pool.acquire() as conn:
            for table, sql in (
                ("pulse_logs", _PRUNE_LOGS),
                ("pulse_spans", _PRUNE_SPANS),
                ("pulse_conversation_map", _PRUNE_CONVERSATION_MAP),
                ("pulse_annotations", _PRUNE_ANNOTATIONS),
                ("pulse_alerts", _PRUNE_ALERTS),
            ):
                result = await conn.execute(sql, days)
                deleted[table] = int(result.split()[-1])
        return deleted
