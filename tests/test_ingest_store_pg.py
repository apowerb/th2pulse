"""Integration tests for Store against a real PostgreSQL.

Gated by ``TH2PULSE_TEST_DSN`` (skipped otherwise). Typical setup::

    docker run -d --rm --name pulse-test-pg -p 127.0.0.1:5433:5432 \
        -e POSTGRES_PASSWORD=t postgres:16
    TH2PULSE_TEST_DSN=postgresql://postgres:t@127.0.0.1:5433/postgres pytest

Each test runs its full lifecycle inside a single event loop (asyncpg
pools are loop-bound).
"""
import asyncio
import os
from datetime import datetime, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

from th2pulse.ingest.parsing import ConversationLink, LogRow, SpanRow  # noqa: E402
from th2pulse.ingest.store import Store  # noqa: E402

DSN = os.environ.get("TH2PULSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TH2PULSE_TEST_DSN not set")

SCHEMA = "th2pulse_test"
TS = datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc)
TRACE, SPAN = "a" * 32, "b" * 16


def _row(body="hello"):
    return LogRow(ts=TS, severity_num=9, severity="INFO", service="svc",
                  trace_id=TRACE, span_id=SPAN, body=body,
                  attributes={"k": "v"}, resource={"service.name": "svc"},
                  event_name="gen_ai.choice")


def _span(span_id="c" * 16):
    return SpanRow(ts=TS, duration_ms=1.5, name="execute_tool t", service="svc",
                   trace_id=TRACE, span_id=span_id, parent_span_id=None,
                   status_code=None, status_message=None,
                   attributes={"gen_ai.tool.name": "t"})


def _link(user="alice@x.io"):
    return ConversationLink(conversation_id="c1", trace_id=TRACE,
                            user_id=user, service="svc", first_seen=TS)


def _with_store(coro_fn):
    """Reset schema, connect, run, close — all inside one event loop."""
    async def runner():
        conn = await asyncpg.connect(DSN)
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {SCHEMA}")
        await conn.close()
        store = Store(DSN, schema=SCHEMA)
        await store.connect()
        try:
            await coro_fn(store)
        finally:
            await store.close()
    asyncio.run(runner())


def test_ddl_idempotent_and_jsonb_roundtrip():
    async def scenario(store):
        # DDL a second time on the same schema (restart) must not fail.
        await store.close()
        await store.connect()
        await store.ingest_logs([_row()], [_link()])
        logs = await store.query_logs(conversation_id="c1")
        assert len(logs) == 1
        assert logs[0]["attributes"] == {"k": "v"}
        assert logs[0]["event_name"] == "gen_ai.choice"
    _with_store(scenario)


def test_span_and_link_replay_is_idempotent():
    async def scenario(store):
        await store.ingest_traces([_link()], [_span()])
        await store.ingest_traces([_link()], [_span()])  # collector retry
        spans = await store.query_spans(conversation_id="c1")
        assert len(spans) == 1
        convs = await store.query_conversations()
        assert len(convs) == 1 and convs[0]["trace_count"] == 1
    _with_store(scenario)


def test_user_scoping_in_sql():
    async def scenario(store):
        await store.ingest_logs([_row()], [_link(user="alice@x.io")])
        await store.ingest_traces([_link(user="alice@x.io")], [_span()])
        assert len(await store.query_logs(user_id="alice@x.io")) == 1
        assert await store.query_logs(user_id="bob@x.io") == []
        assert await store.query_spans(user_id="bob@x.io") == []
        assert await store.query_conversations(user_id="bob@x.io") == []
    _with_store(scenario)


def test_stats_aggregates_and_scoping():
    async def scenario(store):
        inv = SpanRow(ts=TS, duration_ms=2000.0, name="invocation", service="svc",
                      trace_id=TRACE, span_id="d" * 16, parent_span_id=None,
                      status_code=None, status_message=None, attributes={})
        llm = SpanRow(ts=TS, duration_ms=1000.0, name="generate_content m",
                      service="svc", trace_id=TRACE, span_id="e" * 16,
                      parent_span_id=None, status_code=None, status_message=None,
                      attributes={"gen_ai.usage.input_tokens": 100,
                                  "gen_ai.usage.output_tokens": 10})
        await store.ingest_traces([_link()], [inv, llm, _span()])
        since = datetime(2026, 7, 1, tzinfo=timezone.utc)
        stats = await store.query_stats(since=since)
        assert stats["conversations"] == 1 and stats["turns"] == 1
        assert stats["avg_turn_ms"] == 2000.0
        assert stats["tool_calls"] == 1
        assert stats["input_tokens"] == 100 and stats["output_tokens"] == 10
        scoped = await store.query_stats(since=since, user_id="bob@x.io")
        assert scoped["conversations"] == 0 and scoped["input_tokens"] == 0
    _with_store(scenario)


def test_annotations_scoped_write_and_read():
    async def scenario(store):
        await store.ingest_traces([_link(user="alice@x.io")], [_span()])
        denied = await store.insert_annotation(
            "c1", TRACE, "mallory@x.io", "spam", user_id="mallory@x.io",
        )
        assert denied is None
        ok = await store.insert_annotation(
            "c1", TRACE, "alice@x.io", "looks bad", user_id="alice@x.io",
        )
        assert isinstance(ok, int)
        notes = await store.query_annotations("c1", user_id="alice@x.io")
        assert len(notes) == 1 and notes[0]["note"] == "looks bad"
    _with_store(scenario)


def test_failed_batch_rolls_back_entirely():
    async def scenario(store):
        bad_link = ConversationLink(conversation_id=None, trace_id=TRACE,  # type: ignore[arg-type]
                                    user_id=None, service=None, first_seen=TS)
        with pytest.raises(Exception):
            await store.ingest_logs([_row()], [bad_link])
        # The log insert from the failed batch must not have been committed.
        assert await store.query_logs() == []
    _with_store(scenario)
