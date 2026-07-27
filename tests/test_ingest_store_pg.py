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
from datetime import datetime, timedelta, timezone

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


def test_stats_aggregates_dedup_and_cache():
    async def scenario(store):
        inv = SpanRow(ts=TS, duration_ms=2000.0, name="invocation", service="svc",
                      trace_id=TRACE, span_id="d" * 16, parent_span_id=None,
                      status_code=None, status_message=None, attributes={})
        # ADK emits usage on BOTH call_llm and its generate_content child.
        # Only the call_llm figure must count (avoid double-count), and it
        # carries the cached mirror added by th2agent's usage callback.
        call_llm = SpanRow(ts=TS, duration_ms=1000.0, name="call_llm",
                           service="svc", trace_id=TRACE, span_id="e" * 16,
                           parent_span_id=None, status_code=None,
                           status_message=None,
                           attributes={"gen_ai.usage.input_tokens": 100,
                                       "gen_ai.usage.output_tokens": 10,
                                       "gen_ai.usage.cached_input_tokens": 70})
        gen = SpanRow(ts=TS, duration_ms=900.0, name="generate_content m",
                      service="svc", trace_id=TRACE, span_id="f" * 16,
                      parent_span_id="e" * 16, status_code=None,
                      status_message=None,
                      attributes={"gen_ai.usage.input_tokens": 100,
                                  "gen_ai.usage.output_tokens": 10})
        await store.ingest_traces([_link()], [inv, call_llm, gen, _span()])
        since = datetime(2026, 7, 1, tzinfo=timezone.utc)
        stats = await store.query_stats(since=since)
        assert stats["conversations"] == 1 and stats["turns"] == 1
        assert stats["avg_turn_ms"] == 2000.0
        assert stats["tool_calls"] == 1
        # counted once (call_llm), NOT 200 despite the duplicate on generate_content
        assert stats["input_tokens"] == 100 and stats["output_tokens"] == 10
        assert stats["cached_tokens"] == 70
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


def test_alert_lifecycle_open_dedup_resolve():
    async def scenario(store):
        failing = SpanRow(
            ts=datetime.now(timezone.utc), duration_ms=76.0,
            name="execute_tool tool_send_email", service="svc",
            trace_id=TRACE, span_id="f" * 16, parent_span_id=None,
            status_code=None, status_message=None,
            attributes={"gen_ai.tool.name": "tool_send_email"},
            business_error=True,
        )
        await store.ingest_traces([_link()], [failing])

        first = await store.evaluate_alerts(window_minutes=15)
        assert first["opened"] == 1
        second = await store.evaluate_alerts(window_minutes=15)
        assert second["opened"] == 0  # dedup: one open alert per rule+target

        alerts = await store.query_alerts(active=True, user_id="alice@x.io")
        assert len(alerts) == 1
        assert alerts[0]["rule_key"] == "tool_failure"
        assert alerts[0]["target"] == "c1"
        # Scoping: another user sees nothing.
        assert await store.query_alerts(active=True, user_id="bob@x.io") == []

        # Condition clears (evaluate over a window where nothing happened
        # by making the span "old" relative to a 0-minute window).
        cleared = await store.evaluate_alerts(window_minutes=0)
        assert cleared["resolved"] == 1
        assert await store.query_alerts(active=True) == []
    _with_store(scenario)


def test_span_error_status_raises_alert_once():
    async def scenario(store):
        now = datetime.now(timezone.utc)
        # One LLM failure propagates to 4 spans (leaf + ancestors); only the
        # leaf generate_content must count — a single alert, not four.
        common = dict(status_code="2", status_message="APIError: Forbidden",
                      service="svc", trace_id=TRACE, business_error=False)
        gen = SpanRow(ts=now, duration_ms=100.0, name="generate_content m",
                      span_id="a" * 16, parent_span_id=None, attributes={},
                      **common)
        call = SpanRow(ts=now, duration_ms=110.0, name="call_llm",
                       span_id="b" * 16, parent_span_id="a" * 16, attributes={},
                       **common)
        inv = SpanRow(ts=now, duration_ms=120.0, name="invoke_agent x",
                      span_id="c" * 16, parent_span_id=None, attributes={},
                      **common)
        await store.ingest_traces([_link()], [gen, call, inv])

        opened = await store.evaluate_alerts(window_minutes=15)
        assert opened["opened"] == 1  # not 3
        alerts = await store.query_alerts(active=True)
        assert len(alerts) == 1
        assert alerts[0]["rule_key"] == "span_error"
        assert "1 failed LLM/tool call" in alerts[0]["message"]
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


def test_prune_old_drops_aged_rows_and_keeps_open_alerts():
    async def scenario(store):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=20)  # past the 15-day threshold
        old_trace, new_trace = "1" * 32, "2" * 32

        def _s(trace, span, ts):
            return SpanRow(ts=ts, duration_ms=1.0, name="execute_tool t",
                           service="svc", trace_id=trace, span_id=span,
                           parent_span_id=None, status_code=None,
                           status_message=None, attributes={})

        def _l(trace, span, ts, body):
            return LogRow(ts=ts, severity_num=9, severity="INFO", service="svc",
                          trace_id=trace, span_id=span, body=body,
                          attributes={}, resource={}, event_name=None)

        old_link = ConversationLink(conversation_id="cold", trace_id=old_trace,
                                    user_id="u", service="svc", first_seen=old)
        new_link = ConversationLink(conversation_id="cnew", trace_id=new_trace,
                                    user_id="u", service="svc", first_seen=now)
        await store.ingest_traces(
            [old_link, new_link],
            [_s(old_trace, "a" * 16, old), _s(new_trace, "b" * 16, now)],
        )
        await store.ingest_logs(
            [_l(old_trace, "a" * 16, old, "old"),
             _l(new_trace, "b" * 16, now, "new")],
            [],
        )

        # Annotations and alerts need explicit historical timestamps that the
        # public API (created_at/triggered_at default to now()) cannot set.
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f"SET search_path TO {SCHEMA}")
            await conn.execute(
                "INSERT INTO pulse_annotations "
                "(conversation_id, trace_id, author, note, created_at) "
                "VALUES ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10)",
                "cold", old_trace, "a", "old note", old,
                "cnew", new_trace, "a", "new note", now,
            )
            await conn.execute(
                "INSERT INTO pulse_alerts "
                "(rule_key, target, severity, message, triggered_at, resolved_at) "
                "VALUES ($1,$2,$3,$4,$5,$6),($7,$8,$9,$10,$11,$12)",
                "r_open", "t1", "warning", "old but open", old, None,
                "r_done", "t2", "warning", "old and resolved", old, old,
            )
        finally:
            await conn.close()

        deleted = await store.prune_old(15)

        assert deleted["pulse_logs"] == 1
        assert deleted["pulse_spans"] == 1
        assert deleted["pulse_conversation_map"] == 1
        assert deleted["pulse_annotations"] == 1
        assert deleted["pulse_alerts"] == 1  # only the resolved one

        # Recent rows survive across every table.
        assert len(await store.query_spans()) == 1
        assert len(await store.query_logs()) == 1
        convs = await store.query_conversations()
        assert len(convs) == 1 and convs[0]["conversation_id"] == "cnew"
        assert len(await store.query_annotations("cnew")) == 1
        assert await store.query_annotations("cold") == []

        # The old OPEN alert is retained; the old RESOLVED alert is gone.
        remaining = await store.query_alerts(active=False)
        assert len(remaining) == 1
        assert remaining[0]["rule_key"] == "r_open"
    _with_store(scenario)
