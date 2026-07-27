"""Retention must key on last activity, not on when a conversation started.

Regression test for the orphaning bug: a conversation opened weeks ago but
still in use lost its row in ``pulse_conversation_map`` at prune time, so its
fresh logs stayed in the table while becoming unreachable to every
user-scoped query — invisible data that still occupies disk.
"""
import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

from th2pulse.ingest.parsing import ConversationLink, LogRow  # noqa: E402
from th2pulse.ingest.store import Store  # noqa: E402

DSN = os.environ.get("TH2PULSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TH2PULSE_TEST_DSN not set")

SCHEMA = "th2pulse_retention_test"
NOW = datetime.now(tz=timezone.utc)
OLD = NOW - timedelta(days=20)


def _row(ts, trace, body):
    return LogRow(ts=ts, severity_num=9, severity="INFO", service="svc",
                  trace_id=trace, span_id="b" * 16, body=body,
                  attributes={}, resource={})


def _link(trace, ts):
    return ConversationLink(conversation_id="c1", trace_id=trace,
                            user_id="alice@x.io", service="svc", first_seen=ts)


def _with_store(coro_fn):
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


def test_active_conversation_keeps_its_mapping_after_prune():
    """Old first_seen + recent activity: the fresh log must stay reachable.

    The trace_id is deliberately the *same* across both ingests: that is what
    makes the map row keep its old ``first_seen`` (kept via LEAST) while the
    conversation goes on. Pruning on ``first_seen`` deleted that single row
    and took today's log out of reach with it.
    """
    TRACE = "a" * 32

    async def scenario(store):
        await store.ingest_logs([_row(OLD, TRACE, "old-but-same-trace")],
                                [_link(TRACE, OLD)])
        await store.ingest_logs([_row(NOW, TRACE, "fresh")],
                                [_link(TRACE, NOW)])

        # Precondition: one row, old first_seen, recent last_seen.
        convs = await store.query_conversations()
        assert len(convs) == 1

        await store.prune_old(15)

        bodies = [r["body"] for r in await store.query_logs(user_id="alice@x.io")]
        assert "fresh" in bodies, "fresh log became invisible to its own user"
        assert "old-but-same-trace" not in bodies, "expired log should be pruned"
    _with_store(scenario)


def test_fully_idle_conversation_is_pruned():
    """Nothing recent at all: the mapping must still be reclaimed."""
    async def scenario(store):
        await store.ingest_logs([_row(OLD, "a" * 32, "expired")],
                                [_link("a" * 32, OLD)])

        deleted = await store.prune_old(15)

        assert deleted["pulse_conversation_map"] == 1
        assert await store.query_logs(user_id="alice@x.io") == []
    _with_store(scenario)
