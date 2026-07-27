"""Read-side authentication, nested masking, and masking cost.

Each test here covers a finding that shipped green once: the query API was
unauthenticated, nested attributes escaped masking, and the email pattern was
quadratic on text dense in `. - _ % +`.
"""
import time

import pytest
from fastapi.testclient import TestClient

from th2pulse.ingest.app import create_app
from th2pulse.ingest.parsing import LogRow
from th2pulse.masking import mask_log_rows, mask_text

READ_ROUTES = ["/logs", "/spans", "/alerts", "/stats", "/conversations",
               "/annotations?conversation_id=c1"]


class _FakeStore:
    async def connect(self): ...
    async def close(self): ...
    async def query_logs(self, **_): return [{"body": "everyone's telemetry"}]
    async def query_spans(self, **_): return []
    async def query_annotations(self, **_): return []
    async def query_alerts(self, **_): return []
    async def query_stats(self, **_): return {}
    async def query_conversations(self, **_): return []
    async def insert_annotation(self, **_): return 1
    async def evaluate_alerts(self, **_): return {}
    async def prune_old(self, _days): return {}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("TH2PULSE_QUERY_TOKEN", "s3cret")
    monkeypatch.setenv("TH2PULSE_ALERT_INTERVAL_S", "0")
    monkeypatch.setenv("TH2PULSE_RETENTION_INTERVAL_S", "0")
    return TestClient(create_app(_FakeStore()))


@pytest.mark.parametrize("route", READ_ROUTES)
def test_read_routes_reject_missing_token(client, route):
    assert client.get(route).status_code == 401


@pytest.mark.parametrize("route", READ_ROUTES)
def test_read_routes_accept_valid_token(client, route):
    assert client.get(route, headers={"X-Th2Pulse-Query-Token": "s3cret"}).status_code == 200


def test_read_routes_reject_wrong_token(client):
    assert client.get("/logs", headers={"X-Th2Pulse-Query-Token": "nope"}).status_code == 401


def test_post_annotations_requires_token(client):
    payload = {"conversation_id": "c1", "author": "a@b.io", "note": "n"}
    assert client.post("/annotations", json=payload).status_code == 401
    ok = client.post("/annotations", json=payload,
                     headers={"X-Th2Pulse-Query-Token": "s3cret"})
    assert ok.status_code == 200


def test_non_ascii_token_yields_401_not_500(client):
    """A raw non-ASCII byte is legal on the wire; it must not crash the guard.

    Sent as bytes because httpx refuses non-ASCII str headers client-side —
    a real HTTP client has no such qualms, and hmac.compare_digest used to
    raise TypeError on it, turning a bad token into a 500.
    """
    res = client.get("/logs", headers={b"X-Th2Pulse-Query-Token": b"\xe9-bad"})
    assert res.status_code == 401


def test_healthz_stays_open(client):
    assert client.get("/healthz").status_code == 200


def test_open_when_no_token_configured(monkeypatch):
    monkeypatch.delenv("TH2PULSE_QUERY_TOKEN", raising=False)
    monkeypatch.setenv("TH2PULSE_ALERT_INTERVAL_S", "0")
    monkeypatch.setenv("TH2PULSE_RETENTION_INTERVAL_S", "0")
    assert TestClient(create_app(_FakeStore())).get("/logs").status_code == 200


def _row(attributes):
    return LogRow(ts=None, severity_num=9, severity="INFO", service="svc",
                  trace_id="t", span_id="s", body="", attributes=attributes,
                  resource={})


def test_nested_attributes_are_masked():
    """kvlistValue/arrayValue decode to nested containers — PII hid there."""
    attrs = {
        "flat": "victim@example.com",
        "user_info": {"email": "nested@example.com", "deep": {"m": "x@y.io"}},
        "tags": ["free", "list@example.com"],
        "count": 42,
    }
    masked = mask_log_rows([_row(attrs)])[0].attributes
    assert "@example.com" not in str(masked), f"PII survived: {masked}"
    assert masked["count"] == 42, "non-string values must keep their type"
    assert isinstance(masked["tags"], list)


def test_email_masking_is_not_quadratic():
    """Text dense in `. - _ % +` used to cost seconds per 100 KB."""
    hostile = "a.b-c.d_e%f+g" * 8000  # ~104 KB, no real email inside
    start = time.perf_counter()
    mask_text(hostile)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5, f"masking took {elapsed:.2f}s on 100 KB"


def test_real_emails_are_still_masked():
    assert "user@example.com" not in mask_text("write to user@example.com now")
    assert "[masked email]" in mask_text("write to user@example.com now")
    assert "a.b+c@sub.domain.co.uk" not in mask_text("x a.b+c@sub.domain.co.uk y")
