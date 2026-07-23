"""Ingest app endpoints against an in-memory store."""
import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from th2pulse.ingest.app import create_app  # noqa: E402
from th2pulse.ingest.parsing import SEVERITY_FLOOR  # noqa: E402

TRACE = "d" * 32


class FakeStore:
    def __init__(self):
        self.logs, self.links, self.spans = [], [], []

    async def connect(self): ...
    async def close(self): ...

    async def ingest_logs(self, rows, links):
        self.logs.extend(rows)
        self.links.extend(links)
        return len(rows)

    async def ingest_traces(self, links, spans):
        self.links.extend(links)
        self.spans.extend(spans)
        return len(spans)

    async def query_spans(self, conversation_id=None, limit=500, user_id=None):
        return [{"name": s.name, "trace_id": s.trace_id} for s in self.spans]

    async def query_logs(self, conversation_id=None, service=None,
                         min_severity=None, since=None, limit=100, user_id=None):
        out = self.logs
        if conversation_id is not None:
            traces = {link.trace_id for link in self.links
                      if link.conversation_id == conversation_id}
            out = [r for r in out if r.trace_id in traces]
        if user_id is not None:
            traces = {link.trace_id for link in self.links
                      if link.user_id == user_id}
            out = [r for r in out if r.trace_id in traces]
        if min_severity is not None:
            out = [r for r in out if (r.severity_num or 0) >= min_severity]
        return [{"ts": r.ts, "body": r.body, "trace_id": r.trace_id} for r in out]

    async def query_conversations(self, limit=50, user_id=None):
        return [{"conversation_id": link.conversation_id} for link in self.links
                if user_id is None or link.user_id == user_id]


@pytest.fixture()
def client():
    with TestClient(create_app(store=FakeStore())) as c:
        yield c


def _otlp_logs(body: str, severity=9):
    return {"resourceLogs": [{"resource": {"attributes": []}, "scopeLogs": [
        {"logRecords": [{
            "timeUnixNano": "1753200000000000000",
            "severityNumber": severity, "severityText": "INFO",
            "body": {"stringValue": body}, "traceId": TRACE, "spanId": "e" * 16,
        }]}]}]}


def _otlp_traces(conversation_id: str):
    return {"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [
        {"spans": [{
            "traceId": TRACE, "spanId": "f" * 16,
            "name": "execute_tool send_mail",
            "startTimeUnixNano": "1753200000000000000",
            "attributes": [{"key": "gen_ai.conversation.id",
                            "value": {"stringValue": conversation_id}}],
        }]}]}]}


def test_ingest_then_query_by_conversation(client):
    assert client.post("/v1/logs", json=_otlp_logs("agent step")).status_code == 200
    assert client.post("/v1/traces", json=_otlp_traces("conv-1")).status_code == 200

    hit = client.get("/logs", params={"conversation_id": "conv-1"}).json()
    assert hit["count"] == 1 and hit["logs"][0]["body"] == "agent step"

    miss = client.get("/logs", params={"conversation_id": "other"}).json()
    assert miss["count"] == 0


def test_level_filter_and_validation(client):
    client.post("/v1/logs", json=_otlp_logs("info", severity=9))
    client.post("/v1/logs", json=_otlp_logs("boom", severity=SEVERITY_FLOOR["ERROR"]))

    errors = client.get("/logs", params={"level": "error"}).json()
    assert [row["body"] for row in errors["logs"]] == ["boom"]

    assert client.get("/logs", params={"level": "nope"}).status_code == 422


def test_gzip_body_decompressed(client):
    import gzip
    import json as jsonlib

    body = gzip.compress(jsonlib.dumps(_otlp_logs("compressed step")).encode())
    resp = client.post("/v1/logs", content=body, headers={
        "content-type": "application/json", "content-encoding": "gzip",
    })
    assert resp.status_code == 200
    assert client.get("/logs").json()["logs"][0]["body"] == "compressed step"


def test_non_json_content_type_rejected_with_hint(client):
    resp = client.post("/v1/logs", content=b"\x0a\x03abc",
                       headers={"content-type": "application/x-protobuf"})
    assert resp.status_code == 415
    assert "encoding: json" in resp.json()["detail"]


def test_traces_store_spans_served_by_spans_endpoint(client):
    assert client.post("/v1/traces", json=_otlp_traces("conv-9")).status_code == 200
    data = client.get("/spans", params={"conversation_id": "conv-9"}).json()
    assert data["count"] == 1
    assert data["spans"][0]["name"] == "execute_tool send_mail"


def test_metrics_accepted_and_dropped(client):
    assert client.post("/v1/metrics", json={"resourceMetrics": []}).status_code == 200


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_user_scoping_filters_conversations_and_logs(client):
    client.post("/v1/logs", json=_otlp_logs("visible step"))
    payload = _otlp_traces("conv-A")
    payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"].append(
        {"key": "user.id", "value": {"stringValue": "alice@x.io"}},
    )
    client.post("/v1/traces", json=payload)

    mine = client.get("/logs", params={"user_id": "alice@x.io"}).json()
    assert mine["count"] == 1
    other = client.get("/logs", params={"user_id": "bob@x.io"}).json()
    assert other["count"] == 0
    convs = client.get("/conversations", params={"user_id": "bob@x.io"}).json()
    assert convs["count"] == 0


def test_oversized_payload_rejected(client):
    import gzip as gz

    big = b'{"resourceLogs": ["' + b"a" * (11 * 1024 * 1024) + b'"]}'
    resp = client.post("/v1/logs", content=big,
                       headers={"content-type": "application/json"})
    assert resp.status_code == 413

    bomb = gz.compress(b'[" ' + b"b" * (33 * 1024 * 1024) + b'"]')
    resp = client.post("/v1/logs", content=bomb, headers={
        "content-type": "application/json", "content-encoding": "gzip",
    })
    assert resp.status_code == 413


def test_ingest_token_required_when_configured(monkeypatch):
    monkeypatch.setenv("TH2PULSE_INGEST_TOKEN", "sekret")
    with TestClient(create_app(store=FakeStore())) as c:
        no_token = c.post("/v1/logs", json=_otlp_logs("x"))
        assert no_token.status_code == 401
        ok = c.post("/v1/logs", json=_otlp_logs("x"),
                    headers={"X-Th2Pulse-Token": "sekret"})
        assert ok.status_code == 200
