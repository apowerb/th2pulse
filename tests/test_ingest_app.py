"""Ingest app endpoints against an in-memory store."""
from datetime import datetime, timezone

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

    async def insert_logs(self, rows):
        self.logs.extend(rows)
        return len(rows)

    async def upsert_links(self, links):
        self.links.extend(links)
        return len(links)

    async def insert_spans(self, spans):
        self.spans.extend(spans)
        return len(spans)

    async def query_spans(self, conversation_id=None, limit=500):
        return [{"name": s.name, "trace_id": s.trace_id} for s in self.spans]

    async def query_logs(self, conversation_id=None, service=None,
                         min_severity=None, since=None, limit=100):
        out = self.logs
        if conversation_id is not None:
            traces = {l.trace_id for l in self.links
                      if l.conversation_id == conversation_id}
            out = [r for r in out if r.trace_id in traces]
        if min_severity is not None:
            out = [r for r in out if (r.severity_num or 0) >= min_severity]
        return [{"ts": r.ts, "body": r.body, "trace_id": r.trace_id} for r in out]

    async def query_conversations(self, limit=50):
        return [{"conversation_id": l.conversation_id} for l in self.links]


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
    assert [l["body"] for l in errors["logs"]] == ["boom"]

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
