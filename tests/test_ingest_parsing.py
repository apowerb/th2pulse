"""OTLP JSON parsing: rows, ids, timestamps, conversation links."""
from datetime import datetime, timezone

from th2pulse.ingest.parsing import (
    min_severity_number,
    parse_logs,
    parse_traces,
)

TRACE = "a" * 32
SPAN = "b" * 16


def _logs_payload(**record_overrides):
    record = {
        "timeUnixNano": "1753200000000000000",
        "severityNumber": 9,
        "severityText": "INFO",
        "body": {"stringValue": "hello world"},
        "traceId": TRACE,
        "spanId": SPAN,
        "attributes": [
            {"key": "code.function", "value": {"stringValue": "handler"}},
            {"key": "retries", "value": {"intValue": "3"}},
        ],
    }
    record.update(record_overrides)
    return {
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "th2agent-dev"}},
            ]},
            "scopeLogs": [{"logRecords": [record]}],
        }]
    }


def test_parse_logs_row():
    rows, links = parse_logs(_logs_payload())
    assert len(rows) == 1 and links == []
    row = rows[0]
    assert row.service == "th2agent-dev"
    assert row.trace_id == TRACE and row.span_id == SPAN
    assert row.severity == "INFO" and row.severity_num == 9
    assert row.body == "hello world"
    assert row.attributes == {"code.function": "handler", "retries": 3}
    assert row.ts == datetime.fromtimestamp(1753200000, tz=timezone.utc)


def test_parse_logs_zero_ids_become_null():
    rows, _ = parse_logs(_logs_payload(traceId="0" * 32, spanId=""))
    assert rows[0].trace_id is None and rows[0].span_id is None


def test_parse_logs_structured_body_serialized():
    rows, _ = parse_logs(_logs_payload(body={"kvlistValue": {"values": [
        {"key": "event", "value": {"stringValue": "tool_call"}},
    ]}}))
    assert rows[0].body == '{"event": "tool_call"}'


def test_parse_logs_conversation_attr_creates_link():
    rows, links = parse_logs(_logs_payload(attributes=[
        {"key": "gen_ai.conversation.id", "value": {"stringValue": "conv-9"}},
    ]))
    assert len(links) == 1
    assert links[0].conversation_id == "conv-9"
    assert links[0].trace_id == TRACE
    assert links[0].service == "th2agent-dev"


def test_parse_traces_extracts_links():
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "th2agent-dev"}},
            ]},
            "scopeSpans": [{"spans": [
                {
                    "traceId": TRACE,
                    "startTimeUnixNano": "1753200000000000000",
                    "attributes": [
                        {"key": "gen_ai.conversation.id",
                         "value": {"stringValue": "sess-42"}},
                        {"key": "user.id", "value": {"stringValue": "u@x.io"}},
                    ],
                },
                {"traceId": "c" * 32, "attributes": []},  # no conversation attr
            ]}],
        }]
    }
    links = parse_traces(payload)
    assert len(links) == 1
    link = links[0]
    assert link.conversation_id == "sess-42"
    assert link.user_id == "u@x.io"
    assert link.first_seen == datetime.fromtimestamp(1753200000, tz=timezone.utc)


def test_min_severity_number():
    assert min_severity_number("info") == 9
    assert min_severity_number("WARNING") == 13
    assert min_severity_number("bogus") is None
