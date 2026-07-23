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


def test_parse_traces_extracts_links_and_spans():
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "th2agent-dev"}},
            ]},
            "scopeSpans": [{"spans": [
                {
                    "traceId": TRACE,
                    "spanId": SPAN,
                    "name": "execute_tool tool_send_email",
                    "startTimeUnixNano": "1753200000000000000",
                    "endTimeUnixNano": "1753200000250000000",
                    "attributes": [
                        {"key": "gen_ai.conversation.id",
                         "value": {"stringValue": "sess-42"}},
                        {"key": "user.id", "value": {"stringValue": "u@x.io"}},
                        {"key": "gen_ai.tool.name",
                         "value": {"stringValue": "tool_send_email"}},
                    ],
                },
                {"traceId": "c" * 32, "spanId": "e" * 16, "name": "call_llm",
                 "startTimeUnixNano": "1753200001000000000", "attributes": []},
            ]}],
        }]
    }
    links, spans = parse_traces(payload)
    assert len(links) == 1
    link = links[0]
    assert link.conversation_id == "sess-42"
    assert link.user_id == "u@x.io"
    assert link.first_seen == datetime.fromtimestamp(1753200000, tz=timezone.utc)

    assert len(spans) == 2
    tool = spans[0]
    assert tool.name == "execute_tool tool_send_email"
    assert tool.duration_ms == 250.0
    assert tool.attributes["gen_ai.tool.name"] == "tool_send_email"
    assert tool.service == "th2agent-dev"
    assert spans[1].duration_ms is None  # no end timestamp


def test_parse_logs_event_name():
    rows, _ = parse_logs(_logs_payload(eventName="gen_ai.choice"))
    assert rows[0].event_name == "gen_ai.choice"
    rows, _ = parse_logs(_logs_payload())
    assert rows[0].event_name is None


def test_business_error_detection():
    from th2pulse.ingest.parsing import business_error

    fail = {"gcp.vertex.agent.tool_response":
            '{"code": "INTEGRATION_MISSING", "provider": "google_gmail"}'}
    assert business_error(fail) is True
    pending = {"gcp.vertex.agent.tool_response":
               '{"status": "user_input_pending"}'}
    assert business_error(pending) is False
    ok = {"gcp.vertex.agent.tool_response": '{"status": "ok"}'}
    assert business_error(ok) is False
    assert business_error({}) is False
    assert business_error({"gcp.vertex.agent.tool_response": "{oops"}) is False


def test_parse_traces_flags_business_error():
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": []},
            "scopeSpans": [{"spans": [{
                "traceId": TRACE, "spanId": SPAN,
                "name": "execute_tool tool_send_email",
                "startTimeUnixNano": "1753200000000000000",
                "attributes": [
                    {"key": "gcp.vertex.agent.tool_response",
                     "value": {"stringValue": '{"code": "INTEGRATION_MISSING"}'}},
                ],
            }]}],
        }]
    }
    _, spans = parse_traces(payload)
    assert spans[0].business_error is True


def test_min_severity_number():
    assert min_severity_number("info") == 9
    assert min_severity_number("WARNING") == 13
    assert min_severity_number("bogus") is None
