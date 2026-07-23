"""Pure OTLP/HTTP JSON parsing — no I/O, fully unit-testable.

Payloads follow the proto3 JSON mapping emitted by the collector's
``otlphttp`` exporter with ``encoding: json``: camelCase keys, attributes as
``[{"key": ..., "value": {"stringValue": ...}}]`` lists, ``traceId``/``spanId``
hex-encoded, timestamps as stringified unix nanos.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

CONVERSATION_ATTR = "gen_ai.conversation.id"
USER_ATTR = "user.id"

# OTLP severity number ranges (lower bound of each bucket).
SEVERITY_FLOOR = {
    "TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "WARNING": 13,
    "ERROR": 17, "FATAL": 21, "CRITICAL": 21,
}


@dataclass(frozen=True)
class LogRow:
    ts: datetime
    severity_num: int | None
    severity: str | None
    service: str | None
    trace_id: str | None
    span_id: str | None
    body: str
    attributes: dict[str, Any]
    resource: dict[str, Any]
    event_name: str | None = None


@dataclass(frozen=True)
class SpanRow:
    ts: datetime
    duration_ms: float | None
    name: str
    service: str | None
    trace_id: str
    span_id: str
    parent_span_id: str | None
    status_code: str | None
    status_message: str | None
    attributes: dict[str, Any]


@dataclass(frozen=True)
class ConversationLink:
    conversation_id: str
    trace_id: str
    user_id: str | None
    service: str | None
    first_seen: datetime


def _any_value(value: dict[str, Any] | None) -> Any:
    if not isinstance(value, dict):
        return value
    if "stringValue" in value:
        return value["stringValue"]
    if "intValue" in value:
        try:
            return int(value["intValue"])
        except (TypeError, ValueError):
            return value["intValue"]
    if "doubleValue" in value:
        return value["doubleValue"]
    if "boolValue" in value:
        return value["boolValue"]
    if "arrayValue" in value:
        return [_any_value(v) for v in value["arrayValue"].get("values", [])]
    if "kvlistValue" in value:
        return _attributes(value["kvlistValue"].get("values", []))
    if "bytesValue" in value:
        return value["bytesValue"]
    return value or None


def _attributes(attr_list: list[dict[str, Any]] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in attr_list or []:
        key = item.get("key")
        if key:
            out[key] = _any_value(item.get("value"))
    return out


def _hex_id(raw: str | None) -> str | None:
    if not raw or set(raw) == {"0"}:
        return None
    return raw.lower()


def _ts(nanos: str | int | None, fallback: str | int | None = None) -> datetime:
    for candidate in (nanos, fallback):
        try:
            value = int(candidate)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if value > 0:
            return datetime.fromtimestamp(value / 1e9, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _body_text(body: dict[str, Any] | None) -> str:
    value = _any_value(body)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _link_from_attrs(
    attrs: dict[str, Any],
    trace_id: str | None,
    service: str | None,
    seen: datetime,
) -> ConversationLink | None:
    conversation_id = attrs.get(CONVERSATION_ATTR)
    if not conversation_id or not trace_id:
        return None
    user_id = attrs.get(USER_ATTR)
    return ConversationLink(
        conversation_id=str(conversation_id),
        trace_id=trace_id,
        user_id=str(user_id) if user_id is not None else None,
        service=service,
        first_seen=seen,
    )


def parse_logs(payload: dict[str, Any]) -> tuple[list[LogRow], list[ConversationLink]]:
    """OTLP logs payload -> rows + any conversation links found in attributes."""
    rows: list[LogRow] = []
    links: list[ConversationLink] = []
    for resource_logs in payload.get("resourceLogs", []):
        resource = _attributes(resource_logs.get("resource", {}).get("attributes"))
        service = resource.get("service.name")
        for scope_logs in resource_logs.get("scopeLogs", []):
            for record in scope_logs.get("logRecords", []):
                attrs = _attributes(record.get("attributes"))
                trace_id = _hex_id(record.get("traceId"))
                ts = _ts(record.get("timeUnixNano"), record.get("observedTimeUnixNano"))
                rows.append(LogRow(
                    ts=ts,
                    severity_num=record.get("severityNumber"),
                    severity=record.get("severityText") or None,
                    service=service,
                    trace_id=trace_id,
                    span_id=_hex_id(record.get("spanId")),
                    body=_body_text(record.get("body")),
                    attributes=attrs,
                    resource=resource,
                    event_name=record.get("eventName") or None,
                ))
                link = _link_from_attrs(attrs, trace_id, service, ts)
                if link:
                    links.append(link)
    return rows, links


def parse_traces(
    payload: dict[str, Any],
) -> tuple[list[ConversationLink], list[SpanRow]]:
    """OTLP traces payload -> conversation<->trace links + span rows.

    Spans carry what the GenAI event logs redact: tool names, call
    arguments, tool responses, model names, durations — the practical
    substance of agent monitoring.
    """
    links: list[ConversationLink] = []
    spans: list[SpanRow] = []
    for resource_spans in payload.get("resourceSpans", []):
        resource = _attributes(resource_spans.get("resource", {}).get("attributes"))
        service = resource.get("service.name")
        for scope_spans in resource_spans.get("scopeSpans", []):
            for span in scope_spans.get("spans", []):
                attrs = _attributes(span.get("attributes"))
                trace_id = _hex_id(span.get("traceId"))
                span_id = _hex_id(span.get("spanId"))
                start = _ts(span.get("startTimeUnixNano"))
                link = _link_from_attrs(attrs, trace_id, service, start)
                if link:
                    links.append(link)
                if not trace_id or not span_id:
                    continue
                duration_ms = None
                try:
                    duration_ms = (
                        int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])
                    ) / 1e6
                except (KeyError, TypeError, ValueError):
                    pass
                status = span.get("status") or {}
                spans.append(SpanRow(
                    ts=start,
                    duration_ms=duration_ms,
                    name=span.get("name") or "",
                    service=service,
                    trace_id=trace_id,
                    span_id=span_id,
                    parent_span_id=_hex_id(span.get("parentSpanId")),
                    status_code=str(status["code"]) if status.get("code") else None,
                    status_message=status.get("message") or None,
                    attributes=attrs,
                ))
    return links, spans


def min_severity_number(level: str) -> int | None:
    """Human level name -> OTLP severity-number floor (None if unknown)."""
    return SEVERITY_FLOOR.get(level.strip().upper())
