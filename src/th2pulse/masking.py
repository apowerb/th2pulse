"""Selective masking of sensitive patterns in captured content.

Industry-standard alternative to full elision (Langfuse ``mask``,
LangSmith anonymizer, Datadog Sensitive Data Scanner): message content
stays readable, only sensitive substrings are substituted. Applied at
ingestion time — what reaches storage is already masked, raw content is
never persisted.

Enabled by default (``TH2PULSE_MASKING=off`` to disable). Masking plain
text is a no-op when content is elided upstream, so it is always safe to
keep on.
"""
from __future__ import annotations

import dataclasses
import os
import re
from typing import Any

from th2pulse.ingest.parsing import LogRow, SpanRow

# Order matters: specific patterns (bearer/iban/card) before generic ones.
MASK_PRESETS: list[tuple[str, re.Pattern[str]]] = [
    ("[masked secret]", re.compile(
        r"(?i)(?:bearer\s+[a-z0-9._\-]{12,4096}|(?:api[_-]?key|secret|token|password)"
        r"[\"']?\s*[:=]\s*[\"']?[^\s\"',}]{8,4096})")),
    ("[masked iban]", re.compile(
        r"\b[A-Z]{2}\d{2}(?:[ -]?[A-Z0-9]{4}){3,7}(?:[ -]?[A-Z0-9]{1,3})?\b")),
    ("[masked card]", re.compile(r"\b(?:\d[ -]?){13,16}\d\b")),
    # Quantifiers are bounded on purpose. Unbounded `+` runs turn quadratic on
    # text dense in `. - _ % +`: every one of those characters is a word
    # boundary, so each restarts a full scan that backtracks to the end
    # looking for an `@`. Measured 8.4s on 100 KB before bounding — a single
    # attribute value was enough to pin a worker. RFC 5321 caps the local
    # part at 64 characters and a domain at 255, so nothing real is lost.
    ("[masked email]", re.compile(
        r"\b[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9-]{1,63}(?:\.[A-Za-z0-9-]{1,63}){0,12}"
        r"\.[A-Za-z]{2,24}\b")),
    ("[masked phone]", re.compile(
        r"(?<!\w)(?:\+|00)\d{1,3}(?:[ .-]?\(0\))?(?:[ .-]?\d{1,4}){3,6}(?!\w)"
        r"|(?<!\w)0\d(?:[ .-]?\d{2}){4}(?!\w)")),
]

# Span attributes that may carry user content (ADK exports them when
# ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS is on, which is its default).
MASKED_SPAN_ATTRS = (
    "gcp.vertex.agent.tool_call_args",
    "gcp.vertex.agent.tool_response",
    "gcp.vertex.agent.llm_request",
    "gcp.vertex.agent.llm_response",
)


def masking_enabled() -> bool:
    return os.environ.get("TH2PULSE_MASKING", "on").lower() not in ("off", "0", "false")


def mask_text(text: str) -> str:
    """Substitute every sensitive pattern in ``text``. Never raises."""
    if not text:
        return text
    try:
        for replacement, pattern in MASK_PRESETS:
            text = pattern.sub(replacement, text)
        return text
    except Exception:  # noqa: BLE001 - masking must never break ingestion
        return text


def mask_value(value: Any) -> Any:
    """Mask every string reachable inside ``value``, keeping its shape.

    OTLP ``kvlistValue`` and ``arrayValue`` decode into nested dicts and
    lists, so masking only the top level left an email or an IBAN one level
    down perfectly readable in JSONB. Non-string leaves are returned as-is so
    the JSONB round-trip keeps its types.
    """
    if isinstance(value, str):
        return mask_text(value)
    if isinstance(value, dict):
        return {key: mask_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        masked = [mask_value(item) for item in value]
        return type(value)(masked) if isinstance(value, tuple) else masked
    return value


def mask_log_rows(rows: list[LogRow]) -> list[LogRow]:
    """Mask the body *and* the free-form attributes of each record.

    ``logging.info(msg, extra={...})`` lands in ``attributes`` and is stored
    verbatim in JSONB; masking only the body left that side untouched while
    the documentation promised raw content is never persisted.
    """
    out: list[LogRow] = []
    for row in rows:
        body = mask_text(row.body) if row.body else row.body
        attributes = mask_value(row.attributes) if row.attributes else row.attributes
        if body is not row.body or attributes is not row.attributes:
            row = dataclasses.replace(row, body=body, attributes=attributes)
        out.append(row)
    return out


def mask_span_rows(spans: list[SpanRow]) -> list[SpanRow]:
    out: list[SpanRow] = []
    for span in spans:
        touched = {
            key: mask_text(value)
            for key in MASKED_SPAN_ATTRS
            if isinstance((value := span.attributes.get(key)), str)
        }
        if touched:
            span = dataclasses.replace(
                span, attributes={**span.attributes, **touched},
            )
        out.append(span)
    return out
