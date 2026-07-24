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

from th2pulse.ingest.parsing import LogRow, SpanRow

# Order matters: specific patterns (bearer/iban/card) before generic ones.
MASK_PRESETS: list[tuple[str, re.Pattern[str]]] = [
    ("[masked secret]", re.compile(
        r"(?i)(?:bearer\s+[a-z0-9._\-]{12,}|(?:api[_-]?key|secret|token|password)"
        r"[\"']?\s*[:=]\s*[\"']?[^\s\"',}]{8,})")),
    ("[masked iban]", re.compile(
        r"\b[A-Z]{2}\d{2}(?:[ -]?[A-Z0-9]{4}){3,7}(?:[ -]?[A-Z0-9]{1,3})?\b")),
    ("[masked card]", re.compile(r"\b(?:\d[ -]?){13,16}\d\b")),
    ("[masked email]", re.compile(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
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


def mask_log_rows(rows: list[LogRow]) -> list[LogRow]:
    return [
        dataclasses.replace(row, body=mask_text(row.body)) if row.body else row
        for row in rows
    ]


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
