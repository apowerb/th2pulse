"""Conversation-level correlation helpers.

ADK >= 1.36 already stamps ``gen_ai.conversation.id`` (= the session id the
frontend knows) and ``user.id`` on every span it emits — measured on a real
service, nothing to do there.

Use :func:`conversation_context` in the code paths ADK does not cover
(background jobs, plain FastAPI endpoints, RAG/ETL services) so their
telemetry joins the same conversation: it sets OTel baggage for downstream
spans and, when a span is currently recording, tags it directly with the
same attribute names ADK uses.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

CONVERSATION_ATTR = "gen_ai.conversation.id"
USER_ATTR = "user.id"


class BaggageLogFilter(logging.Filter):
    """Stamp OTel baggage identity onto log records as attributes.

    Attached automatically by ``init_observability`` to the OTLP handler:
    records emitted inside :func:`conversation_context` carry
    ``gen_ai.conversation.id`` / ``user.id`` as OTLP log attributes, so a
    log store can filter them **per conversation directly** — no
    conversation↔trace mapping needed for bridged application logs.
    (Stores like Loki normalize dots: query ``gen_ai_conversation_id``.)
    """

    _KEYS = (CONVERSATION_ATTR, USER_ATTR)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from opentelemetry import baggage

            for key in self._KEYS:
                value = baggage.get_baggage(key)
                if value is not None:
                    setattr(record, key, str(value))
        except Exception:  # noqa: BLE001 - never drop or break a record
            pass
        return True


@contextmanager
def conversation_context(
    conversation_id: str, user_id: str | None = None
) -> Iterator[None]:
    """Tag the current OTel context with conversation/user identity."""
    from opentelemetry import baggage, context, trace

    ctx = baggage.set_baggage(CONVERSATION_ATTR, conversation_id)
    if user_id is not None:
        ctx = baggage.set_baggage(USER_ATTR, user_id, context=ctx)
    token = context.attach(ctx)

    span = trace.get_current_span()
    if span.is_recording():
        span.set_attribute(CONVERSATION_ATTR, conversation_id)
        if user_id is not None:
            span.set_attribute(USER_ATTR, user_id)
    try:
        yield
    finally:
        context.detach(token)
