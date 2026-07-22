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

from contextlib import contextmanager
from typing import Iterator

CONVERSATION_ATTR = "gen_ai.conversation.id"
USER_ATTR = "user.id"


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
