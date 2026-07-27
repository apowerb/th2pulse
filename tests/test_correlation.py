"""conversation_context: baggage + span attributes, cleanly detached."""
import logging

from opentelemetry import baggage
from opentelemetry.sdk.trace import TracerProvider

from th2pulse import BaggageLogFilter, conversation_context


def test_baggage_set_and_detached():
    assert baggage.get_baggage("gen_ai.conversation.id") is None
    with conversation_context("conv-1", "user-1"):
        assert baggage.get_baggage("gen_ai.conversation.id") == "conv-1"
        assert baggage.get_baggage("user.id") == "user-1"
    assert baggage.get_baggage("gen_ai.conversation.id") is None


def test_span_attributes_when_recording():
    tracer = TracerProvider().get_tracer("t")
    with tracer.start_as_current_span("s") as span:
        with conversation_context("conv-2", "user-2"):
            assert span.attributes["gen_ai.conversation.id"] == "conv-2"
            assert span.attributes["user.id"] == "user-2"


def test_no_span_is_fine():
    with conversation_context("conv-3"):
        assert baggage.get_baggage("gen_ai.conversation.id") == "conv-3"


def _record() -> logging.LogRecord:
    return logging.LogRecord(
        name="t", level=logging.INFO, pathname=__file__, lineno=1,
        msg="m", args=(), exc_info=None,
    )


def test_baggage_filter_stamps_records_inside_context():
    f = BaggageLogFilter()
    record = _record()
    with conversation_context("conv-4", "u@x.io"):
        assert f.filter(record) is True
    assert getattr(record, "gen_ai.conversation.id") == "conv-4"
    assert getattr(record, "user.id") == "u@x.io"


def test_baggage_filter_noop_outside_context():
    record = _record()
    BaggageLogFilter().filter(record)
    assert not hasattr(record, "gen_ai.conversation.id")
    assert not hasattr(record, "user.id")
