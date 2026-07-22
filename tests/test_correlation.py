"""conversation_context: baggage + span attributes, cleanly detached."""
from opentelemetry import baggage
from opentelemetry.sdk.trace import TracerProvider

from th2pulse import conversation_context


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
