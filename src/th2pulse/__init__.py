"""th2pulse — lightweight OpenTelemetry collection for th2 applications.

One call wires a th2 service to an OpenTelemetry collector:

    import th2pulse
    th2pulse.init_observability("my-service")

See :func:`th2pulse.otel.init_observability` for the full contract.
"""

from th2pulse.correlation import BaggageLogFilter, conversation_context
from th2pulse.otel import force_flush, init_observability

__version__ = "0.1.0"
__all__ = [
    "init_observability",
    "force_flush",
    "conversation_context",
    "BaggageLogFilter",
]
