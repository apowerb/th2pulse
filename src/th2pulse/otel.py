"""OpenTelemetry bootstrap for th2 services.

Encapsulates the wiring pitfalls measured while instrumenting th2agent:

* The exporter used here speaks **OTLP/HTTP** — point
  ``OTEL_EXPORTER_OTLP_ENDPOINT`` at the collector's HTTP port (``4318`` by
  default), not the gRPC one (``4317``). Pointing at the gRPC port fails
  with a silent ``Connection reset`` on the telemetry path.
* Google ADK >= 1.36 sets up its own OTel providers at server startup when
  ``OTEL_EXPORTER_OTLP_*`` env vars are present — but it only exports its
  own GenAI events (spans, metrics, redacted event logs). Standard Python
  ``logging`` records (FastAPI, application code) are **not** bridged.
  :func:`init_observability` closes exactly that gap, and reuses ADK's
  provider when one is already installed instead of fighting it.
* Prompt/response content is never captured by default (OTel GenAI
  redaction applies). :class:`th2pulse.redaction.RedactionFilter` adds an
  extra scrubbing hook for application logs.

Design rule: observability must never take the host application down.
Every entry point is best-effort — on failure it warns and returns
``False``; the application continues untouched.
"""

from __future__ import annotations

import logging
import os
from typing import Sequence

_LOG = logging.getLogger(__name__)

# Module-level state so repeated calls stay idempotent.
_state: dict = {"handler": None}


def _endpoint_from_env() -> str | None:
    return os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") or os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT"
    )


def init_observability(
    service_name: str | None = None,
    *,
    endpoint: str | None = None,
    level: int = logging.INFO,
    redaction_patterns: Sequence[str] | None = None,
) -> bool:
    """Bridge Python ``logging`` to an OTLP collector. Best-effort.

    Args:
        service_name: sets ``OTEL_SERVICE_NAME`` if not already set.
        endpoint: OTLP/HTTP base endpoint (e.g. ``http://127.0.0.1:4318``).
            Falls back to ``OTEL_EXPORTER_OTLP_ENDPOINT`` /
            ``OTEL_EXPORTER_OTLP_LOGS_ENDPOINT``. Explicit env vars win.
        level: minimum level exported (default ``INFO``).
        redaction_patterns: regexes scrubbed from exported records
            (console output is untouched).

    Returns:
        ``True`` when the bridge is active (or already was), ``False``
        when disabled (no endpoint) or on failure. Never raises.

    Notes:
        * Without an endpoint this is a **no-op**: observability is
          strictly opt-in and adds zero overhead when unconfigured.
        * For ADK apps, call this *after* ``get_fast_api_app()`` so the
          handler reuses the provider ADK installed. Calling earlier also
          works — both sides export to the same endpoint.
    """
    try:
        return _init(service_name, endpoint, level, redaction_patterns)
    except Exception as exc:  # noqa: BLE001 - never take the host app down
        _LOG.warning("th2pulse: observability init failed (app unaffected): %s", exc)
        return False


def _init(
    service_name: str | None,
    endpoint: str | None,
    level: int,
    redaction_patterns: Sequence[str] | None,
) -> bool:
    if endpoint:
        os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    if service_name:
        os.environ.setdefault("OTEL_SERVICE_NAME", service_name)
    if not _endpoint_from_env():
        _LOG.info("th2pulse: no OTLP endpoint configured — observability disabled")
        return False
    if _state["handler"] is not None:
        return True

    from opentelemetry._logs import get_logger_provider, set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    provider = get_logger_provider()
    if not hasattr(provider, "add_log_record_processor"):
        # No SDK provider installed yet (plain FastAPI service, worker,
        # script). ADK apps >= 1.36 install one at server startup — in that
        # case the branch below is skipped and we reuse theirs.
        provider = LoggerProvider(resource=Resource.create())
        # The exporter resolves the endpoint from env per the OTLP spec
        # (generic endpoint + /v1/logs, or the logs-specific override).
        provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter())
        )
        set_logger_provider(provider)

    handler = LoggingHandler(level=level, logger_provider=provider)
    from th2pulse.correlation import BaggageLogFilter

    handler.addFilter(BaggageLogFilter())
    if redaction_patterns:
        from th2pulse.redaction import RedactionFilter

        handler.addFilter(RedactionFilter(redaction_patterns))
    logging.getLogger().addHandler(handler)
    _state["handler"] = handler
    _LOG.info("th2pulse: logging bridge active (service=%s)", os.getenv("OTEL_SERVICE_NAME"))
    return True


def force_flush(timeout_millis: int = 5000) -> None:
    """Flush pending telemetry batches. Call before a script exits.

    Long-running services do not need this — the batch processor exports
    continuously and flushes at interpreter shutdown.
    """
    try:
        from opentelemetry._logs import get_logger_provider

        provider = get_logger_provider()
        flush = getattr(provider, "force_flush", None)
        if flush:
            flush(timeout_millis)
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("th2pulse: force_flush failed: %s", exc)


def _reset_for_tests() -> None:
    """Detach the bridge handler (test helper — not public API).

    The global LoggerProvider cannot be uninstalled once set (OTel
    forbids overriding); resetting the handler is enough for idempotence
    tests because a reused provider never gets a second processor.
    """
    handler = _state["handler"]
    if handler is not None:
        logging.getLogger().removeHandler(handler)
        _state["handler"] = None
