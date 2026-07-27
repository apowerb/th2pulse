"""init_observability contract: opt-in, idempotent, never raises."""
import logging

import pytest

from th2pulse import init_observability
from th2pulse.otel import _reset_for_tests


def _otel_handlers() -> list[logging.Handler]:
    from opentelemetry.sdk._logs import LoggingHandler

    return [
        h for h in logging.getLogger().handlers if isinstance(h, LoggingHandler)
    ]


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for var in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_SERVICE_NAME",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1")
    _reset_for_tests()
    yield
    _reset_for_tests()


def test_noop_without_endpoint():
    assert init_observability("test-svc") is False
    assert _otel_handlers() == []


def test_bridge_attaches_once(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
    assert init_observability("test-svc") is True
    assert init_observability("test-svc") is True
    assert len(_otel_handlers()) == 1


def test_endpoint_param_used_when_env_absent(monkeypatch):
    assert (
        init_observability("test-svc", endpoint="http://127.0.0.1:1") is True
    )
    assert len(_otel_handlers()) == 1


def test_never_raises(monkeypatch):
    # A broken endpoint value must not raise at init (export is async).
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "not a url at all")
    result = init_observability("test-svc")
    assert result in (True, False)
