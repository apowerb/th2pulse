"""OTLP/HTTP ingest service: collector -> PostgreSQL -> query API.

Receives OTLP JSON payloads from an OpenTelemetry collector's ``otlphttp``
exporter (configured with ``encoding: json``), stores log records and the
conversation <-> trace mapping in PostgreSQL, and exposes a small query API
for the monitoring frontend.
"""
from th2pulse.ingest.app import create_app

__all__ = ["create_app"]
