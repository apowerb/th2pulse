# th2pulse

A lightweight OpenTelemetry collection and observability library for th2
applications (th2agent, th2llm, th2etl).

## Why

Google ADK (>= 1.36) wires its own OpenTelemetry providers from environment
variables and exports rich agent telemetry out of the box: hierarchical
spans (`invoke_agent → call_llm → generate_content`, `execute_tool`), five
native agent metrics, and GenAI event logs — every span tagged with
`gen_ai.conversation.id` and `user.id`.

What ADK does **not** do (measured on a real service): bridge standard
Python `logging` records — FastAPI, application code, integrations — to
the collector. `th2pulse` closes that gap with one call, and keeps the
whole wiring consistent across th2 services.

## Quickstart

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4318"   # HTTP port!
export OTEL_SERVICE_NAME="my-service"
```

```python
import th2pulse

th2pulse.init_observability("my-service")
```

That's it. Every `logging` record at `INFO`+ now reaches the collector,
correlated with active traces (`trace_id` is injected automatically).

> **Note** — the bridge respects your logging configuration: a record only
> reaches handlers if its *logger* lets it through. An unconfigured root
> logger defaults to `WARNING`, so make sure your app sets its levels
> (`logging.basicConfig(level=logging.INFO)` or equivalent) — th2 services
> already do.

### In an ADK app

Call after the ADK app is built so the bridge reuses the provider ADK
installed (ADK's own telemetry is already flowing at that point):

```python
app = get_fast_api_app(...)
th2pulse.init_observability("th2agent")
```

### In a plain FastAPI service / worker / script

`init_observability` installs the logs provider itself. For short-lived
scripts, flush before exiting:

```python
th2pulse.init_observability("my-batch")
...
th2pulse.force_flush()
```

### Redacting sensitive content

```python
th2pulse.init_observability(
    "my-service",
    redaction_patterns=[r"Bearer [A-Za-z0-9._-]+", r"api[_-]?key=\S+"],
)
```

Scrubbing applies to what leaves the process; console output is untouched.
Prompt/response content is never captured by default (OTel GenAI redaction).

### Correlating non-ADK telemetry with a conversation

```python
from th2pulse import conversation_context

with conversation_context(session_id, user_id=email):
    ...  # spans/logs emitted here join the conversation
```

## Environment variables

| Variable | Purpose |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector base endpoint — use the **HTTP** port (4318), not gRPC (4317) |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | Logs-specific override (full URL) |
| `OTEL_SERVICE_NAME` | Service identity on every signal |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource tags (`env=dev,...`) |

## Guarantees

* **Opt-in**: without an endpoint, `init_observability` is a no-op.
* **Never breaks the host app**: every entry point is best-effort — on
  failure it warns and returns `False`.
* **Idempotent**: repeated calls attach a single handler.
* **No secrets, no prompts**: nothing sensitive is captured by default.

## Development

```bash
uv sync
uv run pytest
```

## Roadmap

* Traces/metrics bootstrap for non-ADK services (FastAPI instrumentation).
* OTLP ingest receiver → PostgreSQL (backing the frontend monitoring API).
* Structured redaction presets shared across th2 services.
