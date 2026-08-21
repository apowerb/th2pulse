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

Bridged log records emitted inside the context also carry
`gen_ai.conversation.id` / `user.id` as OTLP **log attributes** (stamped
from baggage by `BaggageLogFilter`, wired automatically). A log store can
therefore filter application logs per conversation directly — no
conversation↔trace mapping required. Stores that normalize attribute
names (e.g. Loki structured metadata) expose them as
`gen_ai_conversation_id` / `user_id`.

## Ingest service (collector → PostgreSQL → query API)

`th2pulse.ingest` is the storage/query half of the pipeline: an OTLP/HTTP
receiver that persists log records and the conversation ↔ trace mapping in
PostgreSQL, then serves them per conversation to the monitoring frontend.

```
agents / services ──OTLP──▶ collector ──otlphttp (encoding: json)──▶ th2pulse.ingest ──▶ PostgreSQL
                                                                         │
                                                     front ◀── GET /logs?conversation_id=...
```

Point the collector at it:

```yaml
exporters:
  otlphttp/pulse:
    endpoint: http://127.0.0.1:4319
    encoding: json          # required — the ingest speaks OTLP/JSON only
service:
  pipelines:
    logs:   { receivers: [otlp], processors: [batch], exporters: [otlphttp/pulse] }
    traces: { receivers: [otlp], processors: [batch], exporters: [otlphttp/pulse] }
```

Run it:

```bash
pip install "th2pulse[ingest]"
export TH2PULSE_DB_DSN="postgresql://user:pass@host:5432/db?sslmode=require"
export TH2PULSE_DB_SCHEMA="my_schema"        # optional, created if absent
python -m th2pulse.ingest                     # 127.0.0.1:4319
```

Endpoints:

| Route | Purpose |
|---|---|
| `POST /v1/logs`, `/v1/traces` | OTLP receivers (traces feed the conversation map **and** span storage) |
| `POST /v1/metrics` | Accepted and dropped (metrics stay collector-side for now) |
| `GET /logs?conversation_id=&service=&level=&since=&limit=&user_id=` | Log records, newest first |
| `GET /spans?conversation_id=&limit=&user_id=` | Spans: tool executions (name, args, response), LLM calls, durations |
| `GET /conversations?user_id=` | Known conversations with their trace ids |
| `GET /healthz` | Liveness |

Writes are transactional per payload (a collector retry after a failure
cannot duplicate committed rows) and span inserts are idempotent
(`ON CONFLICT DO NOTHING`), so replaying an OTLP export is safe.
`user_id` narrows every query to one user's conversations — callers doing
authorization (e.g. a frontend proxy) should inject it server-side from a
verified identity, never from client input. Set `TH2PULSE_INGEST_TOKEN`
and the POST endpoints require a matching `X-Th2Pulse-Token` header;
`TH2PULSE_QUERY_TOKEN` does the same for the read side. Payloads are
capped (10 MB raw, 32 MB decompressed).

Both tokens are optional *while the bind stays on loopback*, where the
socket is the boundary. On any other bind address they are not: the
service refuses to start with `TH2PULSE_INGEST_HOST` set to something
reachable and either token missing, rather than quietly serving
conversation logs and recorded tool arguments to whoever reaches the
port. Set `TH2PULSE_ALLOW_UNAUTHENTICATED=1` to state that authorization
is handled upstream (a reverse proxy, a private network).

### Docker

```bash
docker run -p 4319:4319 \
  -e TH2PULSE_DB_DSN="postgresql://user:pass@host:5432/db" \
  -e TH2PULSE_INGEST_TOKEN="..." -e TH2PULSE_QUERY_TOKEN="..." \
  apowerb/th2pulse:latest
```

The image binds `0.0.0.0:4319` — a container's loopback reaches nothing —
which is why the tokens above are not optional here. It installs the
published release matching its tag, so `apowerb/th2pulse:0.1.3` contains
th2pulse 0.1.3; it is not built from the working tree. To run a local
change, run the service directly instead:
`uv run --extra ingest python -m th2pulse.ingest`.

The conversation map is built from `gen_ai.conversation.id` / `user.id`
attributes — on spans (where ADK puts them natively) and on log records
(where `conversation_context` propagates them).

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
* Span storage + retention policies in the ingest service.
* Structured redaction presets shared across th2 services.
