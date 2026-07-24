"""Selective masking: sensitive patterns out, everything else readable."""
from datetime import datetime, timezone

from th2pulse.ingest.parsing import LogRow, SpanRow
from th2pulse.masking import mask_log_rows, mask_span_rows, mask_text

TS = datetime(2026, 7, 23, tzinfo=timezone.utc)


def test_mask_text_presets():
    text = ("Contact jean.dupont@acme.fr ou +33 6 12 34 56 78, "
            "IBAN FR76 3000 6000 0112 3456 7890 189, "
            "carte 4970 1234 5678 9012, Bearer abc123def456ghi789, "
            "api_key=sk-superSecretValue42")
    masked = mask_text(text)
    assert "jean.dupont@acme.fr" not in masked
    assert "6 12 34 56 78" not in masked
    assert "3000 6000" not in masked
    assert "5678 9012" not in masked
    assert "abc123def456ghi789" not in masked
    assert "superSecretValue42" not in masked
    assert "[masked email]" in masked
    assert "[masked phone]" in masked
    assert "[masked iban]" in masked
    assert "Contact" in masked  # the readable text survives


def test_mask_text_leaves_normal_content_alone():
    text = "Bonjour Elom, je t'envoie ce petit message pour te saluer."
    assert mask_text(text) == text
    assert mask_text("") == ""


def test_mask_log_rows_and_span_attrs():
    row = LogRow(ts=TS, severity_num=9, severity="INFO", service="s",
                 trace_id="a" * 32, span_id="b" * 16,
                 body='{"content": "mail de test@x.io"}',
                 attributes={}, resource={})
    assert "test@x.io" not in mask_log_rows([row])[0].body

    span = SpanRow(ts=TS, duration_ms=1.0, name="execute_tool send_mail",
                   service="s", trace_id="a" * 32, span_id="c" * 16,
                   parent_span_id=None, status_code=None, status_message=None,
                   attributes={
                       "gcp.vertex.agent.tool_call_args":
                           '{"to": "elom.gnaglo@gmail.com", "subject": "Bonjour"}',
                       "gen_ai.tool.name": "send_mail",
                   })
    masked = mask_span_rows([span])[0]
    assert "elom.gnaglo@gmail.com" not in masked.attributes["gcp.vertex.agent.tool_call_args"]
    assert "Bonjour" in masked.attributes["gcp.vertex.agent.tool_call_args"]
    assert masked.attributes["gen_ai.tool.name"] == "send_mail"  # untouched


def test_masking_never_raises_on_weird_input():
    assert mask_text(None) is None  # type: ignore[arg-type]
