"""The unauthenticated endpoints must not become reachable by accident.

With neither token set, every endpoint accepts any caller that reaches the
socket. That is the documented trade for the default loopback bind. These
tests pin the boundary: the moment the bind address is reachable from
elsewhere, startup refuses instead of exposing the read side silently.
"""
import pytest

from th2pulse.ingest.__main__ import _check_reachable_without_a_token, _is_loopback

_TOKENS = ("TH2PULSE_INGEST_TOKEN", "TH2PULSE_QUERY_TOKEN")


@pytest.fixture(autouse=True)
def _no_ambient_tokens(monkeypatch):
    for name in (*_TOKENS, "TH2PULSE_ALLOW_UNAUTHENTICATED"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.168.1.9", "th2pulse"])
def test_refuses_a_reachable_bind_with_no_token(host):
    with pytest.raises(SystemExit) as excinfo:
        _check_reachable_without_a_token(host)
    message = str(excinfo.value)
    # The message has to name the way out, not just the refusal.
    assert host in message
    assert "TH2PULSE_INGEST_TOKEN" in message
    assert "TH2PULSE_ALLOW_UNAUTHENTICATED" in message


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "127.0.0.53"])
def test_allows_a_loopback_bind_with_no_token(host):
    # Negative control: the guard must not fire on the default the library
    # ships, or every existing local run would start failing.
    _check_reachable_without_a_token(host)


def test_allows_a_reachable_bind_once_both_tokens_are_set(monkeypatch):
    for name in _TOKENS:
        monkeypatch.setenv(name, "s3cret")
    _check_reachable_without_a_token("0.0.0.0")


def test_one_token_is_not_enough(monkeypatch):
    monkeypatch.setenv("TH2PULSE_INGEST_TOKEN", "s3cret")
    with pytest.raises(SystemExit) as excinfo:
        _check_reachable_without_a_token("0.0.0.0")
    # Only the missing one is named -- an operator reading this should not
    # go looking for a variable they already set.
    assert "TH2PULSE_QUERY_TOKEN" in str(excinfo.value)
    assert "TH2PULSE_INGEST_TOKEN" not in str(excinfo.value).split("Set them")[0].replace(
        "TH2PULSE_QUERY_TOKEN", ""
    )


def test_explicit_opt_out_is_honoured(monkeypatch):
    monkeypatch.setenv("TH2PULSE_ALLOW_UNAUTHENTICATED", "1")
    _check_reachable_without_a_token("0.0.0.0")


@pytest.mark.parametrize("value", ["0", "true", "yes", ""])
def test_only_the_exact_opt_out_value_counts(monkeypatch, value):
    # A half-set variable must not read as consent.
    monkeypatch.setenv("TH2PULSE_ALLOW_UNAUTHENTICATED", value)
    with pytest.raises(SystemExit):
        _check_reachable_without_a_token("0.0.0.0")


def test_unparseable_host_is_treated_as_reachable():
    assert _is_loopback("localhost") is False
    assert _is_loopback("") is False
