"""Run the ingest service: ``python -m th2pulse.ingest``.

Environment: TH2PULSE_DB_DSN (required), TH2PULSE_DB_SCHEMA,
TH2PULSE_INGEST_HOST (default 127.0.0.1), TH2PULSE_INGEST_PORT (default 4319).
"""
import ipaddress
import logging
import os

import uvicorn

from th2pulse.ingest.app import create_app

_UNAUTHENTICATED_OPT_OUT = "TH2PULSE_ALLOW_UNAUTHENTICATED"


def _is_loopback(host: str) -> bool:
    """True when binding ``host`` only exposes the socket to this machine.

    Anything that does not parse as an address (a hostname, or the empty
    string) is treated as non-loopback: the safe answer when we cannot tell
    is the one that asks the operator to be explicit.
    """
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _check_reachable_without_a_token(host: str) -> None:
    """Refuse to expose unauthenticated endpoints beyond this machine.

    The tokens are documented as hardening *on top of* the default
    localhost-only bind: with neither set, every endpoint accepts any
    caller that reaches the socket -- including the read side, which serves
    conversation logs and recorded tool arguments and responses. That trade
    is defensible while the socket is loopback-only. It stops being
    defensible the moment the bind address is reachable from elsewhere,
    which is exactly what a container image has to do to be useful.

    So the combination refuses to start rather than starting quietly. Set
    the tokens, or set TH2PULSE_ALLOW_UNAUTHENTICATED=1 to state that
    something else (a reverse proxy, a private network) is doing the
    authorization.
    """
    if _is_loopback(host):
        return
    if os.environ.get(_UNAUTHENTICATED_OPT_OUT) == "1":
        return
    missing = [
        name
        for name in ("TH2PULSE_INGEST_TOKEN", "TH2PULSE_QUERY_TOKEN")
        if not os.environ.get(name)
    ]
    if not missing:
        return
    raise SystemExit(
        f"Refusing to start: TH2PULSE_INGEST_HOST={host} is reachable from "
        f"outside this machine, and {' and '.join(missing)} "
        f"{'is' if len(missing) == 1 else 'are'} not set, so those endpoints "
        "would accept any caller that can reach the port. Set them, or set "
        f"{_UNAUTHENTICATED_OPT_OUT}=1 if authorization is handled upstream."
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    host = os.environ.get("TH2PULSE_INGEST_HOST", "127.0.0.1")
    _check_reachable_without_a_token(host)
    uvicorn.run(
        create_app(),
        host=host,
        port=int(os.environ.get("TH2PULSE_INGEST_PORT", "4319")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
