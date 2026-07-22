"""RedactionFilter scrubs exported records without ever dropping them."""
import logging

import pytest

from th2pulse.redaction import RedactionFilter


def _record(msg: str, *args) -> logging.LogRecord:
    return logging.LogRecord(
        name="t", level=logging.INFO, pathname=__file__, lineno=1,
        msg=msg, args=args, exc_info=None,
    )


def test_scrubs_matches():
    f = RedactionFilter([r"Bearer [A-Za-z0-9._-]+"])
    record = _record("auth header: Bearer abc.def-123")
    assert f.filter(record) is True
    assert record.getMessage() == "auth header: [REDACTED]"


def test_scrubs_formatted_args():
    f = RedactionFilter([r"secret-\d+"])
    record = _record("value=%s", "secret-42")
    f.filter(record)
    assert "secret-42" not in record.getMessage()


def test_untouched_when_no_match():
    f = RedactionFilter([r"nope"])
    record = _record("hello %s", "world")
    f.filter(record)
    assert record.getMessage() == "hello world"


def test_invalid_pattern_raises_at_construction():
    import re

    with pytest.raises(re.error):
        RedactionFilter([r"("])


def test_never_drops_records():
    ok = RedactionFilter([r"x"])
    record = _record("plain")
    record.msg = object()  # getMessage() will fail
    assert ok.filter(record) is True
