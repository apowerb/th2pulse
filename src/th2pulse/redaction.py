"""Scrub sensitive content from exported log records.

The filter is meant for the OTel export handler only: attach it there and
the console keeps the original line while what leaves the process is
scrubbed. (Python passes the same record object to every handler, so
attach the export handler *last* — which is what
:func:`th2pulse.otel.init_observability` does.)
"""

from __future__ import annotations

import logging
import re
from typing import Iterable


class RedactionFilter(logging.Filter):
    """Replace regex matches with a placeholder before export.

    Example::

        RedactionFilter([r"Bearer [A-Za-z0-9._-]+", r"api[_-]?key=\\S+"])
    """

    def __init__(
        self, patterns: Iterable[str], replacement: str = "[REDACTED]"
    ) -> None:
        super().__init__()
        self._compiled = [re.compile(p) for p in patterns]
        self._replacement = replacement

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            message = record.getMessage()
            scrubbed = message
            for rx in self._compiled:
                scrubbed = rx.sub(self._replacement, scrubbed)
            if scrubbed != message:
                record.msg = scrubbed
                record.args = ()
        except Exception:  # noqa: BLE001 - a broken filter must not drop logs
            pass
        return True
