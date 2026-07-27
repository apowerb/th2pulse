"""Run the ingest service: ``python -m th2pulse.ingest``.

Environment: TH2PULSE_DB_DSN (required), TH2PULSE_DB_SCHEMA,
TH2PULSE_INGEST_HOST (default 127.0.0.1), TH2PULSE_INGEST_PORT (default 4319).
"""
import logging
import os

import uvicorn

from th2pulse.ingest.app import create_app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(
        create_app(),
        host=os.environ.get("TH2PULSE_INGEST_HOST", "127.0.0.1"),
        port=int(os.environ.get("TH2PULSE_INGEST_PORT", "4319")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
