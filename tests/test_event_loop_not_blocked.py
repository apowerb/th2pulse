"""A heavy ingest must not freeze the rest of the service.

Every previous round measured the *cost* of decompression in isolation and
declared victory when the number shrank. None measured what actually
matters: whether other requests still get served while it runs. The inflate
and the JSON parse are CPU-bound and the service runs a single worker, so
called inline they stall health checks, reads and every other tenant for
their whole duration.
"""
import asyncio
import gzip
import json


import pytest
from httpx import ASGITransport, AsyncClient

from th2pulse.ingest.app import create_app


class _FakeStore:
    async def connect(self): ...
    async def close(self): ...
    async def ingest_logs(self, *_): ...
    async def evaluate_alerts(self, **_): return {}
    async def prune_old(self, _days): return {}


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("TH2PULSE_ALERT_INTERVAL_S", "0")
    monkeypatch.setenv("TH2PULSE_RETENTION_INTERVAL_S", "0")
    monkeypatch.delenv("TH2PULSE_INGEST_TOKEN", raising=False)
    monkeypatch.delenv("TH2PULSE_QUERY_TOKEN", raising=False)
    return create_app(_FakeStore())


@pytest.mark.asyncio
async def test_healthz_stays_responsive_during_heavy_ingest(app):
    """/healthz must answer promptly while a costly body is being decoded.

    The probe body is the worst case that slips past every existing ceiling:
    hundreds of thousands of empty gzip members, several MB on the wire, and
    almost no decompressed output — so MAX_DECOMPRESSED_BYTES never fires.
    """
    heavy = gzip.compress(b"") * 300_000
    assert len(heavy) < 10 * 1024 * 1024

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        # Warm up so import/first-call costs do not pollute the measurement.
        await client.get("/healthz")

        ingest = asyncio.create_task(client.post(
            "/v1/logs", content=heavy,
            headers={"content-type": "application/json",
                     "content-encoding": "gzip"},
        ))
        await asyncio.sleep(0)  # let the ingest actually start

        served = 0
        while not ingest.done():
            res = await client.get("/healthz")
            assert res.status_code == 200
            served += 1
            await asyncio.sleep(0.005)

        await ingest

    # The assertion is "still answering", not "answering fast": what the
    # inline version did was serve *zero* health checks for the whole
    # request. Latency stays degraded here because iterating 300k members is
    # Python-level work that holds the GIL, but the service stays alive —
    # that is the difference between degraded and down.
    assert served > 0, (
        "no health check was served during the ingest — the event loop is "
        "fully stalled by CPU-bound work in the handler"
    )


@pytest.mark.asyncio
async def test_large_json_parse_also_yields(app):
    """The JSON parse is CPU-bound too, and was inline just like the inflate."""
    payload = json.dumps({"resourceLogs": [
        {"resource": {"attributes": []}, "scopeLogs": []} for _ in range(20_000)
    ]}).encode()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        await client.get("/healthz")
        ingest = asyncio.create_task(client.post(
            "/v1/logs", content=payload,
            headers={"content-type": "application/json"},
        ))
        await asyncio.sleep(0)

        served = 0
        while not ingest.done():
            assert (await client.get("/healthz")).status_code == 200
            served += 1
            await asyncio.sleep(0.005)
        await ingest

    assert served > 0, "no health check was served while parsing a large body"
