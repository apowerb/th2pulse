"""The ingest must create the schema it was told to use.

``search_path`` selects a schema, it does not create one. Pointing
TH2PULSE_DB_SCHEMA at a schema that does not exist yet used to fail startup
with "no schema has been selected to create in" -- which reads like a typo
rather than the missing CREATE it is. That is the normal case, not an edge
one: deploying this service beside an application in a shared database means
giving it a schema of its own.

Gated by ``TH2PULSE_TEST_DSN`` like the other PostgreSQL tests.
"""
import asyncio
import os

import pytest

asyncpg = pytest.importorskip("asyncpg")

from th2pulse.ingest.store import Store  # noqa: E402

DSN = os.environ.get("TH2PULSE_TEST_DSN")


@pytest.mark.parametrize(
    "schema",
    ["9lives", "has-a-dash", "has space", 'quote"injection',
     "th2pulse; DROP TABLE pulse_logs"],
)
def test_a_schema_name_that_is_not_an_identifier_is_refused(schema):
    # Interpolated into DDL, so this is the boundary that keeps it safe.
    # No database needed: the refusal happens before any connection.
    with pytest.raises(ValueError) as excinfo:
        Store("postgresql://unused@127.0.0.1:5432/unused", schema=schema)
    assert "TH2PULSE_DB_SCHEMA" in str(excinfo.value)


@pytest.mark.parametrize("schema", ["th2pulse", "_private", "Pulse2"])
def test_a_plain_identifier_is_accepted(schema):
    # Negative control: the guard must not reject the names people actually
    # use, or nothing would start.
    Store("postgresql://unused@127.0.0.1:5432/unused", schema=schema)


@pytest.mark.skipif(not DSN, reason="TH2PULSE_TEST_DSN not set")
def test_connect_creates_a_schema_that_does_not_exist_yet():
    schema = "pulse_created_by_test"

    async def scenario():
        admin = await asyncpg.connect(DSN)
        try:
            await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            existed = await admin.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name = $1)", schema)
            assert existed is False, "precondition: the schema must be absent"

            store = Store(DSN, schema=schema)
            await store.connect()
            await store.close()

            created = await admin.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name = $1)", schema)
            # And the tables landed in it, not next door in public.
            table = await admin.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = 'pulse_spans')",
                schema)
            await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await admin.close()
        return created, table

    created, table = asyncio.run(scenario())
    assert created is True
    assert table is True


@pytest.mark.skipif(not DSN, reason="TH2PULSE_TEST_DSN not set")
def test_connecting_twice_to_the_same_schema_is_fine():
    schema = "pulse_created_twice"

    async def scenario():
        admin = await asyncpg.connect(DSN)
        try:
            await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            for _ in range(2):
                store = Store(DSN, schema=schema)
                await store.connect()
                await store.close()
            await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await admin.close()

    asyncio.run(scenario())
