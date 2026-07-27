"""_gunzip_bounded must match gzip.decompress byte for byte, and stay bounded.

The bounded inflate exists to stop a decompression bomb from exhausting
memory. A first attempt at it silently truncated payloads over 1 MB; a second
one looped forever on multi-member streams — a worse denial of service than
the bomb it replaced. Hence a differential test against the stdlib rather
than hand-picked assertions.
"""
import gzip
import random
import time
import zlib

import pytest
from fastapi import HTTPException

from th2pulse.ingest.app import (
    MAX_DECOMPRESSED_BYTES,
    _GUNZIP_CHUNK,
    _gunzip_bounded,
)


def _payloads():
    rnd = random.Random(1234)
    yield "vide", b""
    yield "court", b'{"resourceLogs":[]}'
    yield "un chunk pile", b"A" * _GUNZIP_CHUNK
    yield "chunk + 1", b"A" * (_GUNZIP_CHUNK + 1)
    yield "multi chunks", b"A" * (_GUNZIP_CHUNK * 3 + 17)
    yield "incompressible", bytes(rnd.getrandbits(8) for _ in range(200_000))
    yield "texte repetitif", (b"log line 42 " * 50_000)
    for size in (1, 7, 1023, 65_536, 1_000_000):
        yield f"aleatoire {size}", bytes(rnd.getrandbits(8) for _ in range(size))


@pytest.mark.parametrize("name,raw", list(_payloads()), ids=lambda v: v if isinstance(v, str) else "")
def test_matches_stdlib(name, raw):
    assert _gunzip_bounded(gzip.compress(raw)) == raw


def test_multi_member_small():
    """Concatenated members: all of them must come out."""
    raw = [b"premier " * 500, b"deuxieme " * 500, b"troisieme " * 500]
    stream = b"".join(gzip.compress(part) for part in raw)
    assert _gunzip_bounded(stream) == gzip.decompress(stream) == b"".join(raw)


def test_multi_member_first_exceeds_chunk():
    """The case that looped forever: member 1 larger than the slice size."""
    stream = (gzip.compress(b"A" * int(_GUNZIP_CHUNK * 1.5))
              + gzip.compress(b"B" * 100))
    assert _gunzip_bounded(stream) == gzip.decompress(stream)


def test_many_members():
    stream = b"".join(gzip.compress(f"m{i}".encode() * 1000) for i in range(25))
    assert _gunzip_bounded(stream) == gzip.decompress(stream)


def test_hundreds_of_thousands_of_members_stay_cheap():
    """Member count must cost linear time, not quadratic.

    A body of 300k empty members fits well under MAX_BODY_BYTES yet produces
    almost no output, so no size ceiling can catch it. A previous
    implementation recopied the remaining input per member and took ~15s on
    this input, blocking the whole event loop.
    """
    stream = gzip.compress(b"") * 300_000
    assert len(stream) < 10 * 1024 * 1024, "probe must stay under the body cap"
    start = time.perf_counter()
    out = _gunzip_bounded(stream)
    elapsed = time.perf_counter() - start
    assert out == b""
    assert elapsed < 5.0, f"300k members took {elapsed:.1f}s — quadratic again?"


def test_bomb_is_rejected_with_bounded_memory():
    bomb = gzip.compress(b"\0" * (200 * 1024 * 1024))
    with pytest.raises(HTTPException) as exc:
        _gunzip_bounded(bomb)
    assert exc.value.status_code == 413


def test_just_under_ceiling_survives_intact():
    raw = b"A" * (MAX_DECOMPRESSED_BYTES - 1024)
    assert _gunzip_bounded(gzip.compress(raw)) == raw


def test_just_over_ceiling_is_rejected():
    raw = b"A" * (MAX_DECOMPRESSED_BYTES + 1024)
    with pytest.raises(HTTPException) as exc:
        _gunzip_bounded(gzip.compress(raw))
    assert exc.value.status_code == 413


@pytest.mark.parametrize("body", [b"not gzip at all", b"\x1f\x8b" + b"\x00" * 20])
def test_invalid_stream_is_rejected(body):
    with pytest.raises(HTTPException) as exc:
        _gunzip_bounded(body)
    assert exc.value.status_code == 400


def test_truncated_stream_is_rejected():
    full = gzip.compress(b"payload " * 10_000)
    with pytest.raises(HTTPException) as exc:
        _gunzip_bounded(full[: len(full) // 2])
    assert exc.value.status_code == 400


def test_trailing_garbage_behaves_like_stdlib():
    stream = gzip.compress(b"hello") + b"garbage"
    try:
        expected = gzip.decompress(stream)
    except (OSError, zlib.error, EOFError):
        with pytest.raises(HTTPException):
            _gunzip_bounded(stream)
    else:
        assert _gunzip_bounded(stream) == expected
