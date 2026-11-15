"""Verification suite for fanout.Hub (stdlib only).

Run from the repository root:  python3 -m fanout.selftest

Covers: chunk fidelity (boundaries + empty chunks), single shared source
read, on-demand fast/slow progress, disk-spill byte-for-byte comparison
against the source payload, the memory watermark bound, entitlement release
on close, source close-and-await on last close, source error type/message
propagation after the entitled prefix, cancellation-closes-stream,
concurrent-next rejection, closed-stream semantics, ValueError contract,
and repeatable resource release with tempdir hygiene.
"""

import asyncio
import os
import tempfile
import traceback

from . import Hub
from .testing import ControlledSource, make_chunks

_TESTS = []


def test(fn):
    _TESTS.append(fn)
    return fn


async def collect(stream):
    out = []
    async for chunk in stream:
        out.append(chunk)
    return out


def expect_value_error(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except ValueError:
        return
    raise AssertionError("expected ValueError from %r" % (fn,))


class Boom(Exception):
    """Distinct exception type used to check source-error propagation."""


# ---------------------------------------------------------------------
@test
async def test_basic_fanout_preserves_chunks_and_empties():
    sizes = [5, 0, 9, 1, 0, 0, 17, 3]
    chunks = make_chunks(sizes)
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 3, memory_bytes=16, tempdir=tmp)
        got = await asyncio.gather(*[collect(hub.stream(i)) for i in range(3)])
        for g in got:
            assert g == chunks, "stream diverged from source chunks"
        assert src.reads == len(chunks), "source must be read exactly once"
        await hub.aclose()
        assert os.listdir(tmp) == [], "temp files must be removed"


@test
async def test_value_error_contract():
    src = ControlledSource([b"x"])
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 2, 8, tmp)
        hub.stream(0)
        expect_value_error(hub.stream, 0)      # duplicate claim
        expect_value_error(hub.stream, 2)      # out of range
        expect_value_error(hub.stream, -1)     # out of range
        expect_value_error(hub.stream, True)   # not a valid index
        expect_value_error(hub.stream, "0")    # not an integer
        expect_value_error(Hub, ControlledSource([b"x"]), 0, 8, tmp)
        expect_value_error(Hub, ControlledSource([b"x"]), -2, 8, tmp)
        expect_value_error(Hub, ControlledSource([b"x"]), 1, 0, tmp)
        expect_value_error(Hub, ControlledSource([b"x"]), 1, -8, tmp)
        await hub.aclose()
        # claiming after the hub is closed is rejected as well
        expect_value_error(hub.stream, 1)


@test
async def test_disk_spill_matches_source_bytes():
    """Disk cross-check: spilled files must equal the source payload."""
    sizes = [10, 0, 25, 7, 0, 13, 30, 3, 11, 0, 6]
    chunks = make_chunks(sizes)
    limit = 30
    with tempfile.TemporaryDirectory() as tmp:
        sentinel = os.path.join(tmp, "sentinel.bin")
        with open(sentinel, "wb") as fh:
            fh.write(b"original-file")
        src = ControlledSource(chunks)
        hub = Hub(src, 2, memory_bytes=limit, tempdir=tmp)
        fast, slow = hub.stream(0), hub.stream(1)

        got_fast = []
        async for chunk in fast:
            got_fast.append(chunk)
            # memory watermark: never above memory_bytes once the current
            # chunk has been accounted for (all chunks here fit the limit)
            assert hub.cached_payload_bytes <= limit
        assert got_fast == chunks

        # The slow stream has not consumed anything: everything is retained.
        assert hub.buffered_chunks == len(chunks)
        assert hub.spilled_files, "expected some chunks to be spilled"

        # --- disk comparison: every retained chunk, in memory or on disk,
        #     must match the source bytes exactly, in order.
        for off, entry in enumerate(hub._chunks):
            expect = chunks[hub._base + off]
            if entry.path is None:
                assert entry.data == expect
            else:
                assert entry.data is None
                assert os.path.dirname(entry.path) == tmp
                assert os.path.basename(entry.path).startswith("fanout-")
                with open(entry.path, "rb") as fh:
                    assert fh.read() == expect, "spilled chunk %d mismatch" % off
        on_disk = sum(os.path.getsize(p) for p in hub.spilled_files)
        retained = sum(len(c) for c in chunks)
        assert on_disk + hub.cached_payload_bytes == retained
        assert hub.cached_payload_bytes <= limit

        # The slow stream still receives the complete sequence (from disk).
        assert await collect(slow) == chunks

        await hub.aclose()
        await hub.aclose()  # repeatable
        assert src.closed and src.close_calls == 1
        assert src.reads == len(chunks)
        # tempdir hygiene: only the pre-existing file remains, untouched.
        assert sorted(os.listdir(tmp)) == ["sentinel.bin"]
        with open(sentinel, "rb") as fh:
            assert fh.read() == b"original-file"


@test
async def test_oversized_chunk_uses_current_chunk_headroom():
    chunks = make_chunks([10, 100, 10])
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 2, memory_bytes=16, tempdir=tmp)
        s0, s1 = hub.stream(0), hub.stream(1)
        assert await collect(s0) == chunks
        # The 100-byte chunk exceeded memory_bytes alone: it was kept only
        # as the current source chunk and spilled once the next chunk came.
        entries = list(hub._chunks)
        assert entries[1].path is not None, "oversized chunk must be spilled"
        assert hub.cached_payload_bytes <= 16 + 100
        assert await collect(s1) == chunks
        await hub.aclose()
        assert os.listdir(tmp) == []


@test
async def test_close_releases_unread_entitlement():
    chunks = make_chunks([8, 8, 8, 8])
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 3, memory_bytes=1024, tempdir=tmp)
        s0, s1, s2 = hub.stream(0), hub.stream(1), hub.stream(2)
        assert await s0.__anext__() == chunks[0]
        assert await s1.__anext__() == chunks[0]
        assert hub.buffered_chunks == 1  # chunk0 still owed to s2
        await s2.aclose()                # releases s2's unread entitlement
        assert hub.buffered_chunks == 0  # chunk0 released at once
        # the remaining streams stay complete
        assert [chunks[0]] + await collect(s0) == chunks
        assert [chunks[0]] + await collect(s1) == chunks
        assert src.closed, "last close must close the source"
        await hub.aclose()


@test
async def test_last_close_closes_source_and_cleans_tempfiles():
    chunks = make_chunks([4, 4, 4, 4])
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 2, memory_bytes=4, tempdir=tmp)
        s0, s1 = hub.stream(0), hub.stream(1)
        assert await s0.__anext__() == chunks[0]
        assert await s0.__anext__() == chunks[1]  # forces chunk0 to spill
        assert len(hub.spilled_files) == 1
        assert not src.closed
        await s0.aclose()
        assert not src.closed, "source must stay open while a stream lives"
        await s1.aclose()  # last stream closes
        assert src.closed and src.close_calls == 1
        assert os.listdir(tmp) == [], "spill files must be deleted"
        await hub.aclose()  # repeatable


@test
async def test_async_generator_source_is_closed_and_awaited():
    finalized = []
    chunks = make_chunks([3, 3, 3])

    async def gen():
        try:
            for c in chunks:
                yield c
        finally:
            finalized.append(True)

    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(gen(), 2, memory_bytes=1024, tempdir=tmp)
        s0, s1 = hub.stream(0), hub.stream(1)
        assert await s0.__anext__() == chunks[0]
        await s0.aclose()
        assert finalized == [], "generator must not be finalized early"
        await s1.aclose()  # last close -> source closed and awaited
        assert finalized == [True]
        await hub.aclose()


@test
async def test_source_error_propagates_after_prefix():
    chunks = make_chunks([4, 4, 4])
    err = Boom("boom-123")
    src = ControlledSource(chunks, error=err, error_after=2)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 2, memory_bytes=1024, tempdir=tmp)
        s0, s1 = hub.stream(0), hub.stream(1)
        assert await s0.__anext__() == chunks[0]
        assert await s0.__anext__() == chunks[1]
        try:
            await s0.__anext__()
            raise AssertionError("source error must not be swallowed")
        except Boom as exc:
            assert exc is err and str(exc) == "boom-123"
        # s1 observes the same failure only after its own entitled prefix
        assert await s1.__anext__() == chunks[0]
        assert await s1.__anext__() == chunks[1]
        try:
            await s1.__anext__()
            raise AssertionError("source error must not become normal end")
        except Boom as exc:
            assert exc is err and str(exc) == "boom-123"
        # both streams are now closed; further next ends immediately
        try:
            await s0.__anext__()
            raise AssertionError("closed stream must end")
        except StopAsyncIteration:
            pass
        await hub.aclose()
        assert src.closed


@test
async def test_cancel_pending_next_closes_stream():
    chunks = make_chunks([5, 5, 5])
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 2, memory_bytes=1024, tempdir=tmp)
        s0, s1 = hub.stream(0), hub.stream(1)
        src.pause()  # hold the source mid-fetch
        task = asyncio.ensure_future(s0.__anext__())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
            raise AssertionError("expected CancelledError")
        except asyncio.CancelledError:
            pass
        assert s0.closed, "cancelling a next must close the stream"
        try:
            await s0.__anext__()
            raise AssertionError("closed stream must end immediately")
        except StopAsyncIteration:
            pass
        src.resume()
        # the other stream is unaffected and loses nothing
        assert await collect(s1) == chunks
        assert src.reads == len(chunks), "cancelled read must not consume"
        await hub.aclose()


@test
async def test_concurrent_next_same_stream_rejected():
    chunks = make_chunks([5, 5])
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 1, memory_bytes=1024, tempdir=tmp)
        s = hub.stream(0)
        src.pause()
        task = asyncio.ensure_future(s.__anext__())
        await asyncio.sleep(0.05)
        try:
            await s.__anext__()
            raise AssertionError("expected RuntimeError")
        except RuntimeError:
            pass
        src.resume()
        assert await task == chunks[0]
        assert await collect(s) == chunks[1:]
        await hub.aclose()


@test
async def test_concurrent_streams_no_loss_no_duplication():
    sizes = [(i * 37) % 61 for i in range(60)]  # mixed sizes incl. empties
    chunks = make_chunks(sizes)
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 5, memory_bytes=64, tempdir=tmp)

        async def worker(i, delay):
            stream = hub.stream(i)
            out = []
            async for chunk in stream:
                out.append(chunk)
                if delay:
                    await asyncio.sleep(delay)
            return out

        delays = [0, 0.001, 0.002, 0.003, 0.005]
        got = await asyncio.gather(*[worker(i, d) for i, d in enumerate(delays)])
        for g in got:
            assert g == chunks, "lost or duplicated chunks under concurrency"
        assert src.reads == len(chunks), "source must be read exactly once"
        await hub.aclose()
        assert os.listdir(tmp) == []


@test
async def test_repeatable_release():
    src = ControlledSource(make_chunks([4, 4]))
    with tempfile.TemporaryDirectory() as tmp:
        hub = Hub(src, 2, memory_bytes=4, tempdir=tmp)
        s0 = hub.stream(0)
        await s0.aclose()
        await s0.aclose()   # repeatable
        await hub.aclose()
        await hub.aclose()  # repeatable
        assert src.closed and src.close_calls == 1
        assert os.listdir(tmp) == []


@test
async def test_async_context_manager():
    chunks = make_chunks([2, 2])
    src = ControlledSource(chunks)
    with tempfile.TemporaryDirectory() as tmp:
        async with Hub(src, 1, memory_bytes=8, tempdir=tmp) as hub:
            assert await collect(hub.stream(0)) == chunks
        assert src.closed


def main():
    failures = 0
    for fn in _TESTS:
        try:
            asyncio.run(fn())
        except BaseException as exc:  # noqa: BLE001 - report and continue
            failures += 1
            print("FAIL %s: %r" % (fn.__name__, exc))
            traceback.print_exc()
        else:
            print("PASS %s" % fn.__name__)
    print("\n%d/%d tests passed" % (len(_TESTS) - failures, len(_TESTS)))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
