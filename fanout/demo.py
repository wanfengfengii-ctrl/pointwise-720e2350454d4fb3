"""Fast/slow dual-stream demo for fanout.Hub (stdlib only).

Run from the repository root:  python3 -m fanout.demo

One paced source is read exactly once.  A fast subscriber and a slow
subscriber each receive the full chunk sequence; the fast one finishes long
before the slow one starts making progress, while chunks retained for the
slow stream spill to the tempdir once they exceed the memory budget.  The
demo verifies identical output, a single source read, and complete cleanup
of hub-created temp files (a pre-existing file is left untouched).
"""

import asyncio
import os
import tempfile

from . import Hub
from .testing import ControlledSource

CHUNKS = 10
CHUNK_SIZE = 48
MEMORY_BYTES = 128          # room for ~2 chunks; the rest must spill
SOURCE_DELAY = 0.06         # acquisition pace of the source
SLOW_DELAY = 0.25           # per-chunk processing time of the slow stream


async def main():
    tmp = tempfile.mkdtemp(prefix="fanout-demo-")
    preexisting = os.path.join(tmp, "preexisting.txt")
    with open(preexisting, "w", encoding="utf-8") as fh:
        fh.write("do not delete")

    chunks = [b"<%02d>" % i + bytes([65 + i]) * (CHUNK_SIZE - 4)
              for i in range(CHUNKS)]
    src = ControlledSource(chunks, delay=SOURCE_DELAY)
    hub = Hub(src, subscribers=2, memory_bytes=MEMORY_BYTES, tempdir=tmp)
    fast, slow = hub.stream(0), hub.stream(1)

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    stats = {"peak_spilled": 0, "peak_cache": 0}

    def log(msg):
        print("[%5.2fs] %s" % (loop.time() - t0, msg))

    def state():
        stats["peak_spilled"] = max(stats["peak_spilled"], len(hub.spilled_files))
        stats["peak_cache"] = max(stats["peak_cache"], hub.cached_payload_bytes)
        return "cache=%dB buffered=%d spilled=%d" % (
            hub.cached_payload_bytes, hub.buffered_chunks, len(hub.spilled_files))

    async def run_fast():
        got = []
        async for chunk in fast:
            got.append(chunk)
            log("fast <- chunk#%02d (%2dB)  %s" % (len(got) - 1, len(chunk), state()))
        log("fast done")
        return got

    async def run_slow():
        got = []
        async for chunk in slow:
            got.append(chunk)
            log("slow <- chunk#%02d (%2dB)  %s" % (len(got) - 1, len(chunk), state()))
            await asyncio.sleep(SLOW_DELAY)
        log("slow done")
        return got

    log("hub: subscribers=2 memory_bytes=%dB, %d chunks x %dB, tempdir=%s"
        % (MEMORY_BYTES, CHUNKS, CHUNK_SIZE, tmp))
    got_fast, got_slow = await asyncio.gather(run_fast(), run_slow())
    await hub.aclose()

    identical = got_fast == chunks and got_slow == chunks
    leftovers = sorted(os.listdir(tmp))
    log("verify: fast==slow==source -> %s" % identical)
    log("verify: source read %d chunks for 2 subscribers (single shared read), "
        "closed=%s close_calls=%d" % (src.reads, src.closed, src.close_calls))
    log("verify: peak cache=%dB (limit %dB), peak spilled files=%d"
        % (stats["peak_cache"], MEMORY_BYTES, stats["peak_spilled"]))
    log("verify: tempdir now holds %s (pre-existing file kept, spill files gone)"
        % leftovers)

    assert identical, "streams diverged from the source"
    assert src.reads == CHUNKS and src.closed and src.close_calls == 1
    assert stats["peak_cache"] <= MEMORY_BYTES + CHUNK_SIZE
    assert stats["peak_spilled"] > 0, "demo should have exercised spilling"
    assert leftovers == ["preexisting.txt"]
    print("DEMO OK")


if __name__ == "__main__":
    asyncio.run(main())
