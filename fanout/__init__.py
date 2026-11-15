"""fanout — share one non-replayable async byte stream with many consumers.

``Hub(source, subscribers, memory_bytes, tempdir)`` reads ``source`` (an async
iterator of ``bytes``) exactly once and hands out ``subscribers`` independent
views of it.  ``hub.stream(i)`` claims the i-th view (each index can be claimed
at most once) and returns an async iterator with these guarantees:

* every stream sees every chunk, in order, starting from the first chunk;
* chunk boundaries and empty chunks (``b""``) are preserved exactly;
* all streams share a single read of the source — each source chunk is
  fetched once, no matter how many subscribers consume it;
* each stream advances on demand: a fast stream never waits for a slow one.

Chunks that have not yet been consumed by every *live* stream are retained.
Retained payload is kept in memory only up to ``memory_bytes`` bytes plus the
single most recently fetched source chunk; anything beyond that is spilled to
files created inside ``tempdir``.  Chunks consumed by all live streams are
released immediately (their spill files are deleted).

Lifecycle rules:

* closing a stream releases its unread entitlement; the other streams are
  unaffected and stay complete;
* when the last stream closes, the hub closes and awaits the source and
  deletes every temp file it created (pre-existing files in ``tempdir`` are
  never touched);
* normal end-of-source or a source exception is observed by each stream
  separately, only after it has read the prefix it is entitled to; a source
  exception propagates with its original type and message and is never
  swallowed as a normal end;
* a stream must not run two ``next`` calls concurrently (``RuntimeError``);
  calling ``next`` on a closed stream ends immediately; cancelling a pending
  ``next`` counts as closing that stream;
* ``await hub.aclose()`` closes all streams; every release operation is safe
  to repeat.

``subscribers`` and ``memory_bytes`` must be positive integers; an
out-of-range or repeatedly claimed stream index raises ``ValueError``.
"""

import asyncio
import os
import tempfile
from collections import deque

__all__ = ["Hub", "Stream", "__version__"]

__version__ = "1.0.0"


class _Chunk:
    """One retained source chunk.

    The payload lives either in memory (``data``) or in a spill file inside
    the hub's tempdir (``path``).  ``length`` is the payload length in bytes.
    """

    __slots__ = ("data", "path", "length")

    def __init__(self, data, path, length):
        self.data = data
        self.path = path
        self.length = length


class Hub:
    """Fan-out dispatcher for one single-pass async byte stream."""

    def __init__(self, source, subscribers, memory_bytes, tempdir):
        if isinstance(subscribers, bool) or not isinstance(subscribers, int) \
                or subscribers <= 0:
            raise ValueError("subscribers must be a positive integer")
        if isinstance(memory_bytes, bool) or not isinstance(memory_bytes, int) \
                or memory_bytes <= 0:
            raise ValueError("memory_bytes must be a positive integer")
        self._source = source
        self._aiter = source.__aiter__()
        self._n = subscribers
        self._mem_limit = memory_bytes
        self._tempdir = os.fspath(tempdir)

        # Retained chunks; absolute index of self._chunks[0] is self._base.
        self._chunks = deque()
        self._base = 0
        self._mem_bytes = 0          # payload bytes currently held in memory

        self._ended = False          # source raised StopAsyncIteration
        self._error = None           # source raised an exception
        self._lock = asyncio.Lock()  # serializes source reads and source close

        self._route_live = [True] * subscribers   # entitlement not released
        self._cursors = [0] * subscribers         # next chunk index per route
        self._claimed = [False] * subscribers     # stream(i) already handed out
        self._live_count = subscribers

        self._files = set()          # spill files created by this hub
        self._close_task = None      # asyncio.Task closing the source
        self._finalize_task = None   # asyncio.Task running final cleanup
        self._closed = False         # aclose() was called

    # ------------------------------------------------------------------
    # introspection helpers (used by the demo and the verification suite)
    # ------------------------------------------------------------------
    @property
    def subscribers(self):
        return self._n

    @property
    def memory_bytes(self):
        return self._mem_limit

    @property
    def cached_payload_bytes(self):
        """Payload bytes of retained chunks currently held in memory."""
        return self._mem_bytes

    @property
    def buffered_chunks(self):
        """Number of source chunks currently retained for live streams."""
        return len(self._chunks)

    @property
    def spilled_files(self):
        """Spill files currently present in the tempdir (created by this hub)."""
        return tuple(sorted(self._files))

    @property
    def closed(self):
        return self._closed

    # ------------------------------------------------------------------
    # claiming streams
    # ------------------------------------------------------------------
    def stream(self, i):
        """Claim the i-th stream.  Each index can be claimed only once."""
        if isinstance(i, bool) or not isinstance(i, int):
            raise ValueError("stream index must be an integer")
        if i < 0 or i >= self._n:
            raise ValueError("stream index %r out of range [0, %d)" % (i, self._n))
        if self._claimed[i]:
            raise ValueError("stream %d already claimed" % i)
        if not self._route_live[i]:
            raise ValueError("stream %d is already closed" % i)
        self._claimed[i] = True
        return Stream(self, i)

    # ------------------------------------------------------------------
    # retention: append, memory enforcement, spill, load, garbage collect
    # ------------------------------------------------------------------
    def _append(self, data):
        self._chunks.append(_Chunk(data, None, len(data)))
        self._mem_bytes += len(data)
        # The freshly fetched source chunk is the "current" chunk: it may
        # push the cache to at most memory_bytes + len(current).  Enforce
        # the bound by spilling the oldest in-memory chunks to disk.
        self._enforce_memory(protect=self._base + len(self._chunks) - 1)

    def _enforce_memory(self, protect):
        if self._mem_bytes <= self._mem_limit:
            return
        end = self._base + len(self._chunks)
        idx = self._base
        while self._mem_bytes > self._mem_limit and idx < end:
            chunk = self._chunks[idx - self._base]
            if idx != protect and chunk.data is not None and chunk.length:
                self._spill(idx, chunk)
            idx += 1
        # If only the protected (current) chunk remains in memory the cache
        # may still exceed memory_bytes; that is the allowed "+ one current
        # source chunk" headroom.

    def _spill(self, idx, chunk):
        os.makedirs(self._tempdir, exist_ok=True)
        fd, path = tempfile.mkstemp(prefix="fanout-", suffix=".chunk",
                                    dir=self._tempdir)
        self._files.add(path)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(chunk.data)
        except BaseException:
            self._discard_file(path)
            raise
        chunk.path = path
        chunk.data = None
        self._mem_bytes -= chunk.length

    def _load(self, idx):
        chunk = self._chunks[idx - self._base]
        if chunk.data is not None:
            return chunk.data
        with open(chunk.path, "rb") as fh:
            return fh.read()

    def _discard_file(self, path):
        self._files.discard(path)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _gc(self):
        """Release chunks no live stream will read any more."""
        if self._live_count:
            limit = min(self._cursors[i] for i in range(self._n)
                        if self._route_live[i])
        else:
            limit = self._base + len(self._chunks)
        while self._chunks and self._base < limit:
            chunk = self._chunks.popleft()
            self._base += 1
            if chunk.data is not None:
                self._mem_bytes -= chunk.length
            if chunk.path is not None:
                self._discard_file(chunk.path)

    # ------------------------------------------------------------------
    # source reads (single shared read, serialized by self._lock)
    # ------------------------------------------------------------------
    async def _fetch(self, pos):
        """Fetch one source chunk, unless someone else already did.

        Only called when ``pos`` is beyond the retained window.  Records
        normal end / source exceptions instead of raising them, so every
        stream observes termination only after draining its own prefix.
        """
        terminate = False
        async with self._lock:
            if pos < self._base + len(self._chunks):
                return  # another stream fetched it while we waited
            if self._ended or self._error is not None:
                return  # another stream already observed termination
            try:
                data = await self._aiter.__anext__()
                if type(data) is not bytes:
                    data = bytes(data)  # normalize bytes-like payloads
            except StopAsyncIteration:
                self._ended = True
                terminate = True
            except Exception as exc:
                self._error = exc
                terminate = True
            else:
                self._append(data)
        if terminate:
            # The source is finished; close it outside the read lock.
            await self._close_source()

    # ------------------------------------------------------------------
    # shutdown
    # ------------------------------------------------------------------
    async def _release_route(self, i):
        """Release one stream's unread entitlement (idempotent)."""
        if not self._route_live[i]:
            return
        self._route_live[i] = False
        self._live_count -= 1
        self._gc()
        if self._live_count == 0:
            await self._finalize()

    async def _do_close_source(self):
        # Wait for any in-flight source read to finish, then close.
        async with self._lock:
            pass
        aclose = getattr(self._aiter, "aclose", None)
        if aclose is None and self._aiter is not self._source:
            aclose = getattr(self._source, "aclose", None)
        if aclose is not None:
            await aclose()

    async def _close_source(self):
        if self._close_task is None:
            self._close_task = asyncio.get_running_loop().create_task(
                self._do_close_source())
        # Shielded: a cancelled waiter must not abort the shared close.
        await asyncio.shield(self._close_task)

    def _cleanup_files(self):
        """Delete every remaining spill file this hub created (idempotent)."""
        for path in list(self._files):
            try:
                self._discard_file(path)
            except OSError:
                pass

    async def _do_finalize(self):
        try:
            await self._close_source()
        finally:
            self._cleanup_files()

    async def _finalize(self):
        if self._finalize_task is None:
            self._finalize_task = asyncio.get_running_loop().create_task(
                self._do_finalize())
        await asyncio.shield(self._finalize_task)

    async def aclose(self):
        """Close all streams, close and await the source, remove temp files.

        Safe to call any number of times.
        """
        self._closed = True
        for i in range(self._n):
            if self._route_live[i]:
                self._route_live[i] = False
                self._live_count -= 1
        self._gc()
        await self._finalize()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()
        return False


class Stream:
    """One claimed view of a :class:`Hub`; an async iterator of ``bytes``."""

    def __init__(self, hub, index):
        self._hub = hub
        self._i = index
        self._in_next = False

    def __aiter__(self):
        return self

    @property
    def index(self):
        return self._i

    @property
    def closed(self):
        return not self._hub._route_live[self._i]

    async def __anext__(self):
        hub = self._hub
        if not hub._route_live[self._i]:
            raise StopAsyncIteration  # closed stream ends immediately
        if self._in_next:
            raise RuntimeError(
                "concurrent __anext__ on the same stream is not allowed")
        self._in_next = True
        try:
            return await self._next()
        except asyncio.CancelledError:
            # Cancelling a pending next counts as closing this stream.
            await self.aclose()
            raise
        finally:
            self._in_next = False

    async def _next(self):
        hub = self._hub
        i = self._i
        while True:
            if not hub._route_live[i]:
                raise StopAsyncIteration
            pos = hub._cursors[i]
            if pos < hub._base + len(hub._chunks):
                data = hub._load(pos)
                hub._cursors[i] = pos + 1
                hub._gc()
                return data
            if hub._error is not None:
                # The source failed; deliver the original exception only
                # after this stream has drained its entitled prefix.
                error = hub._error
                await self.aclose()
                raise error
            if hub._ended:
                await self.aclose()
                raise StopAsyncIteration
            await hub._fetch(pos)

    async def aclose(self):
        """Close this stream and release its unread entitlement (idempotent)."""
        await self._hub._release_route(self._i)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()
        return False
