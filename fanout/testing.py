"""Controllable async byte sources for tests and demos (stdlib only)."""

import asyncio

__all__ = ["ControlledSource", "make_chunks"]


class ControlledSource:
    """A controllable async ``bytes`` iterator.

    * serves ``chunks`` in order, each exactly once (a single pass);
    * optional per-chunk ``delay`` simulates a slow acquisition stream;
    * ``pause()`` / ``resume()`` gate the next read, so tests can hold the
      source in the middle of a fetch (e.g. to cancel a pending ``next``);
    * optional ``error`` is raised once ``error_after`` chunks have been
      served (defaults to replacing the normal end-of-stream);
    * tracks how many chunks were read and how often it was closed.

    Cancelling a pending read does not consume a chunk and leaves the source
    usable.
    """

    def __init__(self, chunks=(), *, delay=0.0, error=None, error_after=None):
        self._chunks = list(chunks)
        self._delay = delay
        self._error = error
        self._error_after = len(self._chunks) if error_after is None else error_after
        self.reads = 0            # chunks handed out so far
        self.closed = False       # aclose() was called
        self.close_calls = 0      # how many times aclose() ran
        self._gate = asyncio.Event()
        self._gate.set()

    def __aiter__(self):
        return self

    def pause(self):
        """Block subsequent reads until resume() is called."""
        self._gate.clear()

    def resume(self):
        self._gate.set()

    @property
    def pending(self):
        """Chunks not yet read."""
        return len(self._chunks) - self.reads

    async def __anext__(self):
        await self._gate.wait()
        if self._error is not None and self.reads >= self._error_after:
            raise self._error
        if self.reads >= len(self._chunks):
            raise StopAsyncIteration
        if self._delay:
            await asyncio.sleep(self._delay)
        index = self.reads
        self.reads += 1
        return self._chunks[index]

    async def aclose(self):
        self.closed = True
        self.close_calls += 1


def make_chunks(sizes):
    """Deterministic chunks with distinct content: chunk ``i`` is the byte
    ``65 + i % 26`` repeated ``sizes[i]`` times (size 0 yields ``b""``)."""
    return [bytes([65 + (i % 26)]) * n for i, n in enumerate(sizes)]
