"""Eps StreamOp - empty stream."""

from __future__ import annotations

from python_delta.stream_ops.base import StreamOp, DONE


class Eps(StreamOp):
    def __init__(self, stream_type):
        super().__init__(stream_type)

    @property
    def id(self):
        return hash(("Eps", id(self)))

    @property
    def vars(self):
        return set()

    def __str__(self):
        return f"Eps({self.stream_type})"

    def _pull(self):
        return DONE

    def reset(self):
        pass
