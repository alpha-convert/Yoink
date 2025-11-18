"""Var StreamOp - input variable."""

from __future__ import annotations

from python_delta.stream_ops.base import StreamOp, DONE


class Var(StreamOp):
    def __init__(self, name, stream_type):
        super().__init__(stream_type)
        self.name = name
        self.source = None

    @property
    def id(self):
        return hash(("Var", self.name))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"Var({self.name}: {self.stream_type})"

    def _pull(self):
        """Pull from the source iterator."""
        if self.source is None:
            raise RuntimeError(f"Var '{self.name}' has no source bound")
        try:
            v = next(self.source)
            return v
        except StopIteration:
            return DONE

    def reset(self):
        pass
