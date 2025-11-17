"""ResetOp StreamOp - reset a set of nodes."""

from __future__ import annotations

from python_delta.stream_ops.base import StreamOp, DONE


class ResetOp(StreamOp):
    def __init__(self, reset_set, enclosing_block , stream_type):
        super().__init__(stream_type)
        self.reset_set = reset_set
        self.enclosing_block = enclosing_block

    @property
    def id(self):
        return hash(("ResetOp", *map(lambda n: id(n),self.reset_set)))

    @property
    def vars(self):
        return set()

    def _pull(self):
        for node in self.reset_set:
            node.reset()
        return None

    def reset(self):
        pass

