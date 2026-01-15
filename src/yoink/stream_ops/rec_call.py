"""RecCall StreamOp - recursive call that resets a set of nodes."""

from __future__ import annotations

from yoink.stream_ops.base import StreamOp, DONE


class RecCall(StreamOp):
    def __init__(self, reset_set, enclosing_block,stream_type, unsafe = False):
        super().__init__(stream_type)
        self.reset_set = reset_set
        self.enclosing_block = enclosing_block
        self.unsafe = unsafe # Bypass the safe recursion checker

    @property
    def id(self):
        return hash(("RecCall", *map(lambda n: id(n),self.reset_set)))

    @property
    def vars(self):
        return set()

    def _pull(self):
        for node in self.reset_set:
            node.reset()
        return None

    def reset(self):
        pass

    def ensure_legal_recursion(self, is_in_tail):
        assert (is_in_tail or self.unsafe)

