"""ResetOp StreamOp - reset a set of nodes."""

from __future__ import annotations

from python_delta.stream_ops.base import StreamOp, DONE


class ResetBlockEnclosingOp(StreamOp):
    def __init__(self, block_contents, stream_type):
        super().__init__(stream_type)
        self.block_contents = block_contents

    @property
    def id(self):
        return hash(("ResetBlockEnclosingOp", self.block_contents.id))

    @property
    def vars(self):
        return self.block_contents.vars

    def _pull(self):
        return self.block_contents._pull()

    def reset(self):
        pass

