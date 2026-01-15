"""RecursiveSection StreamOp - marks a recursive section with reset capability."""

from __future__ import annotations

from yoink.stream_ops.base import StreamOp, DONE


class RecursiveSection(StreamOp):
    def __init__(self, block_contents, stream_type):
        super().__init__(stream_type)
        self.block_contents = block_contents

    @property
    def id(self):
        return hash(("RecursiveSection", self.block_contents.id))

    @property
    def vars(self):
        return self.block_contents.vars

    def _pull(self):
        return self.block_contents._pull()

    def reset(self):
        pass

    def ensure_legal_recursion(self,is_in_tail : bool):
        self.block_contents.ensure_legal_recursion(is_in_tail = True)