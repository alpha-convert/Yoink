"""SumInj StreamOp - inject stream into sum type."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.event import PlusPuncA, PlusPuncB


class SumInj(StreamOp):
    """Sum injection - emits PlusPuncA (position=0) or PlusPuncB (position=1) tag followed by input stream values."""
    def __init__(self, input_stream, stream_type, position):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.position = position  # 0 for left (PlusPuncA), 1 for right (PlusPuncB)
        self.tag_emitted = False

    @property
    def id(self):
        return hash(("SumInj", self.input_stream.id, self.position))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        """Emit tag first (PlusPuncA if position=0, PlusPuncB if position=1), then pull from input stream."""
        if not self.tag_emitted:
            self.tag_emitted = True
            return PlusPuncA() if self.position == 0 else PlusPuncB()
        return self.input_stream._pull()

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.tag_emitted = False

    def ensure_legal_recursion(self,is_in_tail : bool):
        self.input_stream.ensure_legal_recursion(is_in_tail)