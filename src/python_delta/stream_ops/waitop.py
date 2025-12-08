from __future__ import annotations

from typing import List
import ast

from python_delta.compilation.runtime import Runtime
from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar
from python_delta.stream_ops.typed_buffer import TypedBuffer, make_typed_buffer


class WaitOp(StreamOp):
    """WAIT - waits until an entire value has arrived, buffering it in"""
    def __init__(self, input_stream):
        super().__init__(input_stream.stream_type)
        self.input_stream = input_stream
        self.buffer = make_typed_buffer(input_stream.stream_type)

    @property
    def id(self):
        return hash(("WaitOp", self.input_stream.id, str(self.stream_type)))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        if self.buffer.is_complete():
            return DONE
        v = self.input_stream._pull()
        if v is DONE:
            assert self.buffer.is_complete()
            return DONE
        elif v is None:
            return None
        else:
            self.buffer.poke_event(v)
            return None

    def reset(self):
        self.buffer = make_typed_buffer(self.input_stream.stream_type)

    def ensure_legal_recursion(self,is_in_tail : bool):
        self.input_stream.ensure_legal_recursion(is_in_tail=False)