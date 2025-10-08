from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar
from python_delta.typecheck.types import TyEps

class WaitBuffer:
    def __init__(self):


class WaitOp(StreamOp):
    """WAIT - waits until an entire value has arrived, buffering it in"""
    def __init__(self, input_stream):
        super().__init__(TyEps)
        self.input_stream = input_stream
        self.buffer = []
        self.input_exhausted = False

    @property
    def id(self):
        return hash(("WaitOp", self.input_stream.id, str(self.stream_type)))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        if self.input_exhausted:
            return DONE
        v = self.input_stream._pull()
        if v is DONE:
            self.input_exhausted = True
            return DONE
        elif v is None:
            return None
        else:
            self.buffer.append(v)
            return None

    def reset(self):
        self.buffer = []
        self.input_exhausted = False