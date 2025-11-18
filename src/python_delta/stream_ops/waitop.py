from __future__ import annotations

from typing import List
import ast

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

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """
        TODO: Compilation for WaitOp

        The compilation should:
        1. Store a buffer instance as state (initialized via make_typed_buffer(stream_type))
        2. On each pull:
           - If buffer.is_complete(): return DONE
           - Otherwise: pull from input_stream
           - If input is DONE: assert buffer.is_complete(), return DONE
           - If input is None: return None
           - Otherwise: buffer.poke_event(input), return None
        3. On reset: create new buffer via make_typed_buffer(stream_type)

        Challenge: Need to make make_typed_buffer and TypedBuffer classes available
        at runtime in the compiled code's namespace.
        """
        del ctx, dst  # Unused for now
        raise NotImplementedError("WaitOp compilation not yet implemented")

