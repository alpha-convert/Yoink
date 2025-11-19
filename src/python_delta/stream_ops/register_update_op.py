"""RegisterUpdateOp - Updates a RegisterBuffer with values from a stream."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar
from python_delta.typecheck.types import TyEps


class RegisterUpdateOp(StreamOp):
    """
    Updates a RegisterBuffer with a constant
    """
    def __init__(self, update_val , register_buffer):
        super().__init__(TyEps())
        self.update_val = update_val
        self.register_buffer = register_buffer

    @property
    def id(self):
        return hash(("RegisterUpdateOp", self.update_val, id(self.register_buffer)))

    @property
    def vars(self):
        return {}

    def _pull(self):
        self.register_buffer.update_value(self.update_val)
        return DONE

    def reset(self):
        pass
