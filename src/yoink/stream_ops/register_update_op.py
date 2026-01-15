"""RegisterUpdateOp - Updates a RegisterBuffer with values from a stream."""

from __future__ import annotations

from typing import List
import ast

from yoink.stream_ops.base import StreamOp, DONE
from yoink.compilation import StateVar
from yoink.typecheck.types import TyEps


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

    def ensure_legal_recursion(self,is_in_tail : bool):
        pass