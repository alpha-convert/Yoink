"""Singleton stream operation - emits a single value then is done."""

from __future__ import annotations

import ast
from typing import List

from python_delta.stream_ops.base import StreamOp, DONE


class SingletonOp(StreamOp):
    """Stream operation that emits a single Python value then is done."""

    def __init__(self, value, stream_type):
        super().__init__(stream_type)
        self.value = value
        self.exhausted = False

    @property
    def id(self):
        return hash(("SingletonOp", id(self.value), self.stream_type))

    @property
    def vars(self):
        return set()  # No input streams, so no vars

    def _pull(self):
        if self.exhausted:
            return DONE
        self.exhausted = True
        from python_delta.event import BaseEvent
        return BaseEvent(self.value)

    def reset(self):
        self.exhausted = False

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize exhausted to False."""
        exhausted_var = ctx.state_var(self, 'exhausted')
        return [(exhausted_var.name, False)]