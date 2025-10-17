"""CaseOp StreamOp - case analysis on sum type."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.stream_ops.bufferop import BufferOp
from python_delta.event import PlusPuncA, PlusPuncB, BaseEvent
from python_delta.compilation import StateVar


class CondOp(StreamOp):
    """Conditional on boolean values """
    def __init__(self, cond_stream, left_branch, right_branch, stream_type):
        super().__init__(stream_type)
        self.cond_stream = cond_stream
        self.branches = [left_branch,right_branch] # StreamOp that produces output
        self.active_branch = None

    @property
    def id(self):
        return hash(("CaseOp", self.cond_stream.id, self.branches[0].id, self.branches[1].id))

    @property
    def vars(self):
        return self.cond_stream.vars | self.branches[0].vars | self.branches[1].vars

    def _pull(self):
        """Read tag and route to appropriate branch."""
        if self.active_branch is None:
            b = self.cond_stream._pull()
            if b is None:
                return None
            if b is DONE:
                return DONE

            assert isinstance(b, BaseEvent)
            assert isinstance(b.value,bool)
            if b.value:
                self.active_branch = 0
            else:
                self.active_branch = 1
            return None
        else:
            return self.branches[self.active_branch]._pull()

    def reset(self):
        """Reset state and recursively reset branches."""
        self.active_branch = None