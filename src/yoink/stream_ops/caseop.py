"""CaseOp StreamOp - case analysis on sum type."""

from __future__ import annotations

from typing import List
import ast

from yoink.stream_ops.base import StreamOp, DONE
from yoink.event import PlusPuncA, PlusPuncB


class CaseOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, input_stream, left_branch, right_branch, stream_type):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.branches = [left_branch,right_branch] # StreamOp that produces output
        self.active_branch = -1
        self.tag_read = False

    @property
    def id(self):
        return hash(("CaseOp", self.input_stream.id, self.branches[0].id, self.branches[1].id))

    @property
    def vars(self):
        return self.input_stream.vars | self.branches[0].vars | self.branches[1].vars

    def _pull(self):
        """Read tag and route to appropriate branch."""
        if not self.tag_read:
            tag = self.input_stream._pull()
            if tag is None:
                return None
            if tag is DONE:
                return DONE
            self.tag_read = True

            if isinstance(tag, PlusPuncA):
                self.active_branch = 0
            elif isinstance(tag, PlusPuncB):
                self.active_branch = 1
            else:
                raise RuntimeError(f"Expected PlusPuncA or PlusPuncB tag, got {tag}")
            return None

        if self.active_branch == -1:
            raise RuntimeError("CaseOp._pull() called before tag was read")
        return self.branches[self.active_branch]._pull()

    def reset(self):
        """Reset state and recursively reset branches."""
        self.tag_read = False
        self.active_branch = None

    def ensure_legal_recursion(self,is_in_tail : bool):
        self.input_stream.ensure_legal_recursion(is_in_tail = False)
        self.branches[0].ensure_legal_recursion(is_in_tail)
        self.branches[1].ensure_legal_recursion(is_in_tail)