"""ResetOp StreamOp - reset a set of nodes."""

from __future__ import annotations

from typing import List, Callable
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class ResetOp(StreamOp):
    def __init__(self, reset_set, stream_type):
        super().__init__(stream_type)
        self.reset_set = reset_set

    @property
    def id(self):
        return hash(("ResetOp", *map(lambda n: id(n),self.reset_set)))

    @property
    def vars(self):
        return set()

    def _pull(self):
        for node in self.reset_set:
            node.reset()
        return None

    def reset(self):
        pass

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile reset calls on all nodes in reset_set."""
        reset_stmts = []

        for node in self.reset_set:
            reset_stmts.extend(node._get_reset_stmts(ctx))

        reset_stmts.append(
            ast.Assign(
                targets=[dst.lvalue()],
                value=ast.Constant(value=None)
            )
        )

        return reset_stmts

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        reset_stmts = []
        for node in self.reset_set:
            reset_stmts.extend(node._get_reset_stmts(ctx))
        return reset_stmts + skip_cont

