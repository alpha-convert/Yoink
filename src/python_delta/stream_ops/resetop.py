"""ResetOp StreamOp - reset a set of nodes."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class ResetOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
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

        # Generate reset statements for each node in the reset set
        # In compiled code, nodes don't exist as separate objects - their state is flattened
        # So we need to inline the reset logic from each node's_get_reset_stmts
        for node in self.reset_set:
            reset_stmts.extend(node._get_reset_stmts(ctx))

        # Set dst = None
        reset_stmts.append(
            ast.Assign(
                targets=[dst.store],
                value=ast.Constant(value=None)
            )
        )

        return reset_stmts

