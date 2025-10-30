"""MuxOp StreamOp - multiplex between N input streams based on a selector."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class MuxOp(StreamOp):
    """Multiplex between N streams - can switch back and forth based on selector state."""
    def __init__(self, children, stream_type):
        super().__init__(stream_type)
        self.children = children  # List of StreamOp
        self.active_child = 0  # Index in [0, len(children))

    @property
    def id(self):
        return hash(("MuxOp", *[child.id for child in self.children]))

    @property
    def vars(self):
        result = set()
        for child in self.children:
            result |= child.vars
        return result

    def _pull(self):
        """Pull from the currently active child."""
        return self.children[self.active_child]._pull()

    def set_active_child(self, index):
        """Switch to pulling from a different child."""
        assert 0 <= index < len(self.children), f"Invalid child index {index}, must be in [0, {len(self.children)})"
        self.active_child = index

    def reset(self):
        """Reset state - go back to first child."""
        self.active_child = 0

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile to switch statement based on active_child."""
        state_var = ctx.state_var(self, 'active_child')

        # Build if/elif chain for each child
        cases = []
        for i, child in enumerate(self.children):
            child_stmts = child._compile_stmts(ctx, dst)
            condition = ast.Compare(
                left=state_var.rvalue(),
                ops=[ast.Eq()],
                comparators=[ast.Constant(value=i)]
            )
            if i == 0:
                # First case becomes the if
                pass
            else:
                # Will be added to elif chain
                pass
            cases.append((condition, child_stmts))

        # Build nested if/elif/else
        if len(cases) == 1:
            return cases[0][1]

        # Start with last case as else
        stmt = ast.If(
            test=cases[-1][0],
            body=cases[-1][1],
            orelse=[]
        )

        # Work backwards building elif chain
        for i in range(len(cases) - 2, -1, -1):
            stmt = ast.If(
                test=cases[i][0],
                body=cases[i][1],
                orelse=[stmt]
            )

        return [stmt]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize active_child to 0."""
        state_var = ctx.state_var(self, 'active_child')
        return [(state_var.name, 0)]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset active_child to 0."""
        state_var = ctx.state_var(self, 'active_child')
        return [
            ast.Assign(
                targets=[state_var.lvalue()],
                value=ast.Constant(value=0)
            )
        ]
