"""CaseOp StreamOp - case analysis on sum type."""

from __future__ import annotations

from typing import List, Callable
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

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile boolean condition reading and branch routing."""
        active_branch_var = ctx.state_var(self, 'active_branch')

        cond_tmp = ctx.allocate_temp()
        cond_stmts = self.cond_stream._compile_stmts(ctx, cond_tmp)

        branch0_stmts = self.branches[0]._compile_stmts(ctx, dst)
        branch1_stmts = self.branches[1]._compile_stmts(ctx, dst)

        # Build nested if structure for condition reading
        return [
            ast.If(
                test=ast.Compare(
                    left=active_branch_var.rvalue(),
                    ops=[ast.Is()],
                    comparators=[ast.Constant(value=None)]
                ),
                body=cond_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=cond_tmp.rvalue(),
                            ops=[ast.Is()],
                            comparators=[ast.Constant(value=None)]
                        ),
                        body=[
                            dst.assign(ast.Constant(value=None))
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=cond_tmp.rvalue(),
                                    ops=[ast.Is()],
                                    comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                ),
                                body=[
                                    dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                                ],
                                orelse=[
                                    # Extract boolean value and set active_branch
                                    ast.If(
                                        test=ast.Attribute(
                                            value=cond_tmp.rvalue(),
                                            attr='value',
                                            ctx=ast.Load()
                                        ),
                                        body=[
                                            active_branch_var.assign(ast.Constant(value=0))
                                        ],
                                        orelse=[
                                            active_branch_var.assign(ast.Constant(value=1))
                                        ]
                                    ),
                                    # Set dst = None
                                    dst.assign(ast.Constant(value=None))
                                ]
                            )
                        ]
                    )
                ],
                orelse=[
                    # Route to appropriate branch
                    ast.If(
                        test=ast.Compare(
                            left=active_branch_var.rvalue(),
                            ops=[ast.Eq()],
                            comparators=[ast.Constant(value=0)]
                        ),
                        body=branch0_stmts,
                        orelse=branch1_stmts
                    )
                ]
            )
        ]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize active_branch."""
        active_branch_var = ctx.state_var(self, 'active_branch')
        return [
            (active_branch_var.name, None)
        ]

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        active_branch_var = ctx.state_var(self, 'active_branch')

        def cond_yield_cont(cond_expr):
            return [
                ast.If(
                    test=ast.Attribute(value=cond_expr, attr='value', ctx=ast.Load()),
                    body=[active_branch_var.assign(ast.Constant(value=0))],
                    orelse=[active_branch_var.assign(ast.Constant(value=1))]
                )
            ] + skip_cont

        cond_stmts = self.cond_stream._compile_stmts_cps(ctx, done_cont, skip_cont, cond_yield_cont)

        branch0_stmts = self.branches[0]._compile_stmts_cps(ctx, done_cont, skip_cont, yield_cont)
        branch1_stmts = self.branches[1]._compile_stmts_cps(ctx, done_cont, skip_cont, yield_cont)

        return [
            ast.If(
                test=ast.Compare(
                    left=active_branch_var.rvalue(),
                    ops=[ast.Is()],
                    comparators=[ast.Constant(value=None)]
                ),
                body=cond_stmts,
                orelse=[
                    ast.If(
                        test=ast.Compare(
                            left=active_branch_var.rvalue(),
                            ops=[ast.Eq()],
                            comparators=[ast.Constant(value=0)]
                        ),
                        body=branch0_stmts,
                        orelse=branch1_stmts
                    )
                ]
            )
        ]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset active_branch."""
        active_branch_var = ctx.state_var(self, 'active_branch')
        return [
            active_branch_var.assign(ast.Constant(value=None))
        ]