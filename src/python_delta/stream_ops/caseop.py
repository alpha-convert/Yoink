"""CaseOp StreamOp - case analysis on sum type."""

from __future__ import annotations

from typing import List, Callable
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.event import PlusPuncA, PlusPuncB


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

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize tag_read and active_branch."""
        tag_read_var = ctx.state_var(self, 'tag_read')
        active_branch_var = ctx.state_var(self, 'active_branch')
        return [
            (tag_read_var.name, False),
            (active_branch_var.name, -1)
        ]

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        tag_read_var = ctx.state_var(self, 'tag_read')
        active_branch_var = ctx.state_var(self, 'active_branch')

        def tag_yield_cont(tag_expr):
            return [
                tag_read_var.assign(ast.Constant(value=True)),
                ast.If(
                    test=ast.Call(
                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                        args=[tag_expr, ast.Name(id='PlusPuncA', ctx=ast.Load())],
                        keywords=[]
                    ),
                    body=[
                        active_branch_var.assign(ast.Constant(value=0))
                    ],
                    orelse=[
                        ast.If(
                            test=ast.Call(
                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                args=[tag_expr, ast.Name(id='PlusPuncB', ctx=ast.Load())],
                                keywords=[]
                            ),
                            body=[
                                active_branch_var.assign(ast.Constant(value=1))
                            ],
                            orelse=[
                                ast.Raise(
                                    exc=ast.Call(
                                        func=ast.Name(id='RuntimeError', ctx=ast.Load()),
                                        args=[
                                            ast.JoinedStr(values=[
                                                ast.Constant(value='Expected PlusPuncA or PlusPuncB tag, got '),
                                                ast.FormattedValue(value=tag_expr, conversion=-1, format_spec=None)
                                            ])
                                        ],
                                        keywords=[]
                                    ),
                                    cause=None
                                )
                            ]
                        )
                    ]
                )
            ] + skip_cont

        input_stmts = self.input_stream._compile_stmts_cps(ctx, done_cont, skip_cont, tag_yield_cont)

        branch0_stmts = self.branches[0]._compile_stmts_cps(ctx, done_cont, skip_cont, yield_cont)
        branch1_stmts = self.branches[1]._compile_stmts_cps(ctx, done_cont, skip_cont, yield_cont)

        return [
            ast.If(
                test=ast.UnaryOp(op=ast.Not(), operand=tag_read_var.rvalue()),
                body=input_stmts,
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
        """Reset tag_read and active_branch."""
        tag_read_var = ctx.state_var(self, 'tag_read')
        active_branch_var = ctx.state_var(self, 'active_branch')
        return [
            tag_read_var.assign(ast.Constant(value=False)),
            active_branch_var.assign(ast.Constant(value=-1))
        ]

    def _compile_stmts_generator(
        self,
        ctx,
        done_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        tag_var = ctx.allocate_temp()

        def input_yield_cont(tag_expr):
            # Read tag, route to appropriate branch
            branch0_stmts = self.branches[0]._compile_stmts_generator(ctx, done_cont, yield_cont)
            branch1_stmts = self.branches[1]._compile_stmts_generator(ctx, done_cont, yield_cont)

            return [
                tag_var.assign(tag_expr),
                ast.If(
                    test=ast.Call(
                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                        args=[tag_var.rvalue(), ast.Name(id='PlusPuncA', ctx=ast.Load())],
                        keywords=[]
                    ),
                    body=branch0_stmts,
                    orelse=[
                        ast.If(
                            test=ast.Call(
                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                args=[tag_var.rvalue(), ast.Name(id='PlusPuncB', ctx=ast.Load())],
                                keywords=[]
                            ),
                            body=branch1_stmts,
                            orelse=[
                                ast.Raise(
                                    exc=ast.Call(
                                        func=ast.Name(id='RuntimeError', ctx=ast.Load()),
                                        args=[
                                            ast.JoinedStr(values=[
                                                ast.Constant(value='Expected PlusPuncA or PlusPuncB tag, got '),
                                                ast.FormattedValue(value=tag_var.rvalue(), conversion=-1, format_spec=None)
                                            ])
                                        ],
                                        keywords=[]
                                    ),
                                    cause=None
                                )
                            ]
                        )
                    ]
                )
            ]

        return self.input_stream._compile_stmts_generator(ctx, done_cont, input_yield_cont)

