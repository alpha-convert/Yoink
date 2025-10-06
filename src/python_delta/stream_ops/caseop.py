"""CaseOp StreamOp - case analysis on sum type."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.event import PlusPuncA, PlusPuncB
from python_delta.compilation import StateVar


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

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile tag reading and branch routing."""
        tag_read_var = ctx.allocate_state(self, 'tag_read')
        active_branch_var = ctx.allocate_state(self, 'active_branch')

        tag_tmp = ctx.allocate_temp()
        input_stmts = self.input_stream._compile_stmts(ctx, tag_tmp)

        branch0_stmts = self.branches[0]._compile_stmts(ctx, dst)
        branch1_stmts = self.branches[1]._compile_stmts(ctx, dst)

        # Build nested if/elif structure for tag reading
        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=tag_read_var.attr_load
                ),
                body=input_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=tag_tmp.load,
                            ops=[ast.Is()],
                            comparators=[ast.Constant(value=None)]
                        ),
                        body=[
                            ast.Assign(
                                targets=[dst.store],
                                value=ast.Constant(value=None)
                            )
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=tag_tmp.load,
                                    ops=[ast.Is()],
                                    comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                ),
                                body=[
                                    ast.Assign(
                                        targets=[dst.store],
                                        value=ast.Name(id='DONE', ctx=ast.Load())
                                    )
                                ],
                                orelse=[
                                    # Set tag_read = True
                                    ast.Assign(
                                        targets=[tag_read_var.attr_store],
                                        value=ast.Constant(value=True)
                                    ),
                                    # Check tag type and set active_branch
                                    ast.If(
                                        test=ast.Call(
                                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                                            args=[
                                                tag_tmp.load,
                                                ast.Name(id='PlusPuncA', ctx=ast.Load())
                                            ],
                                            keywords=[]
                                        ),
                                        body=[
                                            ast.Assign(
                                                targets=[active_branch_var.attr_store],
                                                value=ast.Constant(value=0)
                                            )
                                        ],
                                        orelse=[
                                            ast.If(
                                                test=ast.Call(
                                                    func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                    args=[
                                                        tag_tmp.load,
                                                        ast.Name(id='PlusPuncB', ctx=ast.Load())
                                                    ],
                                                    keywords=[]
                                                ),
                                                body=[
                                                    ast.Assign(
                                                        targets=[active_branch_var.attr_store],
                                                        value=ast.Constant(value=1)
                                                    )
                                                ],
                                                orelse=[
                                                    ast.Raise(
                                                        exc=ast.Call(
                                                            func=ast.Name(id='RuntimeError', ctx=ast.Load()),
                                                            args=[
                                                                ast.JoinedStr(values=[
                                                                    ast.Constant(value='Expected PlusPuncA or PlusPuncB tag, got '),
                                                                    ast.FormattedValue(
                                                                        value=tag_tmp.load,
                                                                        conversion=-1,
                                                                        format_spec=None
                                                                    )
                                                                ])
                                                            ],
                                                            keywords=[]
                                                        ),
                                                        cause=None
                                                    )
                                                ]
                                            )
                                        ]
                                    ),
                                    # Set dst = None
                                    ast.Assign(
                                        targets=[dst.store],
                                        value=ast.Constant(value=None)
                                    )
                                ]
                            )
                        ]
                    )
                ],
                orelse=[
                    # Route to appropriate branch
                    ast.If(
                        test=ast.Compare(
                            left=active_branch_var.attr_load,
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
        """Initialize tag_read and active_branch."""
        tag_read_var = ctx.get_state_var(self, 'tag_read')
        active_branch_var = ctx.get_state_var(self, 'active_branch')
        return [
            (tag_read_var.name, False),
            (active_branch_var.name, -1)
        ]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset tag_read and active_branch."""
        tag_read_var = ctx.get_state_var(self, 'tag_read')
        active_branch_var = ctx.get_state_var(self, 'active_branch')
        return [
            ast.Assign(
                targets=[tag_read_var.attr_store],
                value=ast.Constant(value=False)
            ),
            ast.Assign(
                targets=[active_branch_var.attr_store],
                value=ast.Constant(value=-1)
            )
        ]

