"""SumInj StreamOp - inject stream into sum type."""

from __future__ import annotations

from typing import List, Callable
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.event import PlusPuncA, PlusPuncB
from python_delta.compilation import StateVar


class SumInj(StreamOp):
    """Sum injection - emits PlusPuncA (position=0) or PlusPuncB (position=1) tag followed by input stream values."""
    def __init__(self, input_stream, stream_type, position):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.position = position  # 0 for left (PlusPuncA), 1 for right (PlusPuncB)
        self.tag_emitted = False

    @property
    def id(self):
        return hash(("SumInj", self.input_stream.id, self.position))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        """Emit tag first (PlusPuncA if position=0, PlusPuncB if position=1), then pull from input stream."""
        if not self.tag_emitted:
            self.tag_emitted = True
            return PlusPuncA() if self.position == 0 else PlusPuncB()
        return self.input_stream._pull()

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.tag_emitted = False

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile tag emission then delegation."""
        tag_var = ctx.state_var(self, 'tag_emitted')
        input_stmts = self.input_stream._compile_stmts(ctx, dst)

        tag_class = 'PlusPuncA' if self.position == 0 else 'PlusPuncB'

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=tag_var.rvalue()
                ),
                body=[
                    tag_var.assign(ast.Constant(value=True)),
                    dst.assign(ast.Call(
                        func=ast.Name(id=tag_class, ctx=ast.Load()),
                        args=[],
                        keywords=[]
                    ))
                ],
                orelse=input_stmts
            )
        ]

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        tag_var = ctx.state_var(self, 'tag_emitted')

        tag_class = 'PlusPuncA' if self.position == 0 else 'PlusPuncB'

        tag_event = ast.Call(
            func=ast.Name(id=tag_class, ctx=ast.Load()),
            args=[],
            keywords=[]
        )

        input_stmts = self.input_stream._compile_stmts_cps(ctx, done_cont, skip_cont, yield_cont)

        # If tag not emitted, emit it; otherwise delegate to input
        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=tag_var.rvalue()
                ),
                body=[
                    tag_var.assign(ast.Constant(value=True))
                ] + yield_cont(tag_event),  # Use yield continuation for the tag
                orelse=input_stmts  # Delegate to input stream
            )
        ]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize tag_emitted to False."""
        tag_var = ctx.state_var(self, 'tag_emitted')
        return [(tag_var.name, False)]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset tag_emitted to False."""
        tag_var = ctx.state_var(self, 'tag_emitted')
        return [
            tag_var.assign(ast.Constant(value=False))
        ]

    def _compile_stmts_generator(
        self,
        ctx,
        done_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        """Generator version - emit tag, then delegate to input. No state needed!"""
        tag_class = 'PlusPuncA' if self.position == 0 else 'PlusPuncB'

        tag_event = ast.Call(
            func=ast.Name(id=tag_class, ctx=ast.Load()),
            args=[],
            keywords=[]
        )

        # First yield the tag
        tag_yield = yield_cont(tag_event)

        # Then compile input stream
        input_stmts = self.input_stream._compile_stmts_generator(ctx, done_cont, yield_cont)

        # Sequential: emit tag, then run input
        return tag_yield + input_stmts
