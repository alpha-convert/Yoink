"""SinkThen StreamOp - sink one stream then switch to another."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class SinkThen(StreamOp):
    """Sink operation - pulls from first stream until exhausted, then switches to second stream."""
    def __init__(self, first_stream, second_stream, stream_type):
        super().__init__(stream_type)
        self.input_streams = [first_stream, second_stream]
        self.first_exhausted = False

    @property
    def id(self):
        return hash(("SinkThen", self.input_streams[0].id, self.input_streams[1].id))

    @property
    def vars(self):
        return self.input_streams[0].vars | self.input_streams[1].vars

    def _pull(self):
        """Pull from first stream until exhausted, then switch to second stream."""
        if not self.first_exhausted:
            # Pull from first stream and drop the value (sink it)
            val = self.input_streams[0]._pull()
            if val is DONE:
                # First stream exhausted, switch to second
                self.first_exhausted = True
                # Fall through to pull from second stream
            else:
                return None  # Drop all values from first stream

        # Pull from second stream
        return self.input_streams[1]._pull()

    def reset(self):
        """Reset state."""
        self.first_exhausted = False

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile exhaust-first-then-second logic."""
        exhausted_var = ctx.allocate_state(self, 'first_exhausted')

        val_tmp = ctx.allocate_temp()
        s1_stmts = self.input_streams[0]._compile_stmts(ctx, val_tmp)
        s2_stmts = self.input_streams[1]._compile_stmts(ctx, dst)

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=exhausted_var.rvalue()
                ),
                body=s1_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=val_tmp.rvalue(),
                            ops=[ast.Is()],
                            comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                        ),
                        body=[
                            ast.Assign(
                                targets=[exhausted_var.lvalue()],
                                value=ast.Constant(value=True)
                            )
                        ] + s2_stmts,
                        orelse=[
                            ast.Assign(
                                targets=[dst.lvalue()],
                                value=ast.Constant(value=None)
                            )
                        ]
                    )
                ],
                orelse=s2_stmts
            )
        ]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize first_exhausted to False."""
        exhausted_var = ctx.get_state_var(self, 'first_exhausted')
        return [(exhausted_var.name, False)]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset first_exhausted to False."""
        exhausted_var = ctx.get_state_var(self, 'first_exhausted')
        return [
            ast.Assign(
                targets=[exhausted_var.lvalue()],
                value=ast.Constant(value=False)
            )
        ]
