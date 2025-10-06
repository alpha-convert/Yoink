"""CatR StreamOp - concatenation of two streams."""

from __future__ import annotations

from typing import List
import ast
from enum import Enum

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.event import CatEvA, CatPunc
from python_delta.compilation import StateVar

class CatRState(Enum):
    """State machine for CatR operation."""
    FIRST_STREAM = 0   # Pulling from first stream (wrapped in CatEvA)
    SECOND_STREAM = 1  # Pulling from second stream (unwrapped)

class CatR(StreamOp):
    def __init__(self, s1, s2, stream_type):
        super().__init__(stream_type)
        self.input_streams = [s1, s2]
        self.current_state = CatRState.FIRST_STREAM

    @property
    def id(self):
        return hash(("CatR", self.input_streams[0].id, self.input_streams[1].id))

    @property
    def vars(self):
        return self.input_streams[0].vars | self.input_streams[1].vars

    def _pull(self):
        """Pull from first stream (wrapped in CatEvA), then CatPunc, then second stream (unwrapped)."""
        if self.current_state == CatRState.FIRST_STREAM:
            val = self.input_streams[0]._pull()
            if val is DONE:
                self.current_state = CatRState.SECOND_STREAM
                return CatPunc()
            if val is None:
                return None
            return CatEvA(val)
        else:
            return self.input_streams[1]._pull()

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.current_state = CatRState.FIRST_STREAM

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile CatR state machine to if/else with nested conditionals."""
        state_var = ctx.allocate_state(self, 'state')
        tmp = ctx.allocate_temp()

        # Compile children
        s1_stmts = self.input_streams[0]._compile_stmts(ctx, tmp)
        s2_stmts = self.input_streams[1]._compile_stmts(ctx, dst)

        # Build the state machine: if state == FIRST_STREAM: ... else: ...
        return [
            ast.If(
                test=ast.Compare(
                    left=state_var.attr_load,
                    ops=[ast.Eq()],
                    comparators=[ast.Constant(value=CatRState.FIRST_STREAM.value)]
                ),
                body=s1_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=tmp.load,
                            ops=[ast.Is()],
                            comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                        ),
                        body=[
                            ast.Assign(
                                targets=[state_var.attr_store],
                                value=ast.Constant(value=CatRState.SECOND_STREAM.value)
                            ),
                            ast.Assign(
                                targets=[dst.store],
                                value=ast.Call(
                                    func=ast.Name(id='CatPunc', ctx=ast.Load()),
                                    args=[],
                                    keywords=[]
                                )
                            )
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=tmp.load,
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
                                    ast.Assign(
                                        targets=[dst.store],
                                        value=ast.Call(
                                            func=ast.Name(id='CatEvA', ctx=ast.Load()),
                                            args=[tmp.load],
                                            keywords=[]
                                        )
                                    )
                                ]
                            )
                        ]
                    )
                ],
                orelse=s2_stmts
            )
        ]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize state to FIRST_STREAM."""
        state_var = ctx.get_state_var(self, 'state')
        return [(state_var.name, CatRState.FIRST_STREAM.value)]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset state to FIRST_STREAM."""
        state_var = ctx.get_state_var(self, 'state')
        return [
            ast.Assign(
                targets=[state_var.attr_store],
                value=ast.Constant(value=CatRState.FIRST_STREAM.value)
            )
        ]
