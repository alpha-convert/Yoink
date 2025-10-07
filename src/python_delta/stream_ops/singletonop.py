"""Singleton stream operation - emits a single value then is done."""

from __future__ import annotations

import ast
from typing import List

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class SingletonOp(StreamOp):
    """Stream operation that emits a single Python value then is done."""

    def __init__(self, value, stream_type):
        super().__init__(stream_type)
        self.value = value
        self.exhausted = False

    @property
    def id(self):
        return hash(("SingletonOp", id(self.value), self.stream_type))

    @property
    def vars(self):
        return set()  # No input streams, so no vars

    def _pull(self):
        if self.exhausted:
            return DONE
        self.exhausted = True
        from python_delta.event import BaseEvent
        return BaseEvent(self.value)

    def reset(self):
        self.exhausted = False

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile to: if not exhausted: dst = value; exhausted = True else: dst = DONE"""
        exhausted_var = ctx.get_state_var(self, "exhausted")
        from python_delta.event import BaseEvent

        return [
            ast.If(
                test=ast.UnaryOp(op=ast.Not(), operand=exhausted_var.to_ast()),
                body=[
                    dst.assign(ast.Call(
                        func=ast.Name(id='BaseEvent', ctx=ast.Load()),
                        args=[ast.Constant(value=self.value)],
                        keywords=[]
                    )),
                    exhausted_var.assign(ast.Constant(value=True))
                ],
                orelse=[
                    dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                ]
            )
        ]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        exhausted_var = ctx.get_state_var(self, "exhausted")
        return [(exhausted_var, ast.Constant(value=False))]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        exhausted_var = ctx.get_state_var(self, "exhausted")
        return [exhausted_var.assign(ast.Constant(value=False))]
