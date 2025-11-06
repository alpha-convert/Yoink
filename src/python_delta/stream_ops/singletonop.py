"""Singleton stream operation - emits a single value then is done."""

from __future__ import annotations

import ast
from typing import List, Callable

from python_delta.stream_ops.base import StreamOp, DONE


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

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        exhausted_var = ctx.state_var(self, 'exhausted')

        event_expr = ast.Call(
            func=ast.Name(id='BaseEvent', ctx=ast.Load()),
            args=[ast.Constant(value=self.value)],
            keywords=[]
        )

        return [
            ast.If(
                test=exhausted_var.rvalue(),
                body=done_cont,
                orelse=[
                    exhausted_var.assign(ast.Constant(value=True))
                ] + yield_cont(event_expr)
            )
        ]

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """Initialize exhausted to False."""
        exhausted_var = ctx.state_var(self, 'exhausted')
        return [(exhausted_var.name, False)]

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset exhausted to False."""
        exhausted_var = ctx.state_var(self, 'exhausted')
        return [
            exhausted_var.assign(ast.Constant(value=False))
        ]

    def _compile_stmts_generator(
        self,
        ctx,
        done_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        event_expr = ast.Call(
            func=ast.Name(id='BaseEvent', ctx=ast.Load()),
            args=[ast.Constant(value=self.value)],
            keywords=[]
        )

        return yield_cont(event_expr) + done_cont