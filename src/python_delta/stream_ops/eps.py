"""Eps StreamOp - empty stream."""

from __future__ import annotations

from typing import List, Callable
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class Eps(StreamOp):
    def __init__(self, stream_type):
        super().__init__(stream_type)

    @property
    def id(self):
        return hash(("Eps", id(self)))

    @property
    def vars(self):
        return set()

    def __str__(self):
        return f"Eps({self.stream_type})"

    def _pull(self):
        return DONE

    def reset(self):
        pass

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Compile to: dst = DONE"""
        return [
            ast.Assign(
                targets=[dst.lvalue()],
                value=ast.Name(id='DONE', ctx=ast.Load())
            )
        ]

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        return done_cont
