"""Var StreamOp - input variable."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar


class Var(StreamOp):
    def __init__(self, name, stream_type):
        super().__init__(stream_type)
        self.name = name
        self.source = None

    @property
    def id(self):
        return hash(("Var", self.name))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"Var({self.name}: {self.stream_type})"

    def _pull(self):
        """Pull from the source iterator."""
        if self.source is None:
            raise RuntimeError(f"Var '{self.name}' has no source bound")
        try:
            return next(self.source)
        except StopIteration:
            return DONE

    def reset(self):
        pass

    def _compile_stmts(self, ctx: 'CompilationContext', dst: StateVar) -> List[ast.stmt]:
        """Compile to: try: dst = next(self.inputs[idx]) except StopIteration: dst = DONE"""
        input_idx = ctx.var_to_input_idx[self.id]

        return [
            ast.Try(
                body=[
                    ast.Assign(
                        targets=[dst.lvalue()],
                        value=ast.Call(
                            func=ast.Name(id='next', ctx=ast.Load()),
                            args=[
                                ast.Subscript(
                                    value=ast.Attribute(
                                        value=ast.Name(id='self', ctx=ast.Load()),
                                        attr='inputs',
                                        ctx=ast.Load()
                                    ),
                                    slice=ast.Constant(value=input_idx),
                                    ctx=ast.Load()
                                )
                            ],
                            keywords=[]
                        )
                    )
                ],
                handlers=[
                    ast.ExceptHandler(
                        type=ast.Name(id='StopIteration', ctx=ast.Load()),
                        name=None,
                        body=[
                            ast.Assign(
                                targets=[dst.lvalue()],
                                value=ast.Name(id='DONE', ctx=ast.Load())
                            )
                        ]
                    )
                ],
                orelse=[],
                finalbody=[]
            )
        ]
