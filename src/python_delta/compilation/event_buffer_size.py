"""Compiler for generating TypedBuffer construction code.

This module implements a visitor that walks stream types and generates
AST expressions for constructing the appropriate TypedBuffer instances
by recursively walking the type structure.
"""

from __future__ import annotations
from typing import TYPE_CHECKING
import ast

if TYPE_CHECKING:
    from python_delta.typecheck.types import (
        Type,
        TyEps,
        TyCat,
        TyPlus,
        TyStar,
        Singleton,
        TypeVar
    )


from python_delta.compilation.streamtype_visitor import StreamTypeVisitor

class EventBufferSize(StreamTypeVisitor):
    def __init__(self, ctx):
        super().__init__(ctx)

    def visit_TyEps(self, ty: 'TyEps'):
        return 0

    def visit_TyCat(self, ty: 'TyCat') -> ast.expr:
        return self.visit(ty.left_type) +  self.visit(ty.right_type)

    def visit_TyPlus(self, ty: 'TyPlus') -> ast.expr:
        return max(self.visit(ty.left_type),self.visit(ty.right_type))

    def visit_TyStar(self, ty: 'TyStar') -> ast.expr:
        raise NotImplementedError("Typed buffers of star type are not supported")
        
    def visit_Singleton(self, ty: 'Singleton') -> ast.expr:
        return 1

    def visit_TypeVar(self, ty: 'TypeVar') -> ast.expr:
        """Follow type variable links and generate buffer for the linked type."""
        assert ty.link is not None, f"TypeVar {ty.id} must be linked before compilation"
        return self.visit(ty.link)
