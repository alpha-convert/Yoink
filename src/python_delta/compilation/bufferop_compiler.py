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
    from python_delta.compilation import CompilationContext


from python_delta.compilation.bufferop_visitor import BufferOpVisitor

# TODO: should produce two bits of code: (1) that allocates space for the result
# of an expression (a fixed-sized array of bytes to write its results into, as an event sequence), and
# (2)a block of code that writes
class BufferOpCompiler(BufferOpVisitor):
    pass
