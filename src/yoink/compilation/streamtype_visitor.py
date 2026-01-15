"""Base visitor class for stream Type compilation.

This module defines the visitor pattern interface for compiling stream types to Python AST.
Each compilation strategy can implement this visitor to handle different type constructors.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from yoink.typecheck.types import (
        Type,
        TyEps,
        TyCat,
        TyPlus,
        TyStar,
        Singleton,
        TypeVar
    )
    from yoink.compilation import CompilationContext


class StreamTypeVisitor:
    """Base visitor for compiling stream Types.

    This visitor handles the different type constructors in the stream type system,
    allowing different compilation strategies to generate appropriate code
    for each type's operations and structure.
    """

    def __init__(self, ctx: 'CompilationContext'):
        self.ctx = ctx

    def visit(self, ty: 'Type'):
        """Dispatch to the appropriate visit method based on type constructor."""
        method_name = f'visit_{ty.__class__.__name__}'
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(ty)

    def generic_visit(self, ty: 'Type'):
        """Called if no explicit visitor method exists for a ty."""
        raise NotImplementedError(
            f"{self.__class__.__name__} has no visit method for {ty.__class__.__name__}"
        )

    def visit_TyEps(self, ty: 'TyEps'):
        """Visit empty stream type."""
        raise NotImplementedError

    def visit_TyCat(self, ty: 'TyCat'):
        """Visit concatenation/product type."""
        raise NotImplementedError

    def visit_TyPlus(self, ty: 'TyPlus'):
        """Visit sum/choice type."""
        raise NotImplementedError

    def visit_TyStar(self, ty: 'TyStar'):
        """Visit Kleene star/list type."""
        raise NotImplementedError

    def visit_Singleton(self, ty: 'Singleton'):
        """Visit singleton type."""
        raise NotImplementedError

    def visit_TypeVar(self, ty: 'TypeVar'):
        """Visit type variable."""
        raise NotImplementedError
