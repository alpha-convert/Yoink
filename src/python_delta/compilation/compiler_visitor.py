"""Base visitor class for StreamOp compilation.

This module defines the visitor pattern interface for compiling StreamOps to Python AST.
Each compilation strategy (direct, CPS, generator) is implemented as a concrete visitor.
"""

from __future__ import annotations
from typing import List, TYPE_CHECKING
import ast

if TYPE_CHECKING:
    from python_delta.stream_ops.base import StreamOp
    from python_delta.stream_ops.var import Var
    from python_delta.stream_ops.catr import CatR
    from python_delta.stream_ops.catproj import CatProj, CatProjCoordinator
    from python_delta.stream_ops.suminj import SumInj
    from python_delta.stream_ops.caseop import CaseOp
    from python_delta.stream_ops.eps import Eps
    from python_delta.stream_ops.singletonop import SingletonOp
    from python_delta.stream_ops.sinkthen import SinkThen
    from python_delta.stream_ops.resetop import ResetOp
    from python_delta.stream_ops.unsafecast import UnsafeCast
    from python_delta.stream_ops.condop import CondOp
    from python_delta.stream_ops.resetblockenclosing import ResetBlockEnclosingOp
    from python_delta.compilation import CompilationContext, StateVar


class CompilerVisitor:
    """Base visitor for compiling StreamOps to AST statements.

    Each compilation strategy (direct, CPS, generator) extends this class
    and implements visit methods for each StreamOp type.
    """

    def __init__(self, ctx: 'CompilationContext'):
        self.ctx = ctx

    @staticmethod
    def compile(dataflow_graph) -> type:
        """Compile a dataflow graph to a Python class.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The compiled class (not an instance)
        """
        raise NotImplementedError

    @staticmethod
    def get_code(dataflow_graph) -> str:
        """Get the compiled Python code as a string.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The generated Python code as a string
        """
        raise NotImplementedError

    def visit(self, node: 'StreamOp') -> List[ast.stmt]:
        """Dispatch to the appropriate visit method based on node type."""
        method_name = f'visit_{node.__class__.__name__}'
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node: 'StreamOp') -> List[ast.stmt]:
        """Called if no explicit visitor method exists for a node."""
        raise NotImplementedError(
            f"{self.__class__.__name__} has no visit method for {node.__class__.__name__}"
        )

    # Visit methods for each StreamOp type
    # These are abstract and must be implemented by concrete visitors

    def visit_Var(self, node: 'Var') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_CatR(self, node: 'CatR') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_CatProj(self, node: 'CatProj') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_SumInj(self, node: 'SumInj') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_CaseOp(self, node: 'CaseOp') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_Eps(self, node: 'Eps') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_SingletonOp(self, node: 'SingletonOp') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_SinkThen(self, node: 'SinkThen') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_ResetOp(self, node: 'ResetOp') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_UnsafeCast(self, node: 'UnsafeCast') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_CondOp(self, node: 'CondOp') -> List[ast.stmt]:
        raise NotImplementedError

    def visit_ResetBlockEnclosingOp(self, node: 'ResetBlockEnclosingOp') -> List[ast.stmt]:
        raise NotImplementedError

