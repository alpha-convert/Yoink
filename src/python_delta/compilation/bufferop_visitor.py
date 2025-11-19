from __future__ import annotations
from typing import List, TYPE_CHECKING
import ast

if TYPE_CHECKING:
    from python_delta.stream_ops.bufferop import BufferOp, ConstantOp, RegisterBuffer, WaitOpBuffer, BinaryOp, UnaryOp, ComparisonOp
    from python_delta.compilation import CompilationContext, StateVar
    

class BufferOpVisitor:
    def __init__(self, ctx: 'CompilationContext'):
        self.ctx = ctx

    def visit(self, node: 'BufferOp'):
        method_name = f'visit_{node.__class__.__name__}'
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node: 'BufferOp'):
        raise NotImplementedError(
            f"{self.__class__.__name__} has no visit method for {node.__class__.__name__}"
        )

    def visit_ConstantOp(self, node: ConstantOp):
        raise NotImplementedError

    def visit_RegisterBuffer(self, node: RegisterBuffer):
        raise NotImplementedError

    def visit_WaitOpBuffer(self, node: WaitOpBuffer):
        raise NotImplementedError

    def visit_BinaryOp(self, node: BinaryOp):
        raise NotImplementedError

    def visit_UnaryOp(self, node: UnaryOp):
        raise NotImplementedError

    def visit_ComparisonOp(self, node: ComparisonOp) -> List[ast.stmt]:
        raise NotImplementedError
