"""Visitor for generating reset statements for StreamOps."""

from __future__ import annotations
from typing import List, TYPE_CHECKING
import ast

if TYPE_CHECKING:
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
    from python_delta.compilation import CompilationContext


class ResetVisitor:
    """Visitor for generating reset statements."""

    def __init__(self, ctx: 'CompilationContext'):
        self.ctx = ctx

    def visit(self, node) -> List[ast.stmt]:
        """Dispatch to the appropriate visit method based on node type."""
        method_name = f'visit_{node.__class__.__name__}'
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node) -> List[ast.stmt]:
        """Called if no explicit visitor method exists for a node."""
        # Most nodes don't need reset
        return []

    def visit_SingletonOp(self, node: 'SingletonOp') -> List[ast.stmt]:
        """Reset exhausted to False."""
        exhausted_var = self.ctx.state_var(node, 'exhausted')
        return [exhausted_var.assign(ast.Constant(value=False))]

    def visit_CatR(self, node: 'CatR') -> List[ast.stmt]:
        """Reset state to FIRST_STREAM."""
        from python_delta.stream_ops.catr import CatRState
        state_var = self.ctx.state_var(node, 'state')
        return [state_var.assign(ast.Constant(value=CatRState.FIRST_STREAM.value))]

    def visit_CatProj(self, node: 'CatProj') -> List[ast.stmt]:
        """Reset coordinator state."""
        coord = node.coordinator
        seen_punc_var = self.ctx.state_var(coord, 'seen_punc')
        input_exhausted_var = self.ctx.state_var(coord, 'input_exhausted')
        return [
            seen_punc_var.assign(ast.Constant(value=False)),
            input_exhausted_var.assign(ast.Constant(value=False))
        ]

    def visit_SumInj(self, node: 'SumInj') -> List[ast.stmt]:
        """Reset tag_emitted to False."""
        tag_var = self.ctx.state_var(node, 'tag_emitted')
        return [tag_var.assign(ast.Constant(value=False))]

    def visit_CaseOp(self, node: 'CaseOp') -> List[ast.stmt]:
        """Reset tag_read and active_branch."""
        tag_read_var = self.ctx.state_var(node, 'tag_read')
        active_branch_var = self.ctx.state_var(node, 'active_branch')
        return [
            tag_read_var.assign(ast.Constant(value=False)),
            active_branch_var.assign(ast.Constant(value=-1))
        ]

    def visit_SinkThen(self, node: 'SinkThen') -> List[ast.stmt]:
        """Reset first_exhausted."""
        exhausted_var = self.ctx.state_var(node, 'first_exhausted')
        return [exhausted_var.assign(ast.Constant(value=False))]

    def visit_CondOp(self, node: 'CondOp') -> List[ast.stmt]:
        """Reset active_branch."""
        active_branch_var = self.ctx.state_var(node, 'active_branch')
        return [active_branch_var.assign(ast.Constant(value=None))]

    # Nodes that don't need reset
    def visit_Var(self, node: 'Var') -> List[ast.stmt]:
        return []

    def visit_Eps(self, node: 'Eps') -> List[ast.stmt]:
        return []

    def visit_ResetOp(self, node: 'ResetOp') -> List[ast.stmt]:
        return []

    def visit_UnsafeCast(self, node: 'UnsafeCast') -> List[ast.stmt]:
        return []
