
"""Compiler for generating BufferOp evaluation code.

This module implements a visitor that walks BufferOp DAGs and generates
AST statements that write evaluation results into pre-allocated buffers.
Handles DAG structure to ensure each node is evaluated at most once.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, List, Set
import ast

from python_delta.compilation.bufferop_visitor import BufferOpVisitor
from python_delta.compilation.context import StateVar
from python_delta.compilation.event_buffer_size import EventBufferSize
from python_delta.typecheck.types import Singleton

if TYPE_CHECKING:
    from python_delta.stream_ops.bufferop import (
        BufferOp,
        ConstantOp,
        RegisterBuffer,
        WaitOpBuffer,
        BinaryOp,
        UnaryOp,
        ComparisonOp
    )
    from python_delta.compilation import CompilationContext


from python_delta.compilation.bufferop_visitor import BufferOpVisitor


class BufferOpStateCompiler(BufferOpVisitor):
    """Generates AST statements to pre-allocate the buffers that BufferOpCompiler-compiled operations write into.
    Each node has an associated state variable self.ctx.state_var(node,'out_buf') that it must allocate.
    (This is `result_var`)
    """

    def __init__(self, ctx: 'CompilationContext'):
        super().__init__(ctx)
        self.compiled_nodes: Set[int] = set()

    def result_var(self, node : BufferOp) -> StateVar:
        return self.ctx.state_var(node, 'out_buf')

    def visit(self, node: 'BufferOp') -> List[ast.stmt]:
        node_id = id(node)
        if node_id in self.compiled_nodes:
            return [ast.Pass()]
        else:
            stmts = super().visit(node)
            self.compiled_nodes.add(node_id)
            return stmts

    def visit_ConstantOp(self, node: 'ConstantOp') -> List[ast.stmt]:
        buffer_var = self.result_var(node)
        return [
            buffer_var.assign(
                ast.List(elts=[ast.Constant(value=None)], ctx=ast.Load())
            )
        ]
        

    def visit_RegisterBuffer(self, node: 'RegisterBuffer') -> List[ast.stmt]:
        # The buffer is the thing that has type "event list", ad the register is just the base-typed value
        result_var = self.result_var(node)
        register_var = self.ctx.state_var(node,'register')
        return [
            result_var.assign(
                ast.List(elts=[ast.Constant(value=None)], ctx=ast.Load())
            ),
            register_var.assign(ast.Constant(value = None))
        ]

    def visit_WaitOpBuffer(self, node: 'WaitOpBuffer') -> List[ast.stmt]:
        return []

    def visit_BinaryOp(self, node: 'BinaryOp') -> List[ast.stmt]:
        # result_var(node) := [None]
        stmts = []
        stmts.extend(self.visit(node.left))
        stmts.extend(self.visit(node.right))

        buffer_var = self.result_var(node)
        stmts.append(
            buffer_var.assign(
                ast.List(elts=[ast.Constant(value=None)], ctx=ast.Load())
            )
        )
        return stmts

    def visit_UnaryOp(self, node: 'UnaryOp') -> List[ast.stmt]:
        # result_var(node) := [None]
        stmts = []
        stmts.extend(self.visit(node.parent_op))

        buffer_var = self.result_var(node)
        stmts.append(
            buffer_var.assign(
                ast.List(elts=[ast.Constant(value=None)], ctx=ast.Load())
            )
        )
        return stmts

    def visit_ComparisonOp(self, node: 'ComparisonOp') -> List[ast.stmt]:
        # result_var(node) := [None]
        stmts = []
        stmts.extend(self.visit(node.parent_op))
        stmts.extend(self.visit(node.operand))

        buffer_var = self.result_var(node)
        stmts.append(
            buffer_var.assign(
                ast.List(elts=[ast.Constant(value=None)], ctx=ast.Load())
            )
        )
        return stmts