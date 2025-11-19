"""Compiler for generating BufferOp evaluation code.

This module implements a visitor that walks BufferOp DAGs and generates
AST statements that write evaluation results into pre-allocated buffers.
Handles DAG structure to ensure each node is evaluated at most once.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, List, Set
import ast

from python_delta.compilation.context import StateVar

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


class BufferOpCompiler(BufferOpVisitor):
    """Generates AST statements for BufferOp evaluation.

    Walks the BufferOp DAG and generates code that writes results into
    pre-allocated buffers. Tracks visited nodes to handle DAG structure
    correctly (evaluating shared nodes exactly once).

    Note: Buffer allocation is handled separately (assumed to be done upfront).
    This compiler only generates the write/evaluation logic.
    """

    def __init__(self, ctx: 'CompilationContext'):
        super().__init__(ctx)
        self.compiled_nodes: Set[int] = set()

    def result_var(self, node : BufferOp) -> StateVar:
        return self.ctx.state_var(node, 'out_buf')

    def visit(self, node: 'BufferOp') -> List[ast.stmt]:
        # Check if already compiled (DAG handling)
        node_id = id(node)
        if node_id in self.compiled_nodes:
            return [ast.Pass()]
        else:
            stmts = super().visit(node)
            self.compiled_nodes.add(node_id)
            return stmts

    def visit_ConstantOp(self, node: 'ConstantOp') -> List[ast.stmt]:
        """Generate code to write constant value into buffer.

        Generated code: self.out_buf_<id>[0] = BaseEvent(<constant_value>)
        """
        buffer_var = self.ctx.state_var(node, 'out_buf')

        return [
            ast.Assign(
                targets=[
                    ast.Subscript(
                        value=buffer_var.rvalue(),
                        slice=ast.Constant(value=0),
                        ctx=ast.Store()
                    )
                ],
                value=ast.Call(
                    func=ast.Name(id='BaseEvent', ctx=ast.Load()),
                    args=[ast.Constant(value=node.value)],
                    keywords=[]
                )
            )
        ]

    def visit_RegisterBuffer(self, node: 'RegisterBuffer') -> List[ast.stmt]:
        buffer_var = self.ctx.state_var(node, 'out_buf')
        register_var = self.ctx.state_var(node, 'register')

        return [
            ast.Assign(
                targets=[
                    ast.Subscript(
                        value=buffer_var.rvalue(),
                        slice=ast.Constant(value=0),
                        ctx=ast.Store()
                    )
                ],
                value=ast.Subscript(
                    value=ast.Call(
                        func=ast.Attribute(
                            value=register_var.rvalue(),
                            attr='get_events',
                            ctx=ast.Load()
                        ),
                        args=[],
                        keywords=[]
                    ),
                    slice=ast.Constant(value=0),
                    ctx=ast.Load()
                )
            )
        ]

    def visit_WaitOpBuffer(self, node: 'WaitOpBuffer') -> List[ast.stmt]:
        """Generate code to copy events from WaitOp buffer.

        Generated code:
        events = self.wait_op_<id>.buffer.get_events()
        for i, event in enumerate(events):
            self.out_buf_<id>[i] = event
        """
        buffer_var = self.ctx.state_var(node, 'out_buf')
        wait_buffer_var = self.ctx.state_var(node.wait_op, 'buffer')

        # Temporary variable for events
        events_temp = self.ctx.allocate_temp()

        return [
            # events = self.wait_op_buffer.get_events()
            ast.Assign(
                targets=[events_temp.lvalue()],
                value=ast.Call(
                    func=ast.Attribute(
                        value=wait_buffer_var.rvalue(),
                        attr='get_events',
                        ctx=ast.Load()
                    ),
                    args=[],
                    keywords=[]
                )
            ),
            # for i, event in enumerate(events):
            #     self.buffer[i] = event
            ast.For(
                target=ast.Tuple(
                    elts=[
                        ast.Name(id='i', ctx=ast.Store()),
                        ast.Name(id='event', ctx=ast.Store())
                    ],
                    ctx=ast.Store()
                ),
                iter=ast.Call(
                    func=ast.Name(id='enumerate', ctx=ast.Load()),
                    args=[events_temp.rvalue()],
                    keywords=[]
                ),
                body=[
                    ast.Assign(
                        targets=[
                            ast.Subscript(
                                value=buffer_var.rvalue(),
                                slice=ast.Name(id='i', ctx=ast.Load()),
                                ctx=ast.Store()
                            )
                        ],
                        value=ast.Name(id='event', ctx=ast.Load())
                    )
                ],
                orelse=[]
            )
        ]

    def visit_BinaryOp(self, node: 'BinaryOp') -> List[ast.stmt]:
        """Generate code for binary operation.

        Recursively compiles children, then generates:
        self.out_buf_<id>[0] = BaseEvent(self.out_buf_left[0].value <op> self.out_buf_right[0].value)
        """
        stmts = []

        # Compile children first (DAG-safe)
        stmts.extend(self.visit(node.left))
        stmts.extend(self.visit(node.right))

        # Get buffer variables
        buffer_var = self.ctx.state_var(node, 'out_buf')
        left_buffer = self.ctx.state_var(node.left, 'out_buf')
        right_buffer = self.ctx.state_var(node.right, 'out_buf')

        # Apply operator
        op_map = {
            '+': ast.Add(),
            '-': ast.Sub(),
            '*': ast.Mult(),
            '/': ast.Div(),
            '//': ast.FloorDiv(),
            '%': ast.Mod(),
            '**': ast.Pow()
        }

        # Write result: self.buffer[0] = BaseEvent(left.value <op> right.value)
        stmts.append(
            ast.Assign(
                targets=[
                    ast.Subscript(
                        value=buffer_var.rvalue(),
                        slice=ast.Constant(value=0),
                        ctx=ast.Store()
                    )
                ],
                value=ast.Call(
                    func=ast.Name(id='BaseEvent', ctx=ast.Load()),
                    args=[
                        ast.BinOp(
                            left=ast.Attribute(
                                value=ast.Subscript(
                                    value=left_buffer.rvalue(),
                                    slice=ast.Constant(value=0),
                                    ctx=ast.Load()
                                ),
                                attr='value',
                                ctx=ast.Load()
                            ),
                            op=op_map[node.op],
                            right=ast.Attribute(
                                value=ast.Subscript(
                                    value=right_buffer.rvalue(),
                                    slice=ast.Constant(value=0),
                                    ctx=ast.Load()
                                ),
                                attr='value',
                                ctx=ast.Load()
                            )
                        )
                    ],
                    keywords=[]
                )
            )
        )

        return stmts

    def visit_UnaryOp(self, node: 'UnaryOp') -> List[ast.stmt]:
        """Generate code for unary operation.

        Recursively compiles parent, then generates:
        self.out_buf_<id>[0] = BaseEvent(<op> self.out_buf_parent[0].value)
        """
        stmts = []

        # Compile parent first
        stmts.extend(self.visit(node.parent_op))

        # Get buffer variables
        buffer_var = self.ctx.state_var(node, 'out_buf')
        parent_buffer = self.ctx.state_var(node.parent_op, 'out_buf')

        # Apply unary operator
        op_map = {
            '-': ast.USub(),
            '+': ast.UAdd(),
            '~': ast.Invert(),
            'not': ast.Not()
        }

        # Write result: self.buffer[0] = BaseEvent(<op> parent.value)
        stmts.append(
            ast.Assign(
                targets=[
                    ast.Subscript(
                        value=buffer_var.rvalue(),
                        slice=ast.Constant(value=0),
                        ctx=ast.Store()
                    )
                ],
                value=ast.Call(
                    func=ast.Name(id='BaseEvent', ctx=ast.Load()),
                    args=[
                        ast.UnaryOp(
                            op=op_map[node.operator],
                            operand=ast.Attribute(
                                value=ast.Subscript(
                                    value=parent_buffer.rvalue(),
                                    slice=ast.Constant(value=0),
                                    ctx=ast.Load()
                                ),
                                attr='value',
                                ctx=ast.Load()
                            )
                        )
                    ],
                    keywords=[]
                )
            )
        )

        return stmts

    def visit_ComparisonOp(self, node: 'ComparisonOp') -> List[ast.stmt]:
        """Generate code for comparison operation.

        Recursively compiles both operands, then generates:
        self.buffer_<id>[0] = BaseEvent(self.parent_buffer[0].value <op> self.operand_buffer[0].value)
        """
        stmts = []

        # Compile both operands first
        stmts.extend(self.visit(node.parent_op))
        stmts.extend(self.visit(node.operand))

        # Get buffer variables
        buffer_var = self.ctx.state_var(node, 'buffer')
        parent_buffer = self.ctx.state_var(node.parent_op, 'buffer')
        operand_buffer = self.ctx.state_var(node.operand, 'buffer')

        # Apply comparison operator
        op_map = {
            '<': ast.Lt(),
            '<=': ast.LtE(),
            '>': ast.Gt(),
            '>=': ast.GtE(),
            '==': ast.Eq(),
            '!=': ast.NotEq()
        }

        # Write result: self.buffer[0] = BaseEvent(parent.value <op> operand.value)
        stmts.append(
            ast.Assign(
                targets=[
                    ast.Subscript(
                        value=buffer_var.rvalue(),
                        slice=ast.Constant(value=0),
                        ctx=ast.Store()
                    )
                ],
                value=ast.Call(
                    func=ast.Name(id='BaseEvent', ctx=ast.Load()),
                    args=[
                        ast.Compare(
                            left=ast.Attribute(
                                value=ast.Subscript(
                                    value=parent_buffer.rvalue(),
                                    slice=ast.Constant(value=0),
                                    ctx=ast.Load()
                                ),
                                attr='value',
                                ctx=ast.Load()
                            ),
                            ops=[op_map[node.operator]],
                            comparators=[
                                ast.Attribute(
                                    value=ast.Subscript(
                                        value=operand_buffer.rvalue(),
                                        slice=ast.Constant(value=0),
                                        ctx=ast.Load()
                                    ),
                                    attr='value',
                                    ctx=ast.Load()
                                )
                            ]
                        )
                    ],
                    keywords=[]
                )
            )
        )

        return stmts
