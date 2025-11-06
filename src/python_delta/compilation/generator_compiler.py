"""Generator compilation strategy - compiles using Python generator syntax.

This compiler generates code that uses yield statements to produce values.
The resulting code is more compact as it doesn't need explicit state variables
for tracking position - the Python runtime handles that automatically.
"""

from __future__ import annotations
from typing import List, Callable, TYPE_CHECKING
import ast

from python_delta.compilation.compiler_visitor import CompilerVisitor

if TYPE_CHECKING:
    from python_delta.stream_ops.var import Var
    from python_delta.stream_ops.catr import CatR
    from python_delta.stream_ops.catproj import CatProj
    from python_delta.stream_ops.suminj import SumInj
    from python_delta.stream_ops.caseop import CaseOp
    from python_delta.stream_ops.eps import Eps
    from python_delta.stream_ops.singletonop import SingletonOp
    from python_delta.stream_ops.sinkthen import SinkThen
    from python_delta.stream_ops.resetop import ResetOp
    from python_delta.stream_ops.unsafecast import UnsafeCast
    from python_delta.stream_ops.condop import CondOp


class GeneratorCompiler(CompilerVisitor):
    """Generator compilation: uses yield statements.

    This corresponds to the old _compile_stmts_generator() method.
    Generates code that uses Python's generator features.
    """

    def __init__(self, ctx, done_cont: List[ast.stmt],
                 yield_cont: Callable[[ast.expr], List[ast.stmt]]):
        super().__init__(ctx)
        self.done_cont = done_cont
        self.yield_cont = yield_cont

    def visit_Var(self, node: 'Var') -> List[ast.stmt]:
        """Generator version - loop through input iterator."""
        input_idx = self.ctx.var_to_input_idx[node.id]

        tmp_var = self.ctx.allocate_temp()

        next_call = ast.Call(
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

        return [
            ast.While(
                test=ast.Constant(value=True),
                body=[
                    ast.Try(
                        body=[
                            tmp_var.assign(next_call)
                        ] + self.yield_cont(tmp_var.rvalue()),
                        handlers=[
                            ast.ExceptHandler(
                                type=ast.Name(id='StopIteration', ctx=ast.Load()),
                                name=None,
                                body=[ast.Break()]
                            )
                        ],
                        orelse=[],
                        finalbody=[]
                    )
                ],
                orelse=[]
            )
        ] + self.done_cont

    def visit_Eps(self, node: 'Eps') -> List[ast.stmt]:
        """Eps returns immediately - just execute done continuation."""
        return self.done_cont

    def visit_SingletonOp(self, node: 'SingletonOp') -> List[ast.stmt]:
        """Generator version - emit value once, no state needed."""
        event_expr = ast.Call(
            func=ast.Name(id='BaseEvent', ctx=ast.Load()),
            args=[ast.Constant(value=node.value)],
            keywords=[]
        )

        return self.yield_cont(event_expr) + self.done_cont

    def visit_ResetOp(self, node: 'ResetOp') -> List[ast.stmt]:
        """In generator mode, there are no state variables to reset."""
        # The generator's execution flow naturally handles "resetting"
        return []

    def visit_SumInj(self, node: 'SumInj') -> List[ast.stmt]:
        """Generator version - emit tag, then delegate to input. No state needed!"""
        tag_class = 'PlusPuncA' if node.position == 0 else 'PlusPuncB'

        tag_event = ast.Call(
            func=ast.Name(id=tag_class, ctx=ast.Load()),
            args=[],
            keywords=[]
        )

        # First yield the tag
        tag_yield = self.yield_cont(tag_event)

        # Then compile input stream
        input_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
        input_stmts = node.input_stream.accept(input_compiler)

        # Sequential: emit tag, then run input
        return tag_yield + input_stmts

    def visit_UnsafeCast(self, node: 'UnsafeCast') -> List[ast.stmt]:
        """Pass through to input stream."""
        input_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
        return node.input_stream.accept(input_compiler)

    def visit_CatR(self, node: 'CatR') -> List[ast.stmt]:
        """Compile CatR with generators."""
        def first_stream_yield_cont(val_expr):
            # Wrap values from s1 in CatEvA
            return self.yield_cont(
                ast.Call(
                    func=ast.Name(id='CatEvA', ctx=ast.Load()),
                    args=[val_expr],
                    keywords=[]
                )
            )

        first_stream_done_cont = self.yield_cont(
            ast.Call(
                func=ast.Name(id='CatPunc', ctx=ast.Load()),
                args=[],
                keywords=[]
            )
        )

        # Compile s1 - when done, yield CatPunc
        s1_compiler = GeneratorCompiler(self.ctx, first_stream_done_cont, first_stream_yield_cont)
        s1_stmts = node.input_streams[0].accept(s1_compiler)

        # Compile s2 - when done, propagate to parent's done_cont
        s2_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
        s2_stmts = node.input_streams[1].accept(s2_compiler)

        # Sequential execution: run s1, then s2
        return s1_stmts + s2_stmts

    def visit_CatProj(self, node: 'CatProj') -> List[ast.stmt]:
        """Compile CatProj with generators."""
        coord = node.coordinator

        if node.position == 0:
            def input_yield_cont(event_expr):
                return [
                    ast.If(
                        test=ast.Call(
                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                            args=[event_expr, ast.Name(id='CatEvA', ctx=ast.Load())],
                            keywords=[]
                        ),
                        body=self.yield_cont(
                            ast.Attribute(value=event_expr, attr='value', ctx=ast.Load())
                        ),
                        orelse=[ast.Pass()]  # Skip CatPunc and anything else
                    )
                ]

            input_compiler = GeneratorCompiler(self.ctx, self.done_cont, input_yield_cont)
            return coord.input_stream.accept(input_compiler)
        else:
            seen_punc_var = self.ctx.state_var(coord, 'seen_punc')

            def input_yield_cont(event_expr):
                return [
                    ast.If(
                        test=ast.Call(
                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                            args=[event_expr, ast.Name(id='CatPunc', ctx=ast.Load())],
                            keywords=[]
                        ),
                        body=[seen_punc_var.assign(ast.Constant(value=True))],
                        orelse=[
                            ast.If(
                                test=seen_punc_var.rvalue(),
                                body=self.yield_cont(event_expr),
                                orelse=[ast.Pass()]  # Skip CatEvA before punc
                            )
                        ]
                    )
                ]

            input_compiler = GeneratorCompiler(self.ctx, self.done_cont, input_yield_cont)
            return coord.input_stream.accept(input_compiler)

    def visit_CaseOp(self, node: 'CaseOp') -> List[ast.stmt]:
        """Compile CaseOp with generators."""
        tag_var = self.ctx.allocate_temp()

        def input_yield_cont(tag_expr):
            # Read tag, route to appropriate branch
            branch0_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
            branch0_stmts = node.branches[0].accept(branch0_compiler)

            branch1_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
            branch1_stmts = node.branches[1].accept(branch1_compiler)

            return [
                tag_var.assign(tag_expr),
                ast.If(
                    test=ast.Call(
                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                        args=[tag_var.rvalue(), ast.Name(id='PlusPuncA', ctx=ast.Load())],
                        keywords=[]
                    ),
                    body=branch0_stmts,
                    orelse=[
                        ast.If(
                            test=ast.Call(
                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                args=[tag_var.rvalue(), ast.Name(id='PlusPuncB', ctx=ast.Load())],
                                keywords=[]
                            ),
                            body=branch1_stmts,
                            orelse=[
                                ast.Raise(
                                    exc=ast.Call(
                                        func=ast.Name(id='RuntimeError', ctx=ast.Load()),
                                        args=[
                                            ast.JoinedStr(values=[
                                                ast.Constant(value='Expected PlusPuncA or PlusPuncB tag, got '),
                                                ast.FormattedValue(value=tag_var.rvalue(), conversion=-1, format_spec=None)
                                            ])
                                        ],
                                        keywords=[]
                                    ),
                                    cause=None
                                )
                            ]
                        )
                    ]
                )
            ]

        input_compiler = GeneratorCompiler(self.ctx, self.done_cont, input_yield_cont)
        return node.input_stream.accept(input_compiler)

    def visit_SinkThen(self, node: 'SinkThen') -> List[ast.stmt]:
        """Sink s1 (ignore all yields), then run s2."""
        # Sink s1 - ignore all values
        s1_compiler = GeneratorCompiler(self.ctx, [], lambda _: [ast.Pass()])
        s1_stmts = node.input_streams[0].accept(s1_compiler)

        # Run s2 normally
        s2_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
        s2_stmts = node.input_streams[1].accept(s2_compiler)

        return s1_stmts + s2_stmts

    def visit_CondOp(self, node: 'CondOp') -> List[ast.stmt]:
        """Compile CondOp with generators."""
        cond_var = self.ctx.allocate_temp()

        def cond_yield_cont(cond_expr):
            branch0_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
            branch0_stmts = node.branches[0].accept(branch0_compiler)

            branch1_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
            branch1_stmts = node.branches[1].accept(branch1_compiler)

            return [
                cond_var.assign(cond_expr),
                ast.If(
                    test=ast.Attribute(value=cond_var.rvalue(), attr='value', ctx=ast.Load()),
                    body=branch0_stmts,
                    orelse=branch1_stmts
                )
            ]

        cond_compiler = GeneratorCompiler(self.ctx, self.done_cont, cond_yield_cont)
        return node.cond_stream.accept(cond_compiler)
