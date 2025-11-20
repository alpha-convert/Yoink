"""CPS compilation strategy - compiles using continuation-passing style.

This compiler uses continuations to handle control flow:
- done_cont: statements to execute when stream is exhausted (DONE)
- skip_cont: statements to execute when no value is produced (None)
- yield_cont: function that takes an expression and returns statements to execute when a value is yielded
"""

from __future__ import annotations
from typing import List, Callable, TYPE_CHECKING
import ast

from python_delta.compilation.runtime import Runtime
from python_delta.compilation.streamop_visitor import StreamOpVisitor
from python_delta.compilation import CompilationContext, StateVar
from python_delta.compilation.streamop_reset_compiler import StreamOpResetCompiler

if TYPE_CHECKING:
    from python_delta.stream_ops.var import Var
    from python_delta.stream_ops.catr import CatR
    from python_delta.stream_ops.catproj import CatProj
    from python_delta.stream_ops.suminj import SumInj
    from python_delta.stream_ops.caseop import CaseOp
    from python_delta.stream_ops.eps import Eps
    from python_delta.stream_ops.singletonop import SingletonOp
    from python_delta.stream_ops.sinkthen import SinkThen
    from python_delta.stream_ops.rec_call import RecCall
    from python_delta.stream_ops.unsafecast import UnsafeCast
    from python_delta.stream_ops.condop import CondOp
    from python_delta.stream_ops.recursive_section import RecursiveSection


class CPSCompiler(StreamOpVisitor):

    def __init__(self, ctx, done_cont: List[ast.stmt], skip_cont: List[ast.stmt],
                 yield_cont: Callable[[ast.expr], List[ast.stmt]]):
        super().__init__(ctx)
        self.done_cont = done_cont
        self.skip_cont = skip_cont
        self.yield_cont = yield_cont

    @staticmethod
    def compile(dataflow_graph) -> type:
        """Compile a dataflow graph using CPS compilation.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The compiled class (not an instance)
        """
        module_ast = CPSCompiler._generate_module_ast(dataflow_graph)

        # Compile to bytecode and execute
        code = compile(module_ast, '<generated>', 'exec')

        runtime = Runtime()
        return runtime.exec(code)

    @staticmethod
    def get_code(dataflow_graph) -> str:
        """Get the compiled Python code as a string.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The generated Python code as a string
        """
        module_ast = CPSCompiler._generate_module_ast(dataflow_graph)
        return ast.unparse(module_ast)

    @staticmethod
    def _generate_module_ast(dataflow_graph) -> ast.Module:
        """Generate the complete module AST for CPS compilation."""
        ctx = CompilationContext()

        # Map input vars to their indices
        ctx.var_to_input_idx = {var.id: i for i, var in enumerate(dataflow_graph.input_vars)}

        # Compile the output node
        result_var = StateVar('result', tmp=True)
        done_cont = [result_var.assign(ast.Name(id='DONE', ctx=ast.Load()))]
        skip_cont = [result_var.assign(ast.Constant(value=None))]
        yield_cont = lambda expr: [result_var.assign(expr)]

        compiler = CPSCompiler(ctx, done_cont, skip_cont, yield_cont)
        output_stmts = dataflow_graph.outputs.accept(compiler)

        # Generate the class AST
        class_ast = CPSCompiler._generate_class_ast(dataflow_graph, ctx, output_stmts)

        # Generate module AST
        module_ast = ast.Module(body=[class_ast], type_ignores=[])
        ast.fix_missing_locations(module_ast)

        return module_ast

    @staticmethod
    def _generate_class_ast(dataflow_graph, ctx: CompilationContext, output_stmts: List[ast.stmt]) -> ast.ClassDef:
        """Generate the complete FlattenedIterator class for CPS compilation."""
        body = [
            CPSCompiler._generate_init(dataflow_graph, ctx),
            CPSCompiler._generate_iter(),
            CPSCompiler._generate_next(ctx, output_stmts),
            CPSCompiler._generate_reset(dataflow_graph, ctx),
        ]

        return ast.ClassDef(
            name='FlattenedIterator',
            bases=[],
            keywords=[],
            body=body,
            decorator_list=[],
        )

    @staticmethod
    def _generate_init(dataflow_graph, ctx: CompilationContext) -> ast.FunctionDef:
        """Generate __init__ method with state initialization."""
        body: List[ast.stmt] = [
            # self.inputs = list(input_iterators)
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr='inputs',
                    ctx=ast.Store()
                )],
                value=ast.Call(
                    func=ast.Name(id='list', ctx=ast.Load()),
                    args=[ast.Name(id='input_iterators', ctx=ast.Load())],
                    keywords=[]
                )
            )
        ]

        body.extend(StreamOpResetCompiler(ctx).compile_all(dataflow_graph.nodes))

        return ast.FunctionDef(
            name='__init__',
            args=ast.arguments(
                args=[ast.arg(arg='self', annotation=None)],
                vararg=ast.arg(arg='input_iterators', annotation=None),
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
                posonlyargs=[]
            ),
            body=body,
            decorator_list=[],
            returns=None,
        )

    @staticmethod
    def _generate_iter() -> ast.FunctionDef:
        """Generate __iter__ method."""
        return ast.FunctionDef(
            name='__iter__',
            args=ast.arguments(
                args=[ast.arg(arg='self', annotation=None)],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
                posonlyargs=[]
            ),
            body=[ast.Return(value=ast.Name(id='self', ctx=ast.Load()))],
            decorator_list=[],
            returns=None,
        )

    @staticmethod
    def _generate_next(ctx: CompilationContext, output_stmts: List[ast.stmt]) -> ast.FunctionDef:
        """Generate __next__ method."""
        body = output_stmts + [
            ast.If(
                test=ast.Compare(
                    left=ast.Name(id='result', ctx=ast.Load()),
                    ops=[ast.Is()],
                    comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                ),
                body=[ast.Raise(exc=ast.Call(
                    func=ast.Name(id='StopIteration', ctx=ast.Load()),
                    args=[],
                    keywords=[]
                ), cause=None)],
                orelse=[]
            ),
            ast.Return(value=ast.Name(id='result', ctx=ast.Load()))
        ]

        return ast.FunctionDef(
            name='__next__',
            args=ast.arguments(
                args=[ast.arg(arg='self', annotation=None)],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
                posonlyargs=[]
            ),
            body=body,
            decorator_list=[],
            returns=None,
        )

    @staticmethod
    def _generate_reset(dataflow_graph, ctx: CompilationContext) -> ast.FunctionDef:
        """Generate reset method."""

        body = StreamOpResetCompiler(ctx).compile_all(dataflow_graph.nodes)

        return ast.FunctionDef(
            name='reset',
            args=ast.arguments(
                args=[ast.arg(arg='self', annotation=None)],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
                posonlyargs=[]
            ),
            body=body,
            decorator_list=[],
            returns=None,
        )

    def visit_Var(self, node: 'Var') -> List[ast.stmt]:
        """Compile to: try: tmp = next(self.inputs[idx]); yield_cont(tmp) except StopIteration: done_cont"""
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
            ast.Try(
                body=[
                    tmp_var.assign(next_call)
                ] + self.yield_cont(tmp_var.rvalue()),
                handlers=[
                    ast.ExceptHandler(
                        type=ast.Name(id='StopIteration', ctx=ast.Load()),
                        name=None,
                        body=self.done_cont
                    )
                ],
                orelse=[],
                finalbody=[]
            )
        ]

    def visit_Eps(self, node: 'Eps') -> List[ast.stmt]:
        """Eps immediately executes done continuation."""
        return self.done_cont

    def visit_SingletonOp(self, node: 'SingletonOp') -> List[ast.stmt]:
        """Emit value once, then done."""
        exhausted_var = self.ctx.state_var(node, 'exhausted')

        event_expr = ast.Call(
            func=ast.Name(id='BaseEvent', ctx=ast.Load()),
            args=[ast.Constant(value=node.value)],
            keywords=[]
        )

        return [
            ast.If(
                test=exhausted_var.rvalue(),
                body=self.done_cont,
                orelse=[
                    exhausted_var.assign(ast.Constant(value=True))
                ] + self.yield_cont(event_expr)
            )
        ]

    def visit_RecCall(self, node: 'RecCall') -> List[ast.stmt]:
        reset_stmts = StreamOpResetCompiler(self.ctx).compile_all(node.reset_set)
        return reset_stmts + self.skip_cont

    def visit_SumInj(self, node: 'SumInj') -> List[ast.stmt]:
        """Emit tag, then compile input stream."""
        tag_var = self.ctx.state_var(node, 'tag_emitted')

        tag_class = 'PlusPuncA' if node.position == 0 else 'PlusPuncB'

        tag_event = ast.Call(
            func=ast.Name(id=tag_class, ctx=ast.Load()),
            args=[],
            keywords=[]
        )

        input_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        input_stmts = node.input_stream.accept(input_compiler)

        # If tag not emitted, emit it; otherwise delegate to input
        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=tag_var.rvalue()
                ),
                body=[
                    tag_var.assign(ast.Constant(value=True))
                ] + self.yield_cont(tag_event),  # Use yield continuation for the tag
                orelse=input_stmts  # Delegate to input stream
            )
        ]

    def visit_UnsafeCast(self, node: 'UnsafeCast') -> List[ast.stmt]:
        """Pass through to input stream."""
        input_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        return node.input_stream.accept(input_compiler)

    def visit_CatR(self, node: 'CatR') -> List[ast.stmt]:
        """Compile CatR state machine with CPS."""
        from python_delta.stream_ops.catr import CatRState

        state_var = self.ctx.state_var(node, 'state')

        def first_stream_yield_cont(val_expr):
            return self.yield_cont(
                ast.Call(
                    func=ast.Name(id='CatEvA', ctx=ast.Load()),
                    args=[val_expr],
                    keywords=[]
                )
            )

        first_stream_done_cont = [
            state_var.assign(ast.Constant(value=CatRState.SECOND_STREAM.value))
        ] + self.yield_cont(
            ast.Call(
                func=ast.Name(id='CatPunc', ctx=ast.Load()),
                args=[],
                keywords=[]
            )
        )

        s1_compiler = CPSCompiler(self.ctx, first_stream_done_cont, self.skip_cont, first_stream_yield_cont)
        s1_stmts = node.input_streams[0].accept(s1_compiler)

        s2_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        s2_stmts = node.input_streams[1].accept(s2_compiler)

        return [
            ast.If(
                test=ast.Compare(
                    left=state_var.rvalue(),
                    ops=[ast.Eq()],
                    comparators=[ast.Constant(value=CatRState.FIRST_STREAM.value)]
                ),
                body=s1_stmts,
                orelse=s2_stmts
            )
        ]

    def visit_CatProj(self, node: 'CatProj') -> List[ast.stmt]:
        """Compile CatProj with CPS."""
        coord = node.coordinator
        coord_id = coord.id

        seen_punc_var = self.ctx.state_var(coord, 'seen_punc')
        input_exhausted_var = self.ctx.state_var(coord, 'input_exhausted')

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
                        orelse=[
                            ast.If(
                                test=ast.Call(
                                    func=ast.Name(id='isinstance', ctx=ast.Load()),
                                    args=[event_expr, ast.Name(id='CatPunc', ctx=ast.Load())],
                                    keywords=[]
                                ),
                                body=[seen_punc_var.assign(ast.Constant(value=True))] + self.done_cont,
                                orelse=self.skip_cont
                            )
                        ]
                    )
                ]

            input_done_cont = [input_exhausted_var.assign(ast.Constant(value=True))] + self.done_cont

            input_compiler = CPSCompiler(self.ctx, input_done_cont, self.skip_cont, input_yield_cont)
            input_stmts = coord.input_stream.accept(input_compiler)

            return [
                ast.If(
                    test=input_exhausted_var.rvalue(),
                    body=self.done_cont,
                    orelse=[
                        ast.If(
                            test=seen_punc_var.rvalue(),
                            body=self.done_cont,
                            orelse=input_stmts
                        )
                    ]
                )
            ]
        else:
            def input_yield_cont(event_expr):
                # Position 1: skip events until CatPunc, then pass through all tail events
                return [
                    ast.If(
                        test=ast.UnaryOp(
                            op=ast.Not(),
                            operand=seen_punc_var.rvalue()
                        ),
                        body=[
                            # Before punc: skip CatEvA and CatPunc
                            ast.If(
                                test=ast.Call(
                                    func=ast.Name(id='isinstance', ctx=ast.Load()),
                                    args=[event_expr, ast.Name(id='CatEvA', ctx=ast.Load())],
                                    keywords=[]
                                ),
                                body=self.skip_cont,
                                orelse=[
                                    ast.If(
                                        test=ast.Call(
                                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                                            args=[event_expr, ast.Name(id='CatPunc', ctx=ast.Load())],
                                            keywords=[]
                                        ),
                                        body=[seen_punc_var.assign(ast.Constant(value=True))] + self.skip_cont,
                                        orelse=self.skip_cont
                                    )
                                ]
                            )
                        ],
                        orelse=self.yield_cont(event_expr)  # After punc: pass through all events
                    )
                ]

            input_done_cont = [input_exhausted_var.assign(ast.Constant(value=True))] + self.done_cont

            input_compiler = CPSCompiler(self.ctx, input_done_cont, self.skip_cont, input_yield_cont)
            input_stmts = coord.input_stream.accept(input_compiler)

            return [
                ast.If(
                    test=input_exhausted_var.rvalue(),
                    body=self.done_cont,
                    orelse=input_stmts
                )
            ]

    def visit_CaseOp(self, node: 'CaseOp') -> List[ast.stmt]:
        """Compile tag reading and branch routing with CPS."""
        tag_read_var = self.ctx.state_var(node, 'tag_read')
        active_branch_var = self.ctx.state_var(node, 'active_branch')

        def tag_yield_cont(tag_expr):
            return [
                tag_read_var.assign(ast.Constant(value=True)),
                ast.If(
                    test=ast.Call(
                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                        args=[tag_expr, ast.Name(id='PlusPuncA', ctx=ast.Load())],
                        keywords=[]
                    ),
                    body=[
                        active_branch_var.assign(ast.Constant(value=0))
                    ],
                    orelse=[
                        ast.If(
                            test=ast.Call(
                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                args=[tag_expr, ast.Name(id='PlusPuncB', ctx=ast.Load())],
                                keywords=[]
                            ),
                            body=[
                                active_branch_var.assign(ast.Constant(value=1))
                            ],
                            orelse=[
                                ast.Raise(
                                    exc=ast.Call(
                                        func=ast.Name(id='RuntimeError', ctx=ast.Load()),
                                        args=[
                                            ast.JoinedStr(values=[
                                                ast.Constant(value='Expected PlusPuncA or PlusPuncB tag, got '),
                                                ast.FormattedValue(value=tag_expr, conversion=-1, format_spec=None)
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
            ] + self.skip_cont

        input_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, tag_yield_cont)
        input_stmts = node.input_stream.accept(input_compiler)

        branch0_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        branch0_stmts = node.branches[0].accept(branch0_compiler)

        branch1_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        branch1_stmts = node.branches[1].accept(branch1_compiler)

        return [
            ast.If(
                test=ast.UnaryOp(op=ast.Not(), operand=tag_read_var.rvalue()),
                body=input_stmts,
                orelse=[
                    ast.If(
                        test=ast.Compare(
                            left=active_branch_var.rvalue(),
                            ops=[ast.Eq()],
                            comparators=[ast.Constant(value=0)]
                        ),
                        body=branch0_stmts,
                        orelse=branch1_stmts
                    )
                ]
            )
        ]

    def visit_SinkThen(self, node: 'SinkThen') -> List[ast.stmt]:
        """Compile exhaust-first-then-second logic with CPS."""
        exhausted_var = self.ctx.state_var(node, 'first_exhausted')

        s1_done_cont = [
            exhausted_var.assign(ast.Constant(value=True))
        ] + self.skip_cont

        s1_compiler = CPSCompiler(self.ctx, s1_done_cont, self.skip_cont, lambda _: self.skip_cont)
        s1_stmts = node.input_streams[0].accept(s1_compiler)

        s2_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        s2_stmts = node.input_streams[1].accept(s2_compiler)

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=exhausted_var.rvalue()
                ),
                body=s1_stmts,
                orelse=s2_stmts
            )
        ]

    def visit_CondOp(self, node: 'CondOp') -> List[ast.stmt]:
        """Compile boolean condition reading and branch routing with CPS."""
        active_branch_var = self.ctx.state_var(node, 'active_branch')

        def cond_yield_cont(cond_expr):
            return [
                ast.If(
                    test=ast.Attribute(value=cond_expr, attr='value', ctx=ast.Load()),
                    body=[active_branch_var.assign(ast.Constant(value=0))],
                    orelse=[active_branch_var.assign(ast.Constant(value=1))]
                )
            ] + self.skip_cont

        cond_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, cond_yield_cont)
        cond_stmts = node.cond_stream.accept(cond_compiler)

        branch0_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        branch0_stmts = node.branches[0].accept(branch0_compiler)

        branch1_compiler = CPSCompiler(self.ctx, self.done_cont, self.skip_cont, self.yield_cont)
        branch1_stmts = node.branches[1].accept(branch1_compiler)

        return [
            ast.If(
                test=ast.Compare(
                    left=active_branch_var.rvalue(),
                    ops=[ast.Is()],
                    comparators=[ast.Constant(value=None)]
                ),
                body=cond_stmts,
                orelse=[
                    ast.If(
                        test=ast.Compare(
                            left=active_branch_var.rvalue(),
                            ops=[ast.Eq()],
                            comparators=[ast.Constant(value=0)]
                        ),
                        body=branch0_stmts,
                        orelse=branch1_stmts
                    )
                ]
            )
        ]


    def visit_RecursiveSection(self, node: 'RecursiveSection') -> List[ast.stmt]:
        return self.visit(node.block_contents)

    def visit_EmitOp(self, node : 'EmitOp') -> List[ast.stmt]:
        return [ast.Pass()]

    def visit_WaitOp(self, node : WaitOp) -> List[ast.stmt]:
        return [ast.Pass()]