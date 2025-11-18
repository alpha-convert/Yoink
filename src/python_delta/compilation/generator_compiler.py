"""Generator compilation strategy - compiles using Python generator syntax.

This compiler generates code that uses yield statements to produce values.
The resulting code is more compact as it doesn't need explicit state variables
for tracking position - the Python runtime handles that automatically.
"""

from __future__ import annotations
from typing import List, Callable, TYPE_CHECKING
import ast

from python_delta.compilation.compiler_visitor import CompilerVisitor
from python_delta.compilation import CompilationContext, StateVar

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

    @staticmethod
    def compile(dataflow_graph) -> type:
        """Compile a dataflow graph using generator compilation.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The compiled class (not an instance)
        """
        module_ast = GeneratorCompiler._generate_module_ast(dataflow_graph)

        code = compile(module_ast, '<generated>', 'exec')

        # Execute in namespace with event types and DONE
        from python_delta.stream_ops import DONE, CatRState
        from python_delta.event import BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
        namespace = {
            'DONE': DONE,
            'BaseEvent': BaseEvent,
            'CatEvA': CatEvA,
            'CatPunc': CatPunc,
            'ParEvA': ParEvA,
            'ParEvB': ParEvB,
            'PlusPuncA': PlusPuncA,
            'PlusPuncB': PlusPuncB,
            'CatRState': CatRState,
        }
        exec(code, namespace)

        return namespace['FlattenedIterator']

    @staticmethod
    def get_code(dataflow_graph) -> str:
        """Get the compiled Python code as a string.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The generated Python code as a string
        """
        module_ast = GeneratorCompiler._generate_module_ast(dataflow_graph)
        return ast.unparse(module_ast)

    @staticmethod
    def _generate_module_ast(dataflow_graph) -> ast.Module:
        """Generate the complete module AST for generator compilation.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The module AST
        """
        ctx = CompilationContext()

        # Map input vars to their indices
        ctx.var_to_input_idx = {var.id: i for i, var in enumerate(dataflow_graph.input_vars)}

        # Compile the output node
        done_cont = [ast.Return(value=None)]  # End the generator
        yield_cont = lambda expr: [ast.Expr(value=ast.Yield(value=expr))]  # Yield values
        compiler = GeneratorCompiler(ctx, done_cont, yield_cont)
        output_stmts = dataflow_graph.outputs.accept(compiler)

        # Generate the class AST
        class_ast = GeneratorCompiler._generate_class_ast(dataflow_graph, ctx, output_stmts)

        # Create and return the module AST
        module_ast = ast.Module(body=[class_ast], type_ignores=[])
        ast.fix_missing_locations(module_ast)
        return module_ast

    @staticmethod
    def _generate_class_ast(dataflow_graph, ctx: CompilationContext, output_stmts: List[ast.stmt]) -> ast.ClassDef:
        """Generate the complete FlattenedIterator class for generator compilation."""
        body = [
            GeneratorCompiler._generate_init(dataflow_graph, ctx),
            GeneratorCompiler._generate_iter(output_stmts, ctx),
            GeneratorCompiler._generate_reset(dataflow_graph, ctx),
        ]

        return ast.ClassDef(
            name='FlattenedIterator',
            bases=[],
            keywords=[],
            body=body,
            decorator_list=[],
        )

    @staticmethod
    def _generate_init(_dataflow_graph, _ctx: CompilationContext) -> ast.FunctionDef:
        """Generate __init__ method - just store inputs.

        Generators don't need explicit state initialization like DirectCompiler/CPSCompiler.
        The generator's execution position IS the state.
        """
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
    def _generate_iter(output_stmts: List[ast.stmt], ctx: CompilationContext) -> ast.FunctionDef:
        """Generate __iter__ method as a generator function."""
        # Generate exception class definitions at the top of __iter__
        exception_defs = []
        for exc_name in ctx.escape_exceptions.values():
            exception_defs.append(
                ast.ClassDef(
                    name=exc_name,
                    bases=[ast.Name(id='Exception', ctx=ast.Load())],
                    keywords=[],
                    body=[ast.Pass()],
                    decorator_list=[]
                )
            )
        for exc_name in ctx.recurse_exceptions.values():
            exception_defs.append(
                ast.ClassDef(
                    name=exc_name,
                    bases=[ast.Name(id='Exception', ctx=ast.Load())],
                    keywords=[],
                    body=[ast.Pass()],
                    decorator_list=[]
                )
            )

        # Generate state variable initializations
        state_inits = []
        for node_id, state_vars in ctx.state_vars.items():
            for var_name, state_var in state_vars.items():
                state_inits.append(
                    ast.Assign(
                        targets=[state_var.lvalue()],
                        value=ast.Constant(value=False)
                    )
                )

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
            body=exception_defs + state_inits + output_stmts,
            decorator_list=[],
            returns=None,
        )

    @staticmethod
    def _generate_reset(dataflow_graph, ctx: CompilationContext) -> ast.FunctionDef:
        """Generate reset method - reset state variables to initial values."""
        # body: List[ast.stmt] = []

        # # Use ResetVisitor to generate reset statements for all nodes
        # reset_visitor = ResetVisitor(ctx)
        # for node in dataflow_graph.nodes:
        #     body.extend(reset_visitor.visit(node))

        # # If no state to reset, just pass
        # if not body:

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
            body=[ast.Pass()],
            decorator_list=[],
            returns=None,
        )

    def visit_Var(self, node: 'Var') -> List[ast.stmt]:
        """Generator version - loop through input iterator."""
        input_idx = self.ctx.var_to_input_idx[node.id]

        tmp_var = self.ctx.allocate_temp()

        input_access = ast.Subscript(
            value=ast.Attribute(
                value=ast.Name(id='self', ctx=ast.Load()),
                attr='inputs',
                ctx=ast.Load()
            ),
            slice=ast.Constant(value=input_idx),
            ctx=ast.Load()
        )

        return [
            ast.Try(
                body=[
                    ast.While(
                        test=ast.NamedExpr(
                            target=ast.Name(id=tmp_var.name, ctx=ast.Store()),
                            value=ast.Call(
                                func=ast.Name(id='next', ctx=ast.Load()),
                                args=[input_access],
                                keywords=[]
                            )
                        ),
                        body=self.yield_cont(tmp_var.rvalue()),
                        orelse=[]
                    )
                ],
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
        return self.done_cont

    def visit_SingletonOp(self, node: 'SingletonOp') -> List[ast.stmt]:
        """Generator version - emit value once, no state needed."""
        event_expr = ast.Call(
            func=ast.Name(id='BaseEvent', ctx=ast.Load()),
            args=[ast.Constant(value=node.value)],
            keywords=[]
        )

        return self.yield_cont(event_expr) + self.done_cont

    def visit_RecCall(self, node: 'RecCall') -> List[ast.stmt]:
        """Reset state variables for all nodes in reset_set, then raise recurse exception to restart loop."""
        reset_stmts = []

        # Reset state variables for all nodes in the reset set
        for reset_node in node.reset_set:
            if reset_node.id in self.ctx.state_vars:
                for var_name, state_var in self.ctx.state_vars[reset_node.id].items():
                    reset_stmts.append(
                        ast.Assign(
                            targets=[state_var.lvalue()],
                            value=ast.Constant(value=False)
                        )
                    )

        # Raise the enclosing block's RECURSE exception to restart the loop
        recurse_exc = self.ctx.recurse_exception(node.enclosing_block)
        reset_stmts.append(
            ast.Raise(
                exc=ast.Call(
                    func=ast.Name(id=recurse_exc, ctx=ast.Load()),
                    args=[],
                    keywords=[]
                ),
                cause=None
            )
        )
        return reset_stmts

    def visit_SumInj(self, node: 'SumInj') -> List[ast.stmt]:
        """Generator version - emit tag, then delegate to input. No state needed!"""
        tag_class = 'PlusPuncA' if node.position == 0 else 'PlusPuncB'

        tag_event = ast.Call(
            func=ast.Name(id=tag_class, ctx=ast.Load()),
            args=[],
            keywords=[]
        )

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
        def first_stream_yield_cont(val_expr):
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
            # Position 0: extract CatEvA values until CatPunc
            # When we see CatPunc, set seen_punc, execute done_cont, and raise escape exception
            escape_exc = self.ctx.escape_exception(coord)
            seen_punc_var = self.ctx.state_var(coord, 'seen_punc')

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
                                body=[
                                    seen_punc_var.assign(ast.Constant(value=True)),
                                    ast.Raise(
                                        exc=ast.Call(
                                            func=ast.Name(id=escape_exc, ctx=ast.Load()),
                                            args=[],
                                            keywords=[]
                                        ),
                                        cause=None
                                    )
                                ],
                                orelse=[ast.Pass()]
                            )
                        ]
                    )
                ]

            input_compiler = GeneratorCompiler(self.ctx, self.done_cont, input_yield_cont)
            input_stmts = coord.input_stream.accept(input_compiler)

            # Check seen_punc FIRST - if already true, immediately execute done_cont
            # Otherwise, wrap input processing in try/except to catch escape exception
            return [
                ast.If(
                    test=seen_punc_var.rvalue(),
                    body=self.done_cont,
                    orelse=[
                        ast.Try(
                            body=input_stmts,
                            handlers=[
                                ast.ExceptHandler(
                                    type=ast.Name(id=escape_exc, ctx=ast.Load()),
                                    name=None,
                                    body=self.done_cont
                                )
                            ],
                            orelse=[],
                            finalbody=[]
                        )
                    ]
                )
            ]
        else:
            assert node.position == 1
            # Position 1: Skip events until CatPunc, then yield everything after
            # Use the same seen_punc state var that position 0 sets
            seen_punc_var = self.ctx.state_var(coord, 'seen_punc')

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
                                body=[ast.Pass()],  # Skip CatEvA
                                orelse=[
                                    ast.If(
                                        test=ast.Call(
                                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                                            args=[event_expr, ast.Name(id='CatPunc', ctx=ast.Load())],
                                            keywords=[]
                                        ),
                                        body=[
                                            seen_punc_var.assign(ast.Constant(value=True)),
                                            # ast.Pass()  # Skip the first CatPunc
                                        ],
                                        orelse=[]  # Skip everything else before punc
                                    )
                                ]
                            )
                        ],
                        orelse=self.yield_cont(event_expr)  # After punc: pass through all events
                    )
                ]

            input_compiler = GeneratorCompiler(self.ctx, self.done_cont, input_yield_cont)
            input_stmts = coord.input_stream.accept(input_compiler)

            # Initialize seen_punc before processing
            return [seen_punc_var.assign(ast.Constant(value=False))] + input_stmts

    def visit_CaseOp(self, node: 'CaseOp') -> List[ast.stmt]:
        """Compile CaseOp with generators."""
        tag_var = self.ctx.allocate_temp()

        branch0_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
        branch0_stmts = node.branches[0].accept(branch0_compiler)

        branch1_compiler = GeneratorCompiler(self.ctx, self.done_cont, self.yield_cont)
        branch1_stmts = node.branches[1].accept(branch1_compiler)

        def input_yield_cont(tag_expr):
            return [
                ast.If(
                    test=ast.Call(
                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                        args=[tag_expr, ast.Name(id='PlusPuncA', ctx=ast.Load())],
                        keywords=[]
                    ),
                    body=branch0_stmts,
                    orelse=branch1_stmts
                )
            ]

        input_compiler = GeneratorCompiler(self.ctx, self.done_cont, input_yield_cont)
        return node.input_stream.accept(input_compiler)

    def visit_SinkThen(self, node: 'SinkThen') -> List[ast.stmt]:
        """Sink s1 (ignore all yields), then run s2."""
        # Sink s1 - ignore all values
        s1_compiler = GeneratorCompiler(self.ctx, [ast.Pass()], lambda _: [ast.Pass()])
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

        # TODO: this involves significant code duplication.
        # It seems to me like we should be able to generate code here
        # that looks like the CPS compiler code!
        # It pulls *one* event out and then continues to run.
        cond_compiler = GeneratorCompiler(self.ctx, self.done_cont, cond_yield_cont)
        return node.cond_stream.accept(cond_compiler)

    def visit_RecursiveSection(self, node: 'RecursiveSection') -> List[ast.stmt]:
        """Wrap the block contents in a nested try/while/try structure for reset control.

        Structure:
        try:                              # Outer try - catches escape exception
            while True:
                try:                      # Inner try - catches recurse exception
                    block_stmts
                    raise EscapeException # Normal completion exits to done_cont
                except RecurseException:  # ResetOp raises this to restart loop
                    pass
        except EscapeException:
            done_cont
        """
        # Get two node-unique exceptions for this block
        escape_exc = self.ctx.escape_exception(node)
        recurse_exc = self.ctx.recurse_exception(node)

        # Compile the block contents with done_cont that raises the escape exception
        block_done_cont: List[ast.stmt] = [
            ast.Raise(
                exc=ast.Call(
                    func=ast.Name(id=escape_exc, ctx=ast.Load()),
                    args=[],
                    keywords=[]
                ),
                cause=None
            )
        ]
        block_compiler = GeneratorCompiler(self.ctx, block_done_cont, self.yield_cont)
        block_stmts = node.block_contents.accept(block_compiler)

        # Nested structure: outer try catches escape, inner try catches recurse
        return [
            ast.Try(
                body=[
                    ast.While(
                        test=ast.Constant(value=True),
                        body=[
                            ast.Try(
                                body=block_stmts,
                                handlers=[
                                    ast.ExceptHandler(
                                        type=ast.Name(id=recurse_exc, ctx=ast.Load()),
                                        name=None,
                                        body=[ast.Continue()]  # Recurse - loop continues
                                    )
                                ],
                                orelse=[],
                                finalbody=[]
                            )
                        ],
                        orelse=[]
                    )
                ],
                handlers=[
                    ast.ExceptHandler(
                        type=ast.Name(id=escape_exc, ctx=ast.Load()),
                        name=None,
                        body=self.done_cont  # Escape - exit to done_cont
                    )
                ],
                orelse=[],
                finalbody=[]
            )
        ]