from __future__ import annotations

import ast
from python_delta.compilation import CompilationContext, StateVar
from python_delta.stream_ops import DONE, CatRState
from python_delta.event import CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB


class DataflowGraph:
    """
    A dataflow graph representing a traced stream function that can be executed
    with concrete iterators or composed with other traced functions.
    """
    def __init__(self, nodes, input_vars, outputs, original_func, input_types):
        """
        Args:
            nodes: The traced computation graph
            input_vars: List of Var nodes representing function inputs
            outputs: The output StreamOp(s) from the traced function
            original_func: The original untraced function
            input_types: List of input types for the function
        """
        self.nodes = nodes
        self.input_vars = input_vars
        self.outputs = outputs
        self.original_func = original_func
        self.input_types = input_types

    def __call__(self, *args):
        """
        Call the function with either symbolic (for composition) or concrete arguments.

        If first argument is a Delta instance, re-trace the function for composition.
        Otherwise, treat as concrete iterators and use the pre-compiled graph.
        """
        if len(args) == 0:
            return self.run(*args)

        from python_delta.delta import Delta
        # Check if first arg is a Delta instance
        if isinstance(args[0], Delta):
            if len(args) != len(self.input_types) + 1:
                raise ValueError(f"Expected {len(self.input_types) + 1} arguments (delta + {len(self.input_types)} streams), got {len(args)}")
            return self.original_func(*args)
        else:
            return self.run(*args)

    def run(self, *iterators):
        """
        Execute the dataflow graph with concrete iterators.

        Args:
            *iterators: Concrete iterators to bind to input variables

        Returns:
            The output stream(s), ready to be iterated
        """
        if len(iterators) != len(self.input_vars):
            raise ValueError(f"Expected {len(self.input_vars)} iterators, got {len(iterators)}")

        # Reset all nodes to initial state
        for node in self.nodes:
            node.reset()

        # Bind concrete iterators to Var sources
        for var, iterator in zip(self.input_vars, iterators):
            var.source = iterator

        # Return the output stream(s)
        return self.outputs

    def to_graphviz(self):
        """
        Generate a graphviz DOT representation of the computation graph.

        Returns:
            str: A DOT format string suitable for rendering with graphviz

        Example:
            >>> @Delta.jit
            >>> def concat(delta, x: STRING_TY, y: STRING_TY):
            ...     return delta.catr(x, y)
            >>> print(concat.to_graphviz())
        """
        from python_delta.util.viz_builder import VizBuilder
        return VizBuilder(self).to_graphviz()

    def save_graphviz(self, filename):
        """
        Save the graphviz DOT representation to a file.

        Args:
            filename (str): Path to save the DOT file. If it ends with .png, .pdf, or .svg,
                          will attempt to render using graphviz (if available).

        Returns:
            str: Path to the saved file

        Example:
            >>> @Delta.jit
            >>> def concat(delta, x: STRING_TY, y: STRING_TY):
            ...     return delta.catr(x, y)
            >>> concat.save_graphviz('graph.png')  # Requires graphviz installed
            >>> concat.save_graphviz('graph.dot')  # Just save DOT file
        """
        from python_delta.util.viz_builder import VizBuilder
        return VizBuilder(self).save(filename)

    def compile(self, cps = False, generator = False) -> type:
        """
        Compile the StreamOp graph into a single flat iterator class.

        Args:
            cps: Use CPS-style compilation with state machine
            generator: Use generator-based compilation (implies CPS without state vars)

        Returns the compiled class (not an instance).
        """
        if cps and generator:
            raise ValueError("cps and generator are mutually exclusive")

        ctx = CompilationContext()

        # Map input vars to their indices
        ctx.var_to_input_idx = {var.id: i for i, var in enumerate(self.input_vars)}

        # Handle tuple outputs (e.g., from catl which returns (x, y))
        # For now, only compile single outputs
        if isinstance(self.outputs, tuple):
            raise NotImplementedError("Compilation of tuple outputs not yet supported")

        # Compile the output node (this will recursively compile all dependencies)
        result_var = StateVar('result', tmp=True)
        if generator:
            # Use visitor pattern for generator compilation
            from python_delta.compilation.generator_compiler import GeneratorCompiler
            done_cont = [ast.Return(value=None)]  # End the generator
            yield_cont = lambda expr: [ast.Expr(value=ast.Yield(value=expr))]  # Yield values
            compiler = GeneratorCompiler(ctx, done_cont, yield_cont)
            output_stmts = self.outputs.accept(compiler)
        elif cps:
            # Use visitor pattern for CPS compilation
            from python_delta.compilation.cps_compiler import CPSCompiler
            done_cont = [result_var.assign(ast.Name(id='DONE', ctx=ast.Load()))]
            skip_cont = [result_var.assign(ast.Constant(value=None))]
            yield_cont = lambda expr: [result_var.assign(expr)]
            compiler = CPSCompiler(ctx, done_cont, skip_cont, yield_cont)
            output_stmts = self.outputs.accept(compiler)
        else:
            # Use visitor pattern for direct compilation
            from python_delta.compilation.direct_compiler import DirectCompiler
            compiler = DirectCompiler(ctx, result_var)
            output_stmts = self.outputs.accept(compiler)

        # Generate the class AST
        class_ast = self._generate_class_ast(ctx, output_stmts, is_generator=generator)

        # Compile to bytecode and execute
        module_ast = ast.Module(body=[class_ast], type_ignores=[])
        ast.fix_missing_locations(module_ast)

        code = compile(module_ast, '<generated>', 'exec')

        # Execute in namespace with event types and DONE
        from python_delta.event import BaseEvent
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

    def get_code(self, cps = False) -> str:
        """
        Get the compiled Python code for this dataflow graph as a string.

        Returns:
            str: The generated Python code
        """
        ctx = CompilationContext()

        # Map input vars to their indices
        ctx.var_to_input_idx = {var.id: i for i, var in enumerate(self.input_vars)}

        # Handle tuple outputs (e.g., from catl which returns (x, y))
        if isinstance(self.outputs, tuple):
            raise NotImplementedError("Compilation of tuple outputs not yet supported")

        # Compile the output node (this will recursively compile all dependencies)
        result_var = StateVar('result', tmp=True)
        if cps:
            done_cont = [result_var.assign(ast.Name(id='DONE', ctx=ast.Load()))]
            skip_cont = [result_var.assign(ast.Constant(value=None))]
            yield_cont = lambda expr: [result_var.assign(expr)]
            output_stmts = self.outputs._compile_stmts_cps(ctx, done_cont, skip_cont, yield_cont)
        else:
            output_stmts = self.outputs._compile_stmts(ctx, result_var)

        # Generate the class AST
        class_ast = self._generate_class_ast(ctx, output_stmts)

        # Generate module AST
        module_ast = ast.Module(body=[class_ast], type_ignores=[])
        ast.fix_missing_locations(module_ast)

        # Unparse to Python code
        return ast.unparse(module_ast)

    def print_code(self, cps = False):
        print(self.get_code(cps))

    def _generate_class_ast(self, ctx: CompilationContext, output_stmts: list, is_generator: bool = False) -> ast.ClassDef:
        """Generate the complete FlattenedIterator class."""
        if is_generator:
            # For generators, __iter__ is a generator function containing output_stmts
            body = [
                self._generate_init(ctx),
                self._generate_iter_generator(output_stmts),
                self._generate_reset(ctx),
            ]
        else:
            # For non-generators, use __next__ method
            body = [
                self._generate_init(ctx),
                self._generate_iter(),
                self._generate_next(ctx, output_stmts),
                self._generate_reset(ctx),
            ]

        return ast.ClassDef(
            name='FlattenedIterator',
            bases=[],
            keywords=[],
            body=body,
            decorator_list=[],
        )

    def _generate_init(self, ctx: CompilationContext) -> ast.FunctionDef:
        """Generate __init__ method with state initialization."""
        body = [
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

        # Add state initializers from all nodes
        for node in self.nodes:
            for var_name, initial_value in node._get_state_initializers(ctx):
                body.append(
                    ast.Assign(
                        targets=[ast.Attribute(
                            value=ast.Name(id='self', ctx=ast.Load()),
                            attr=var_name,
                            ctx=ast.Store()
                        )],
                        value=ast.Constant(value=initial_value)
                    )
                )

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

    def _generate_iter(self) -> ast.FunctionDef:
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

    def _generate_iter_generator(self, output_stmts: list) -> ast.FunctionDef:
        """Generate __iter__ method as a generator function."""
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
            body=output_stmts,
            decorator_list=[],
            returns=None,
        )

    def _generate_next(self, ctx: CompilationContext, output_stmts: list) -> ast.FunctionDef:
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

    def _generate_reset(self, ctx: CompilationContext) -> ast.FunctionDef:
        """Generate reset method."""
        body = []

        for node in self.nodes:
            body.extend(node._get_reset_stmts(ctx))

        if not body:
            body = [ast.Pass()]

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