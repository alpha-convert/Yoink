"""Direct compilation strategy - compiles to state machine with explicit result variable."""

from __future__ import annotations
from typing import List, TYPE_CHECKING
import ast

from python_delta.compilation.compiler_visitor import CompilerVisitor
from python_delta.compilation import CompilationContext, StateVar
from python_delta.compilation.reset_visitor import ResetVisitor

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
    from python_delta.stream_ops.resetblockenclosing import ResetBlockEnclosingOp


class DirectCompiler(CompilerVisitor):
    """Direct compilation: state machine with explicit result variable.

    Generates code that assigns to a destination variable (dst).
    """

    def __init__(self, ctx, dst: StateVar):
        super().__init__(ctx)
        self.dst = dst

    @staticmethod
    def compile(dataflow_graph) -> type:
        """Compile a dataflow graph using direct compilation.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The compiled class (not an instance)
        """
        module_ast = DirectCompiler._generate_module_ast(dataflow_graph)

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
        module_ast = DirectCompiler._generate_module_ast(dataflow_graph)
        return ast.unparse(module_ast)

    @staticmethod
    def _generate_module_ast(dataflow_graph) -> ast.Module:
        """Generate the complete module AST for direct compilation.

        Args:
            dataflow_graph: The DataflowGraph to compile

        Returns:
            The module AST
        """
        ctx = CompilationContext()

        # Map input vars to their indices
        ctx.var_to_input_idx = {var.id: i for i, var in enumerate(dataflow_graph.input_vars)}

        # Compile the output node
        result_var = StateVar('result', tmp=True)
        compiler = DirectCompiler(ctx, result_var)
        output_stmts = dataflow_graph.outputs.accept(compiler)

        # Generate the class AST
        class_ast = DirectCompiler._generate_class_ast(dataflow_graph, ctx, output_stmts)

        # Create and return the module AST
        module_ast = ast.Module(body=[class_ast], type_ignores=[])
        ast.fix_missing_locations(module_ast)
        return module_ast

    @staticmethod
    def _generate_class_ast(dataflow_graph, ctx: CompilationContext, output_stmts: List[ast.stmt]) -> ast.ClassDef:
        """Generate the complete FlattenedIterator class for direct compilation."""
        body = [
            DirectCompiler._generate_init(dataflow_graph, ctx),
            DirectCompiler._generate_iter(),
            DirectCompiler._generate_next(ctx, output_stmts),
            DirectCompiler._generate_reset(dataflow_graph, ctx),
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

        # Add state initializers from all nodes (use ResetVisitor since initializing = resetting to initial state)
        reset_visitor = ResetVisitor(ctx)
        for node in dataflow_graph.nodes:
            body.extend(reset_visitor.visit(node))

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
        from python_delta.compilation.reset_visitor import ResetVisitor

        visitor = ResetVisitor(ctx)
        body = []

        for node in dataflow_graph.nodes:
            body.extend(visitor.visit(node))

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

    def visit_Var(self, node: 'Var') -> List[ast.stmt]:
        """Compile to: try: dst = next(self.inputs[idx]) except StopIteration: dst = DONE"""
        input_idx = self.ctx.var_to_input_idx[node.id]

        return [
            ast.Try(
                body=[
                    ast.Assign(
                        targets=[self.dst.lvalue()],
                        value=ast.Call(
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
                    )
                ],
                handlers=[
                    ast.ExceptHandler(
                        type=ast.Name(id='StopIteration', ctx=ast.Load()),
                        name=None,
                        body=[
                            ast.Assign(
                                targets=[self.dst.lvalue()],
                                value=ast.Name(id='DONE', ctx=ast.Load())
                            )
                        ]
                    )
                ],
                orelse=[],
                finalbody=[]
            )
        ]

    def visit_Eps(self, node: 'Eps') -> List[ast.stmt]:
        """Eps immediately returns DONE."""
        return [
            ast.Assign(
                targets=[self.dst.lvalue()],
                value=ast.Name(id='DONE', ctx=ast.Load())
            )
        ]

    def visit_SingletonOp(self, node: 'SingletonOp') -> List[ast.stmt]:
        """Emit value once, then DONE."""
        exhausted_var = self.ctx.state_var(node, 'exhausted')

        return [
            ast.If(
                test=exhausted_var.rvalue(),
                body=[
                    self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                ],
                orelse=[
                    exhausted_var.assign(ast.Constant(value=True)),
                    self.dst.assign(ast.Call(
                        func=ast.Name(id='BaseEvent', ctx=ast.Load()),
                        args=[ast.Constant(value=node.value)],
                        keywords=[]
                    ))
                ]
            )
        ]

    def visit_ResetOp(self, node: 'ResetOp') -> List[ast.stmt]:
        """Compile reset calls on all nodes in reset_set."""
        from python_delta.compilation.reset_visitor import ResetVisitor

        visitor = ResetVisitor(self.ctx)
        reset_stmts = []

        for reset_node in node.reset_set:
            reset_stmts.extend(visitor.visit(reset_node))

        reset_stmts.append(
            ast.Assign(
                targets=[self.dst.lvalue()],
                value=ast.Constant(value=None)
            )
        )

        return reset_stmts

    def visit_SumInj(self, node: 'SumInj') -> List[ast.stmt]:
        """Emit tag, then compile input stream."""
        tag_var = self.ctx.state_var(node, 'tag_emitted')

        tag_class = 'PlusPuncA' if node.position == 0 else 'PlusPuncB'

        input_compiler = DirectCompiler(self.ctx, self.dst)
        input_stmts = node.input_stream.accept(input_compiler)

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=tag_var.rvalue()
                ),
                body=[
                    tag_var.assign(ast.Constant(value=True)),
                    self.dst.assign(ast.Call(
                        func=ast.Name(id=tag_class, ctx=ast.Load()),
                        args=[],
                        keywords=[]
                    ))
                ],
                orelse=input_stmts
            )
        ]

    def visit_UnsafeCast(self, node: 'UnsafeCast') -> List[ast.stmt]:
        """Pass through to input stream."""
        input_compiler = DirectCompiler(self.ctx, self.dst)
        return node.input_stream.accept(input_compiler)

    def visit_CatR(self, node: 'CatR') -> List[ast.stmt]:
        """Compile CatR state machine."""
        from python_delta.stream_ops.catr import CatRState

        state_var = self.ctx.state_var(node, 'state')
        tmp = self.ctx.allocate_temp()

        # Compile children
        s1_compiler = DirectCompiler(self.ctx, tmp)
        s1_stmts = node.input_streams[0].accept(s1_compiler)

        s2_compiler = DirectCompiler(self.ctx, self.dst)
        s2_stmts = node.input_streams[1].accept(s2_compiler)

        # Build the state machine
        return [
            ast.If(
                test=ast.Compare(
                    left=state_var.rvalue(),
                    ops=[ast.Eq()],
                    comparators=[ast.Constant(value=CatRState.FIRST_STREAM.value)]
                ),
                body=s1_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=tmp.rvalue(),
                            ops=[ast.Is()],
                            comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                        ),
                        body=[
                            state_var.assign(ast.Constant(value=CatRState.SECOND_STREAM.value)),
                            self.dst.assign(ast.Call(
                                func=ast.Name(id='CatPunc', ctx=ast.Load()),
                                args=[],
                                keywords=[]
                            ))
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=tmp.rvalue(),
                                    ops=[ast.Is()],
                                    comparators=[ast.Constant(value=None)]
                                ),
                                body=[
                                    self.dst.assign(ast.Constant(value=None))
                                ],
                                orelse=[
                                    self.dst.assign(ast.Call(
                                        func=ast.Name(id='CatEvA', ctx=ast.Load()),
                                        args=[tmp.rvalue()],
                                        keywords=[]
                                    ))
                                ]
                            )
                        ]
                    )
                ],
                orelse=s2_stmts
            )
        ]

    def visit_CatProj(self, node: 'CatProj') -> List[ast.stmt]:
        """Inline coordinator logic with event filtering based on position."""
        coord = node.coordinator
        coord_id = coord.id

        seen_punc_var = self.ctx.state_var(coord, 'seen_punc')
        input_exhausted_var = self.ctx.state_var(coord, 'input_exhausted')

        event_tmp = self.ctx.allocate_temp()
        input_compiler = DirectCompiler(self.ctx, event_tmp)
        input_stmts = coord.input_stream.accept(input_compiler)

        if node.position == 0:
            # Position 0: extract CatEvA values until CatPunc
            return [
                ast.If(
                    test=input_exhausted_var.rvalue(),
                    body=[
                        self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                    ],
                    orelse=[
                        ast.If(
                            test=seen_punc_var.rvalue(),
                            body=[
                                self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                            ],
                            orelse=input_stmts + [
                                ast.If(
                                    test=ast.Compare(
                                        left=event_tmp.rvalue(),
                                        ops=[ast.Is()],
                                        comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                    ),
                                    body=[
                                        input_exhausted_var.assign(ast.Constant(value=True)),
                                        self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                                    ],
                                    orelse=[
                                        ast.If(
                                            test=ast.Call(
                                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                args=[
                                                    event_tmp.rvalue(),
                                                    ast.Name(id='CatEvA', ctx=ast.Load())
                                                ],
                                                keywords=[]
                                            ),
                                            body=[
                                                self.dst.assign(ast.Attribute(
                                                    value=event_tmp.rvalue(),
                                                    attr='value',
                                                    ctx=ast.Load()
                                                ))
                                            ],
                                            orelse=[
                                                ast.If(
                                                    test=ast.Call(
                                                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                        args=[
                                                            event_tmp.rvalue(),
                                                            ast.Name(id='CatPunc', ctx=ast.Load())
                                                        ],
                                                        keywords=[]
                                                    ),
                                                    body=[
                                                        seen_punc_var.assign(ast.Constant(value=True)),
                                                        self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                                                    ],
                                                    orelse=[
                                                        self.dst.assign(ast.Constant(value=None))
                                                    ]
                                                )
                                            ]
                                        )
                                    ]
                                )
                            ]
                        )
                    ]
                )
            ]
        else:  # position == 1
            # Position 1: skip events until CatPunc, then pass through all tail events
            return [
                ast.If(
                    test=input_exhausted_var.rvalue(),
                    body=[
                        self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                    ],
                    orelse=input_stmts + [
                        ast.If(
                            test=ast.Compare(
                                left=event_tmp.rvalue(),
                                ops=[ast.Is()],
                                comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                            ),
                            body=[
                                input_exhausted_var.assign(ast.Constant(value=True)),
                                self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                            ],
                            orelse=[
                                # Check if we've seen punc yet
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
                                                args=[
                                                    event_tmp.rvalue(),
                                                    ast.Name(id='CatEvA', ctx=ast.Load())
                                                ],
                                                keywords=[]
                                            ),
                                            body=[
                                                self.dst.assign(ast.Constant(value=None))
                                            ],
                                            orelse=[
                                                ast.If(
                                                    test=ast.Call(
                                                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                        args=[
                                                            event_tmp.rvalue(),
                                                            ast.Name(id='CatPunc', ctx=ast.Load())
                                                        ],
                                                        keywords=[]
                                                    ),
                                                    body=[
                                                        seen_punc_var.assign(ast.Constant(value=True)),
                                                        self.dst.assign(ast.Constant(value=None))
                                                    ],
                                                    orelse=[
                                                        self.dst.assign(ast.Constant(value=None))
                                                    ]
                                                )
                                            ]
                                        )
                                    ],
                                    orelse=[
                                        # After punc: pass through all events
                                        self.dst.assign(event_tmp.rvalue())
                                    ]
                                )
                            ]
                        )
                    ]
                )
            ]

    def visit_CaseOp(self, node: 'CaseOp') -> List[ast.stmt]:
        """Compile tag reading and branch routing."""
        tag_read_var = self.ctx.state_var(node, 'tag_read')
        active_branch_var = self.ctx.state_var(node, 'active_branch')

        tag_tmp = self.ctx.allocate_temp()
        input_compiler = DirectCompiler(self.ctx, tag_tmp)
        input_stmts = node.input_stream.accept(input_compiler)

        branch0_compiler = DirectCompiler(self.ctx, self.dst)
        branch0_stmts = node.branches[0].accept(branch0_compiler)

        branch1_compiler = DirectCompiler(self.ctx, self.dst)
        branch1_stmts = node.branches[1].accept(branch1_compiler)

        # Build nested if/elif structure for tag reading
        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=tag_read_var.rvalue()
                ),
                body=input_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=tag_tmp.rvalue(),
                            ops=[ast.Is()],
                            comparators=[ast.Constant(value=None)]
                        ),
                        body=[
                            self.dst.assign(ast.Constant(value=None))
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=tag_tmp.rvalue(),
                                    ops=[ast.Is()],
                                    comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                ),
                                body=[
                                    self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                                ],
                                orelse=[
                                    # Set tag_read = True
                                    tag_read_var.assign(ast.Constant(value=True)),
                                    # Check tag type and set active_branch
                                    ast.If(
                                        test=ast.Call(
                                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                                            args=[
                                                tag_tmp.rvalue(),
                                                ast.Name(id='PlusPuncA', ctx=ast.Load())
                                            ],
                                            keywords=[]
                                        ),
                                        body=[
                                            active_branch_var.assign(ast.Constant(value=0))
                                        ],
                                        orelse=[
                                            ast.If(
                                                test=ast.Call(
                                                    func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                    args=[
                                                        tag_tmp.rvalue(),
                                                        ast.Name(id='PlusPuncB', ctx=ast.Load())
                                                    ],
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
                                                                    ast.FormattedValue(
                                                                        value=tag_tmp.rvalue(),
                                                                        conversion=-1,
                                                                        format_spec=None
                                                                    )
                                                                ])
                                                            ],
                                                            keywords=[]
                                                        ),
                                                        cause=None
                                                    )
                                                ]
                                            )
                                        ]
                                    ),
                                    # Set dst = None
                                    self.dst.assign(ast.Constant(value=None))
                                ]
                            )
                        ]
                    )
                ],
                orelse=[
                    # Route to appropriate branch
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
        """Compile exhaust-first-then-second logic."""
        exhausted_var = self.ctx.state_var(node, 'first_exhausted')

        val_tmp = self.ctx.allocate_temp()
        s1_compiler = DirectCompiler(self.ctx, val_tmp)
        s1_stmts = node.input_streams[0].accept(s1_compiler)

        s2_compiler = DirectCompiler(self.ctx, self.dst)
        s2_stmts = node.input_streams[1].accept(s2_compiler)

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=exhausted_var.rvalue()
                ),
                body=s1_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=val_tmp.rvalue(),
                            ops=[ast.Is()],
                            comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                        ),
                        body=[
                            ast.Assign(
                                targets=[exhausted_var.lvalue()],
                                value=ast.Constant(value=True)
                            )
                        ] + s2_stmts,
                        orelse=[
                            ast.Assign(
                                targets=[self.dst.lvalue()],
                                value=ast.Constant(value=None)
                            )
                        ]
                    )
                ],
                orelse=s2_stmts
            )
        ]

    def visit_CondOp(self, node: 'CondOp') -> List[ast.stmt]:
        """Compile boolean condition reading and branch routing."""
        active_branch_var = self.ctx.state_var(node, 'active_branch')

        cond_tmp = self.ctx.allocate_temp()
        cond_compiler = DirectCompiler(self.ctx, cond_tmp)
        cond_stmts = node.cond_stream.accept(cond_compiler)

        branch0_compiler = DirectCompiler(self.ctx, self.dst)
        branch0_stmts = node.branches[0].accept(branch0_compiler)

        branch1_compiler = DirectCompiler(self.ctx, self.dst)
        branch1_stmts = node.branches[1].accept(branch1_compiler)

        # Build nested if structure for condition reading
        return [
            ast.If(
                test=ast.Compare(
                    left=active_branch_var.rvalue(),
                    ops=[ast.Is()],
                    comparators=[ast.Constant(value=None)]
                ),
                body=cond_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=cond_tmp.rvalue(),
                            ops=[ast.Is()],
                            comparators=[ast.Constant(value=None)]
                        ),
                        body=[
                            self.dst.assign(ast.Constant(value=None))
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=cond_tmp.rvalue(),
                                    ops=[ast.Is()],
                                    comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                ),
                                body=[
                                    self.dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                                ],
                                orelse=[
                                    # Extract boolean value and set active_branch
                                    ast.If(
                                        test=ast.Attribute(
                                            value=cond_tmp.rvalue(),
                                            attr='value',
                                            ctx=ast.Load()
                                        ),
                                        body=[
                                            active_branch_var.assign(ast.Constant(value=0))
                                        ],
                                        orelse=[
                                            active_branch_var.assign(ast.Constant(value=1))
                                        ]
                                    ),
                                    # Set dst = None
                                    self.dst.assign(ast.Constant(value=None))
                                ]
                            )
                        ]
                    )
                ],
                orelse=[
                    # Route to appropriate branch
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

    def visit_ResetBlockEnclosingOp(self, node: 'ResetBlockEnclosingOp') -> List[ast.stmt]:
        return self.visit(node.block_contents)