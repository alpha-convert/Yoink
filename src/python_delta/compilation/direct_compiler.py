"""Direct compilation strategy - compiles to state machine with explicit result variable."""

from __future__ import annotations
from typing import List, TYPE_CHECKING
import ast

from python_delta.compilation.compiler_visitor import CompilerVisitor
from python_delta.compilation import StateVar

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


class DirectCompiler(CompilerVisitor):
    """Direct compilation: state machine with explicit result variable.

    Generates code that assigns to a destination variable (dst).
    """

    def __init__(self, ctx, dst: StateVar):
        super().__init__(ctx)
        self.dst = dst

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
        reset_stmts = []

        for reset_node in node.reset_set:
            reset_stmts.extend(reset_node._get_reset_stmts(self.ctx))

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

        # Allocate state for coordinator (shared between positions)
        if coord_id not in self.ctx.state_vars:
            seen_punc_var = self.ctx.state_var(coord, 'seen_punc')
            input_exhausted_var = self.ctx.state_var(coord, 'input_exhausted')
        else:
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
            # Position 1: skip CatEvA and CatPunc, return tail events
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
                                                self.dst.assign(event_tmp.rvalue())
                                            ]
                                        )
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
