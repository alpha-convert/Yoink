from __future__ import annotations

from python_delta.event import CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
from python_delta.compilation import CompilationContext
from enum import Enum
from typing import List
import ast


class Done:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "Done"


DONE = Done()


class CatRState(Enum):
    """State machine for CatR operation."""
    FIRST_STREAM = 0   # Pulling from first stream (wrapped in CatEvA)
    SECOND_STREAM = 1  # Pulling from second stream (unwrapped)


class StreamOp:
    """Base class for stream operations."""
    def __init__(self, stream_type):
        self.stream_type = stream_type

    @property
    def id(self):
        """Compute structural ID from operation structure. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement id property")

    @property
    def vars(self):
        """Compute vars set from operation structure. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement vars property")

    def __str__(self):
        return f"{self.__class__.__name__}({self.stream_type})"

    def __iter__(self):
        return self

    def _pull(self):
        """Pull the next element from the stream.

        Returns:
            - A value (including None for skips)
            - DONE sentinel when stream is exhausted
        """
        raise NotImplementedError("Subclasses must implement _pull")

    def __next__(self):
        """Pull the next element from the stream. Raise StopIteration when exhausted."""
        result = self._pull()
        if result is DONE:
            raise StopIteration
        return result

    def reset(self):
        """Reset the stream to its initial state for reuse."""
        raise NotImplementedError("Subclasses must implement reset")

    def _compile_stmts(self, ctx: 'CompilationContext', dst: str) -> List[ast.stmt]:
        """
        Compile this node's _pull() logic to AST statements.

        Args:
            ctx: Compilation context with state allocation and child destinations
            dst: Name of destination variable to write result into

        Returns:
            List of AST statements that compute the pull and assign to dst

        The generated statements must:
        1. Execute the pull logic for this operation
        2. Assign the result to the variable named by `dst`
        3. The result must be a value, None (skip), or DONE (exhausted)

        Must be implemented by all subclasses.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement _compile_stmts")

    def _get_state_initializers(self, ctx: 'CompilationContext') -> List[tuple]:
        """
        Return list of (state_var_name, initial_value) for __init__.

        Default: empty list (stateless nodes like Var, Eps, UnsafeCast).
        Override for stateful nodes (CatR, SumInj, CaseOp, etc.).

        Args:
            ctx: Compilation context

        Returns:
            List of (state_var_name, initial_value) tuples
        """
        return []

    def _get_reset_stmts(self, ctx: 'CompilationContext') -> List[ast.stmt]:
        """
        Return list of AST statements to reset this node's state.

        Default: empty list (stateless nodes).
        Override for stateful nodes.

        Args:
            ctx: Compilation context

        Returns:
            List of AST assignment statements to reset state variables
        """
        return []
    
class Var(StreamOp):
    def __init__(self, name, stream_type):
        super().__init__(stream_type)
        self.name = name
        self.source = None

    @property
    def id(self):
        return hash(("Var", self.name))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"Var({self.name}: {self.stream_type})"

    def _pull(self):
        """Pull from the source iterator."""
        if self.source is None:
            raise RuntimeError(f"Var '{self.name}' has no source bound")
        try:
            return next(self.source)
        except StopIteration:
            return DONE

    def reset(self):
        pass

    def _compile_stmts(self, ctx: 'CompilationContext', dst: str) -> List[ast.stmt]:
        """Compile to: try: dst = next(self.inputs[idx]) except StopIteration: dst = DONE"""
        input_idx = ctx.var_to_input_idx[self.id]

        return [
            ast.Try(
                body=[
                    ast.Assign(
                        targets=[ast.Name(id=dst, ctx=ast.Store())],
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
                                targets=[ast.Name(id=dst, ctx=ast.Store())],
                                value=ast.Name(id='DONE', ctx=ast.Load())
                            )
                        ]
                    )
                ],
                orelse=[],
                finalbody=[]
            )
        ]


class Eps(StreamOp):
    def __init__(self, stream_type):
        super().__init__(stream_type)

    @property
    def id(self):
        return hash(("Eps", id(self)))

    @property
    def vars(self):
        return set()

    def __str__(self):
        return f"Eps({self.stream_type})"

    def _pull(self):
        return DONE

    def reset(self):
        pass

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Compile to: dst = DONE"""
        return [
            ast.Assign(
                targets=[ast.Name(id=dst, ctx=ast.Store())],
                value=ast.Name(id='DONE', ctx=ast.Load())
            )
        ]


class CatR(StreamOp):
    def __init__(self, s1, s2, stream_type):
        super().__init__(stream_type)
        self.input_streams = [s1, s2]
        self.current_state = CatRState.FIRST_STREAM

    @property
    def id(self):
        return hash(("CatR", self.input_streams[0].id, self.input_streams[1].id))

    @property
    def vars(self):
        return self.input_streams[0].vars | self.input_streams[1].vars

    def _pull(self):
        """Pull from first stream (wrapped in CatEvA), then CatPunc, then second stream (unwrapped)."""
        if self.current_state == CatRState.FIRST_STREAM:
            val = self.input_streams[0]._pull()
            if val is DONE:
                self.current_state = CatRState.SECOND_STREAM
                return CatPunc()
            if val is None:
                return None
            return CatEvA(val)
        else:
            return self.input_streams[1]._pull()

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.current_state = CatRState.FIRST_STREAM

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Compile CatR state machine to if/else with nested conditionals."""
        state_var = ctx.allocate_state(self, 'state')
        tmp = ctx.allocate_temp()

        # Compile children
        s1_stmts = self.input_streams[0]._compile_stmts(ctx, tmp)
        s2_stmts = self.input_streams[1]._compile_stmts(ctx, dst)

        # Build the state machine: if state == FIRST_STREAM: ... else: ...
        return [
            ast.If(
                test=ast.Compare(
                    left=ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=state_var,
                        ctx=ast.Load()
                    ),
                    ops=[ast.Eq()],
                    comparators=[ast.Constant(value=CatRState.FIRST_STREAM.value)]
                ),
                body=s1_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=ast.Name(id=tmp, ctx=ast.Load()),
                            ops=[ast.Is()],
                            comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                        ),
                        body=[
                            ast.Assign(
                                targets=[ast.Attribute(
                                    value=ast.Name(id='self', ctx=ast.Load()),
                                    attr=state_var,
                                    ctx=ast.Store()
                                )],
                                value=ast.Constant(value=CatRState.SECOND_STREAM.value)
                            ),
                            ast.Assign(
                                targets=[ast.Name(id=dst, ctx=ast.Store())],
                                value=ast.Call(
                                    func=ast.Name(id='CatPunc', ctx=ast.Load()),
                                    args=[],
                                    keywords=[]
                                )
                            )
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=ast.Name(id=tmp, ctx=ast.Load()),
                                    ops=[ast.Is()],
                                    comparators=[ast.Constant(value=None)]
                                ),
                                body=[
                                    ast.Assign(
                                        targets=[ast.Name(id=dst, ctx=ast.Store())],
                                        value=ast.Constant(value=None)
                                    )
                                ],
                                orelse=[
                                    ast.Assign(
                                        targets=[ast.Name(id=dst, ctx=ast.Store())],
                                        value=ast.Call(
                                            func=ast.Name(id='CatEvA', ctx=ast.Load()),
                                            args=[ast.Name(id=tmp, ctx=ast.Load())],
                                            keywords=[]
                                        )
                                    )
                                ]
                            )
                        ]
                    )
                ],
                orelse=s2_stmts
            )
        ]

    def _get_state_initializers(self, ctx: CompilationContext) -> List[tuple]:
        """Initialize state to FIRST_STREAM."""
        state_var = ctx.get_state_var(self, 'state')
        return [(state_var, CatRState.FIRST_STREAM.value)]

    def _get_reset_stmts(self, ctx: CompilationContext) -> List[ast.stmt]:
        """Reset state to FIRST_STREAM."""
        state_var = ctx.get_state_var(self, 'state')
        return [
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=state_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=CatRState.FIRST_STREAM.value)
            )
        ]


class CatProjCoordinator(StreamOp):
    """Coordinator for catl that manages shared state between two CatProj instances."""
    def __init__(self, input_stream, stream_type):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.seen_punc = False
        self.input_exhausted = False

    @property
    def id(self):
        return hash(("CatProjCoordinator", self.input_stream.id))

    @property
    def vars(self):
        return self.input_stream.vars

    def pull_for_position(self, position):
        """
        Pull the next event for the given position.

        For position 0: returns unwrapped CatEvA values until CatPunc
        For position 1: skips CatEvA events until CatPunc is seen, then returns unwrapped tail events
        """
        if self.input_exhausted:
            return DONE

        if position == 0 and self.seen_punc:
            return DONE

        event = self.input_stream._pull()
        if event is DONE:
            self.input_exhausted = True
            return DONE

        if position == 0:
            # Position 0: return CatEvA values, stop at CatPunc
            if isinstance(event, CatEvA):
                return event.value
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                return DONE
            elif event is None:
                return None
            else:
                # Shouldn't happen in position 0 before punc
                return None
        else:
            # Position 1: skip CatEvA events and CatPunc, return tail events
            if isinstance(event, CatEvA):
                return None  # Skip wrapped events
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                return None  # Skip the punc itself
            elif event is None:
                return None
            else:
                # After CatPunc, return unwrapped tail events
                return event

    def _pull(self):
        """Coordinators are not directly iterable."""
        raise NotImplementedError("CatProjCoordinator should not be iterated directly")

    def reset(self):
        self.seen_punc = False
        self.input_exhausted = False


class CatProj(StreamOp):
    """Projection from a TyCat stream."""
    def __init__(self, coordinator, stream_type, position):
        assert isinstance(coordinator,CatProjCoordinator)
        super().__init__(stream_type)
        self.coordinator = coordinator  # CatProjCoordinator instance
        self.position = position  # 0 or 1

    @property
    def id(self):
        return hash(("CatProj", self.coordinator.id, self.position))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"CatProj{self.position}({self.stream_type})"

    def _pull(self):
        return self.coordinator.pull_for_position(self.position)

    def reset(self):
        """Reset is handled by the coordinator."""
        pass  # Coordinator manages the state

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Inline coordinator logic with event filtering based on position."""
        coord = self.coordinator
        coord_id = coord.id

        # Allocate state for coordinator (shared between positions)
        if coord_id not in ctx.state_vars:
            seen_punc_var = ctx.allocate_state(coord, 'seen_punc')
            input_exhausted_var = ctx.allocate_state(coord, 'input_exhausted')
        else:
            seen_punc_var = ctx.get_state_var(coord, 'seen_punc')
            input_exhausted_var = ctx.get_state_var(coord, 'input_exhausted')

        event_tmp = ctx.allocate_temp()
        input_stmts = coord.input_stream._compile_stmts(ctx, event_tmp)

        if self.position == 0:
            # Position 0: extract CatEvA values until CatPunc
            return [
                ast.If(
                    test=ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=input_exhausted_var,
                        ctx=ast.Load()
                    ),
                    body=[
                        ast.Assign(
                            targets=[ast.Name(id=dst, ctx=ast.Store())],
                            value=ast.Name(id='DONE', ctx=ast.Load())
                        )
                    ],
                    orelse=[
                        ast.If(
                            test=ast.Attribute(
                                value=ast.Name(id='self', ctx=ast.Load()),
                                attr=seen_punc_var,
                                ctx=ast.Load()
                            ),
                            body=[
                                ast.Assign(
                                    targets=[ast.Name(id=dst, ctx=ast.Store())],
                                    value=ast.Name(id='DONE', ctx=ast.Load())
                                )
                            ],
                            orelse=input_stmts + [
                                ast.If(
                                    test=ast.Compare(
                                        left=ast.Name(id=event_tmp, ctx=ast.Load()),
                                        ops=[ast.Is()],
                                        comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                    ),
                                    body=[
                                        ast.Assign(
                                            targets=[ast.Attribute(
                                                value=ast.Name(id='self', ctx=ast.Load()),
                                                attr=input_exhausted_var,
                                                ctx=ast.Store()
                                            )],
                                            value=ast.Constant(value=True)
                                        ),
                                        ast.Assign(
                                            targets=[ast.Name(id=dst, ctx=ast.Store())],
                                            value=ast.Name(id='DONE', ctx=ast.Load())
                                        )
                                    ],
                                    orelse=[
                                        ast.If(
                                            test=ast.Call(
                                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                args=[
                                                    ast.Name(id=event_tmp, ctx=ast.Load()),
                                                    ast.Name(id='CatEvA', ctx=ast.Load())
                                                ],
                                                keywords=[]
                                            ),
                                            body=[
                                                ast.Assign(
                                                    targets=[ast.Name(id=dst, ctx=ast.Store())],
                                                    value=ast.Attribute(
                                                        value=ast.Name(id=event_tmp, ctx=ast.Load()),
                                                        attr='value',
                                                        ctx=ast.Load()
                                                    )
                                                )
                                            ],
                                            orelse=[
                                                ast.If(
                                                    test=ast.Call(
                                                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                        args=[
                                                            ast.Name(id=event_tmp, ctx=ast.Load()),
                                                            ast.Name(id='CatPunc', ctx=ast.Load())
                                                        ],
                                                        keywords=[]
                                                    ),
                                                    body=[
                                                        ast.Assign(
                                                            targets=[ast.Attribute(
                                                                value=ast.Name(id='self', ctx=ast.Load()),
                                                                attr=seen_punc_var,
                                                                ctx=ast.Store()
                                                            )],
                                                            value=ast.Constant(value=True)
                                                        ),
                                                        ast.Assign(
                                                            targets=[ast.Name(id=dst, ctx=ast.Store())],
                                                            value=ast.Name(id='DONE', ctx=ast.Load())
                                                        )
                                                    ],
                                                    orelse=[
                                                        ast.Assign(
                                                            targets=[ast.Name(id=dst, ctx=ast.Store())],
                                                            value=ast.Constant(value=None)
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
                )
            ]
        else:  # position == 1
            # Position 1: skip CatEvA and CatPunc, return tail events
            return [
                ast.If(
                    test=ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=input_exhausted_var,
                        ctx=ast.Load()
                    ),
                    body=[
                        ast.Assign(
                            targets=[ast.Name(id=dst, ctx=ast.Store())],
                            value=ast.Name(id='DONE', ctx=ast.Load())
                        )
                    ],
                    orelse=input_stmts + [
                        ast.If(
                            test=ast.Compare(
                                left=ast.Name(id=event_tmp, ctx=ast.Load()),
                                ops=[ast.Is()],
                                comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                            ),
                            body=[
                                ast.Assign(
                                    targets=[ast.Attribute(
                                        value=ast.Name(id='self', ctx=ast.Load()),
                                        attr=input_exhausted_var,
                                        ctx=ast.Store()
                                    )],
                                    value=ast.Constant(value=True)
                                ),
                                ast.Assign(
                                    targets=[ast.Name(id=dst, ctx=ast.Store())],
                                    value=ast.Name(id='DONE', ctx=ast.Load())
                                )
                            ],
                            orelse=[
                                ast.If(
                                    test=ast.Call(
                                        func=ast.Name(id='isinstance', ctx=ast.Load()),
                                        args=[
                                            ast.Name(id=event_tmp, ctx=ast.Load()),
                                            ast.Name(id='CatEvA', ctx=ast.Load())
                                        ],
                                        keywords=[]
                                    ),
                                    body=[
                                        ast.Assign(
                                            targets=[ast.Name(id=dst, ctx=ast.Store())],
                                            value=ast.Constant(value=None)
                                        )
                                    ],
                                    orelse=[
                                        ast.If(
                                            test=ast.Call(
                                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                args=[
                                                    ast.Name(id=event_tmp, ctx=ast.Load()),
                                                    ast.Name(id='CatPunc', ctx=ast.Load())
                                                ],
                                                keywords=[]
                                            ),
                                            body=[
                                                ast.Assign(
                                                    targets=[ast.Attribute(
                                                        value=ast.Name(id='self', ctx=ast.Load()),
                                                        attr=seen_punc_var,
                                                        ctx=ast.Store()
                                                    )],
                                                    value=ast.Constant(value=True)
                                                ),
                                                ast.Assign(
                                                    targets=[ast.Name(id=dst, ctx=ast.Store())],
                                                    value=ast.Constant(value=None)
                                                )
                                            ],
                                            orelse=[
                                                ast.Assign(
                                                    targets=[ast.Name(id=dst, ctx=ast.Store())],
                                                    value=ast.Name(id=event_tmp, ctx=ast.Load())
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

    def _get_state_initializers(self, ctx: CompilationContext) -> List[tuple]:
        """State is managed by coordinator, initialized once."""
        coord = self.coordinator

        # Only initialize coordinator state once, even though multiple CatProj use it
        if coord.id in ctx.state_vars:
            # Check if we haven't already returned these initializers
            init_marker = f'coord_init_{coord.id}'
            if init_marker not in ctx.compiled_nodes:
                ctx.compiled_nodes.add(init_marker)
                seen_punc_var = ctx.get_state_var(coord, 'seen_punc')
                input_exhausted_var = ctx.get_state_var(coord, 'input_exhausted')
                return [
                    (seen_punc_var, False),
                    (input_exhausted_var, False)
                ]

        return []

    def _get_reset_stmts(self, ctx: CompilationContext) -> List[ast.stmt]:
        """Reset coordinator state (only generate once for first CatProj)."""
        coord = self.coordinator
        if coord.id not in ctx.state_vars:
            # This shouldn't happen if _get_state_initializers was called
            return []

        # Only reset if this is the first CatProj we're processing
        # Actually, both CatProj instances will try to reset, which is fine
        # since they reset the same state variables
        seen_punc_var = ctx.get_state_var(coord, 'seen_punc')
        input_exhausted_var = ctx.get_state_var(coord, 'input_exhausted')
        return [
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=seen_punc_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=False)
            ),
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=input_exhausted_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=False)
            )
        ]


class SumInj(StreamOp):
    """Sum injection - emits PlusPuncA (position=0) or PlusPuncB (position=1) tag followed by input stream values."""
    def __init__(self, input_stream, stream_type, position):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.position = position  # 0 for left (PlusPuncA), 1 for right (PlusPuncB)
        self.tag_emitted = False

    @property
    def id(self):
        return hash(("SumInj", self.input_stream.id, self.position))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        """Emit tag first (PlusPuncA if position=0, PlusPuncB if position=1), then pull from input stream."""
        if not self.tag_emitted:
            self.tag_emitted = True
            return PlusPuncA() if self.position == 0 else PlusPuncB()
        return self.input_stream._pull()

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.tag_emitted = False

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Compile tag emission then delegation."""
        tag_var = ctx.allocate_state(self, 'tag_emitted')
        input_stmts = self.input_stream._compile_stmts(ctx, dst)

        tag_class = 'PlusPuncA' if self.position == 0 else 'PlusPuncB'

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=tag_var,
                        ctx=ast.Load()
                    )
                ),
                body=[
                    ast.Assign(
                        targets=[ast.Attribute(
                            value=ast.Name(id='self', ctx=ast.Load()),
                            attr=tag_var,
                            ctx=ast.Store()
                        )],
                        value=ast.Constant(value=True)
                    ),
                    ast.Assign(
                        targets=[ast.Name(id=dst, ctx=ast.Store())],
                        value=ast.Call(
                            func=ast.Name(id=tag_class, ctx=ast.Load()),
                            args=[],
                            keywords=[]
                        )
                    )
                ],
                orelse=input_stmts
            )
        ]

    def _get_state_initializers(self, ctx: CompilationContext) -> List[tuple]:
        """Initialize tag_emitted to False."""
        tag_var = ctx.get_state_var(self, 'tag_emitted')
        return [(tag_var, False)]

    def _get_reset_stmts(self, ctx: CompilationContext) -> List[ast.stmt]:
        """Reset tag_emitted to False."""
        tag_var = ctx.get_state_var(self, 'tag_emitted')
        return [
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=tag_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=False)
            )
        ]


class CaseOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, input_stream, left_branch, right_branch, stream_type):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.branches = [left_branch,right_branch] # StreamOp that produces output
        self.active_branch = -1
        self.tag_read = False

    @property
    def id(self):
        return hash(("CaseOp", self.input_stream.id, self.branches[0].id, self.branches[1].id))

    @property
    def vars(self):
        return self.input_stream.vars | self.branches[0].vars | self.branches[1].vars

    def _pull(self):
        """Read tag and route to appropriate branch."""
        if not self.tag_read:
            tag = self.input_stream._pull()
            if tag is None:
                return None
            if tag is DONE:
                return DONE
            self.tag_read = True

            if isinstance(tag, PlusPuncA):
                self.active_branch = 0
            elif isinstance(tag, PlusPuncB):
                self.active_branch = 1
            else:
                raise RuntimeError(f"Expected PlusPuncA or PlusPuncB tag, got {tag}")
            return None

        if self.active_branch == -1:
            raise RuntimeError("CaseOp._pull() called before tag was read")
        return self.branches[self.active_branch]._pull()

    def reset(self):
        """Reset state and recursively reset branches."""
        self.tag_read = False
        self.active_branch = None

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Compile tag reading and branch routing."""
        tag_read_var = ctx.allocate_state(self, 'tag_read')
        active_branch_var = ctx.allocate_state(self, 'active_branch')

        tag_tmp = ctx.allocate_temp()
        input_stmts = self.input_stream._compile_stmts(ctx, tag_tmp)

        branch0_stmts = self.branches[0]._compile_stmts(ctx, dst)
        branch1_stmts = self.branches[1]._compile_stmts(ctx, dst)

        # Build nested if/elif structure for tag reading
        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=tag_read_var,
                        ctx=ast.Load()
                    )
                ),
                body=input_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=ast.Name(id=tag_tmp, ctx=ast.Load()),
                            ops=[ast.Is()],
                            comparators=[ast.Constant(value=None)]
                        ),
                        body=[
                            ast.Assign(
                                targets=[ast.Name(id=dst, ctx=ast.Store())],
                                value=ast.Constant(value=None)
                            )
                        ],
                        orelse=[
                            ast.If(
                                test=ast.Compare(
                                    left=ast.Name(id=tag_tmp, ctx=ast.Load()),
                                    ops=[ast.Is()],
                                    comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                ),
                                body=[
                                    ast.Assign(
                                        targets=[ast.Name(id=dst, ctx=ast.Store())],
                                        value=ast.Name(id='DONE', ctx=ast.Load())
                                    )
                                ],
                                orelse=[
                                    # Set tag_read = True
                                    ast.Assign(
                                        targets=[ast.Attribute(
                                            value=ast.Name(id='self', ctx=ast.Load()),
                                            attr=tag_read_var,
                                            ctx=ast.Store()
                                        )],
                                        value=ast.Constant(value=True)
                                    ),
                                    # Check tag type and set active_branch
                                    ast.If(
                                        test=ast.Call(
                                            func=ast.Name(id='isinstance', ctx=ast.Load()),
                                            args=[
                                                ast.Name(id=tag_tmp, ctx=ast.Load()),
                                                ast.Name(id='PlusPuncA', ctx=ast.Load())
                                            ],
                                            keywords=[]
                                        ),
                                        body=[
                                            ast.Assign(
                                                targets=[ast.Attribute(
                                                    value=ast.Name(id='self', ctx=ast.Load()),
                                                    attr=active_branch_var,
                                                    ctx=ast.Store()
                                                )],
                                                value=ast.Constant(value=0)
                                            )
                                        ],
                                        orelse=[
                                            ast.If(
                                                test=ast.Call(
                                                    func=ast.Name(id='isinstance', ctx=ast.Load()),
                                                    args=[
                                                        ast.Name(id=tag_tmp, ctx=ast.Load()),
                                                        ast.Name(id='PlusPuncB', ctx=ast.Load())
                                                    ],
                                                    keywords=[]
                                                ),
                                                body=[
                                                    ast.Assign(
                                                        targets=[ast.Attribute(
                                                            value=ast.Name(id='self', ctx=ast.Load()),
                                                            attr=active_branch_var,
                                                            ctx=ast.Store()
                                                        )],
                                                        value=ast.Constant(value=1)
                                                    )
                                                ],
                                                orelse=[
                                                    ast.Raise(
                                                        exc=ast.Call(
                                                            func=ast.Name(id='RuntimeError', ctx=ast.Load()),
                                                            args=[
                                                                ast.JoinedStr(values=[
                                                                    ast.Constant(value='Expected PlusPuncA or PlusPuncB tag, got '),
                                                                    ast.FormattedValue(
                                                                        value=ast.Name(id=tag_tmp, ctx=ast.Load()),
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
                                    ast.Assign(
                                        targets=[ast.Name(id=dst, ctx=ast.Store())],
                                        value=ast.Constant(value=None)
                                    )
                                ]
                            )
                        ]
                    )
                ],
                orelse=[
                    # Route to appropriate branch
                    ast.If(
                        test=ast.Compare(
                            left=ast.Attribute(
                                value=ast.Name(id='self', ctx=ast.Load()),
                                attr=active_branch_var,
                                ctx=ast.Load()
                            ),
                            ops=[ast.Eq()],
                            comparators=[ast.Constant(value=0)]
                        ),
                        body=branch0_stmts,
                        orelse=branch1_stmts
                    )
                ]
            )
        ]

    def _get_state_initializers(self, ctx: CompilationContext) -> List[tuple]:
        """Initialize tag_read and active_branch."""
        tag_read_var = ctx.get_state_var(self, 'tag_read')
        active_branch_var = ctx.get_state_var(self, 'active_branch')
        return [
            (tag_read_var, False),
            (active_branch_var, -1)
        ]

    def _get_reset_stmts(self, ctx: CompilationContext) -> List[ast.stmt]:
        """Reset tag_read and active_branch."""
        tag_read_var = ctx.get_state_var(self, 'tag_read')
        active_branch_var = ctx.get_state_var(self, 'active_branch')
        return [
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=tag_read_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=False)
            ),
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=active_branch_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=-1)
            )
        ]


class SinkThen(StreamOp):
    """Sink operation - pulls from first stream until exhausted, then switches to second stream."""
    def __init__(self, first_stream, second_stream, stream_type):
        super().__init__(stream_type)
        self.input_streams = [first_stream, second_stream]
        self.first_exhausted = False

    @property
    def id(self):
        return hash(("SinkThen", self.input_streams[0].id, self.input_streams[1].id))

    @property
    def vars(self):
        return self.input_streams[0].vars | self.input_streams[1].vars

    def _pull(self):
        """Pull from first stream until exhausted, then switch to second stream."""
        if not self.first_exhausted:
            # Pull from first stream and drop the value (sink it)
            val = self.input_streams[0]._pull()
            if val is DONE:
                # First stream exhausted, switch to second
                self.first_exhausted = True
                # Fall through to pull from second stream
            else:
                return None  # Drop all values from first stream

        # Pull from second stream
        return self.input_streams[1]._pull()

    def reset(self):
        """Reset state."""
        self.first_exhausted = False

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Compile exhaust-first-then-second logic."""
        exhausted_var = ctx.allocate_state(self, 'first_exhausted')

        val_tmp = ctx.allocate_temp()
        s1_stmts = self.input_streams[0]._compile_stmts(ctx, val_tmp)
        s2_stmts = self.input_streams[1]._compile_stmts(ctx, dst)

        return [
            ast.If(
                test=ast.UnaryOp(
                    op=ast.Not(),
                    operand=ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=exhausted_var,
                        ctx=ast.Load()
                    )
                ),
                body=s1_stmts + [
                    ast.If(
                        test=ast.Compare(
                            left=ast.Name(id=val_tmp, ctx=ast.Load()),
                            ops=[ast.Is()],
                            comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                        ),
                        body=[
                            ast.Assign(
                                targets=[ast.Attribute(
                                    value=ast.Name(id='self', ctx=ast.Load()),
                                    attr=exhausted_var,
                                    ctx=ast.Store()
                                )],
                                value=ast.Constant(value=True)
                            )
                        ] + s2_stmts,
                        orelse=[
                            ast.Assign(
                                targets=[ast.Name(id=dst, ctx=ast.Store())],
                                value=ast.Constant(value=None)
                            )
                        ]
                    )
                ],
                orelse=s2_stmts
            )
        ]

    def _get_state_initializers(self, ctx: CompilationContext) -> List[tuple]:
        """Initialize first_exhausted to False."""
        exhausted_var = ctx.get_state_var(self, 'first_exhausted')
        return [(exhausted_var, False)]

    def _get_reset_stmts(self, ctx: CompilationContext) -> List[ast.stmt]:
        """Reset first_exhausted to False."""
        exhausted_var = ctx.get_state_var(self, 'first_exhausted')
        return [
            ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='self', ctx=ast.Load()),
                    attr=exhausted_var,
                    ctx=ast.Store()
                )],
                value=ast.Constant(value=False)
            )
        ]



class ResetOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, reset_set, stream_type):
        super().__init__(stream_type)
        self.reset_set = reset_set

    @property
    def id(self):
        return hash(("ResetOp", *map(lambda n: id(n),self.reset_set)))

    @property
    def vars(self):
        return set()

    def _pull(self):
        for node in self.reset_set:
            node.reset()
        return None

    def reset(self):
        pass

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Compile reset calls on all nodes in reset_set."""
        reset_stmts = []

        # Generate reset statements for each node in the reset set
        # In compiled code, nodes don't exist as separate objects - their state is flattened
        # So we need to inline the reset logic from each node's _get_reset_stmts
        for node in self.reset_set:
            reset_stmts.extend(node._get_reset_stmts(ctx))

        # Set dst = None
        reset_stmts.append(
            ast.Assign(
                targets=[ast.Name(id=dst, ctx=ast.Store())],
                value=ast.Constant(value=None)
            )
        )

        return reset_stmts


class UnsafeCast(StreamOp):
    """Unsafe cast - forwards data from input stream with a different type annotation."""
    def __init__(self, input_stream, target_type):
        super().__init__(target_type)
        self.input_stream = input_stream

    @property
    def id(self):
        return hash(("UnsafeCast", self.input_stream.id, str(self.stream_type)))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        """Forward data from input stream without modification."""
        return self.input_stream._pull()

    def reset(self):
        pass

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Passthrough - just compile child to same destination."""
        return self.input_stream._compile_stmts(ctx, dst)