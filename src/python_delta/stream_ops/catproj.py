"""CatProj StreamOp - project from concatenated stream."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.event import CatEvA, CatPunc, PlusPuncA, PlusPuncB
from python_delta.compilation import StateVar


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

    def _compile_stmts(self, ctx, dst: StateVar) -> List[ast.stmt]:
        """Inline coordinator logic with event filtering based on position."""
        coord = self.coordinator
        coord_id = coord.id

        # Allocate state for coordinator (shared between positions)
        if coord_id not in ctx.state_vars:
            seen_punc_var = ctx.state_var(coord, 'seen_punc')
            input_exhausted_var = ctx.state_var(coord, 'input_exhausted')
        else:
            seen_punc_var = ctx.state_var(coord, 'seen_punc')
            input_exhausted_var = ctx.state_var(coord, 'input_exhausted')

        event_tmp = ctx.allocate_temp()
        input_stmts = coord.input_stream._compile_stmts(ctx, event_tmp)

        if self.position == 0:
            # Position 0: extract CatEvA values until CatPunc
            return [
                ast.If(
                    test=input_exhausted_var.rvalue(),
                    body=[
                        ast.Assign(
                            targets=[dst.lvalue()],
                            value=ast.Name(id='DONE', ctx=ast.Load())
                        )
                    ],
                    orelse=[
                        ast.If(
                            test=seen_punc_var.rvalue(),
                            body=[
                                ast.Assign(
                                    targets=[dst.lvalue()],
                                    value=ast.Name(id='DONE', ctx=ast.Load())
                                )
                            ],
                            orelse=input_stmts + [
                                ast.If(
                                    test=ast.Compare(
                                        left=event_tmp.rvalue(),
                                        ops=[ast.Is()],
                                        comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                                    ),
                                    body=[
                                        ast.Assign(
                                            targets=[input_exhausted_var.lvalue()],
                                            value=ast.Constant(value=True)
                                        ),
                                        ast.Assign(
                                            targets=[dst.lvalue()],
                                            value=ast.Name(id='DONE', ctx=ast.Load())
                                        )
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
                                                ast.Assign(
                                                    targets=[dst.lvalue()],
                                                    value=ast.Attribute(
                                                        value=event_tmp.rvalue(),
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
                                                            event_tmp.rvalue(),
                                                            ast.Name(id='CatPunc', ctx=ast.Load())
                                                        ],
                                                        keywords=[]
                                                    ),
                                                    body=[
                                                        ast.Assign(
                                                            targets=[seen_punc_var.lvalue()],
                                                            value=ast.Constant(value=True)
                                                        ),
                                                        ast.Assign(
                                                            targets=[dst.lvalue()],
                                                            value=ast.Name(id='DONE', ctx=ast.Load())
                                                        )
                                                    ],
                                                    orelse=[
                                                        ast.Assign(
                                                            targets=[dst.lvalue()],
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
                    test=input_exhausted_var.rvalue(),
                    body=[
                        ast.Assign(
                            targets=[dst.lvalue()],
                            value=ast.Name(id='DONE', ctx=ast.Load())
                        )
                    ],
                    orelse=input_stmts + [
                        ast.If(
                            test=ast.Compare(
                                left=event_tmp.rvalue(),
                                ops=[ast.Is()],
                                comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                            ),
                            body=[
                                ast.Assign(
                                    targets=[input_exhausted_var.lvalue()],
                                    value=ast.Constant(value=True)
                                ),
                                ast.Assign(
                                    targets=[dst.lvalue()],
                                    value=ast.Name(id='DONE', ctx=ast.Load())
                                )
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
                                        ast.Assign(
                                            targets=[dst.lvalue()],
                                            value=ast.Constant(value=None)
                                        )
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
                                                ast.Assign(
                                                    targets=[seen_punc_var.lvalue()],
                                                    value=ast.Constant(value=True)
                                                ),
                                                ast.Assign(
                                                    targets=[dst.lvalue()],
                                                    value=ast.Constant(value=None)
                                                )
                                            ],
                                            orelse=[
                                                ast.Assign(
                                                    targets=[dst.lvalue()],
                                                    value=event_tmp.rvalue()
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

    def _get_state_initializers(self, ctx) -> List[tuple]:
        """State is managed by coordinator, initialized once."""
        coord = self.coordinator

        # Only initialize coordinator state once, even though multiple CatProj use it
        if coord.id in ctx.state_vars:
            # Check if we haven't already returned these initializers
            init_marker = f'coord_init_{coord.id}'
            if init_marker not in ctx.compiled_nodes:
                ctx.compiled_nodes.add(init_marker)
                seen_punc_var = ctx.state_var(coord, 'seen_punc')
                input_exhausted_var = ctx.state_var(coord, 'input_exhausted')
                return [
                    (seen_punc_var.name, False),
                    (input_exhausted_var.name, False)
                ]

        return []

    def _get_reset_stmts(self, ctx) -> List[ast.stmt]:
        """Reset coordinator state (only generate once for first CatProj)."""
        coord = self.coordinator
        if coord.id not in ctx.state_vars:
            # This shouldn't happen if _get_state_initializers was called
            return []

        # Only reset if this is the first CatProj we're processing
        # Actually, both CatProj instances will try to reset, which is fine
        # since they reset the same state variables
        seen_punc_var = ctx.state_var(coord, 'seen_punc')
        input_exhausted_var = ctx.state_var(coord, 'input_exhausted')
        return [
            ast.Assign(
                targets=[seen_punc_var.lvalue()],
                value=ast.Constant(value=False)
            ),
            ast.Assign(
                targets=[input_exhausted_var.lvalue()],
                value=ast.Constant(value=False)
            )
        ]