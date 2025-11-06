from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.typecheck.types import Type, Singleton, TyCat, TyPlus, TyStar, TyEps, TypeVar
from python_delta.event import BaseEvent, CatEvA, CatPunc, PlusPuncA, PlusPuncB

def value_to_events(value, stream_type):
    """
    Recursively convert a Python value to its event sequence.

    Crawls the value and stream_type structures in parallel,
    generating the appropriate events.

    Args:
        value: Python value (int, tuple, list, tagged union, etc.)
        stream_type: Stream type describing the value structure

    Returns:
        List of events
    """
    if isinstance(stream_type, TypeVar):
        assert stream_type.link is not None
        return value_to_events(value,stream_type.link)

    elif isinstance(stream_type, TyEps):
        return []

    elif isinstance(stream_type, Singleton):
        return [BaseEvent(value)]

    elif isinstance(stream_type, TyCat):
        left_val, right_val = value
        left_events = value_to_events(left_val, stream_type.left_type)
        right_events = value_to_events(right_val, stream_type.right_type)
        # Wrap left events in CatEvA, add CatPunc, then right events
        return [CatEvA(e) for e in left_events] + [CatPunc()] + right_events

    elif isinstance(stream_type, TyPlus):
        tag, tagged_val = value
        if tag == 'left':
            tagged_events = value_to_events(tagged_val, stream_type.left_type)
            return [PlusPuncA()] + tagged_events
        else:
            tagged_events = value_to_events(tagged_val, stream_type.right_type)
            return [PlusPuncB()] + tagged_events

    elif isinstance(stream_type, TyStar):
        # value is a list
        if len(value) == 0:
            return [PlusPuncA()]  # nil
        else:
            # cons: emit PlusPuncB, first element wrapped in CatEvA, CatPunc, then rest
            first_events = value_to_events(value[0], stream_type.element_type)
            rest_events = value_to_events(value[1:], stream_type)  # Recursive call with TyStar
            return [PlusPuncB()] + [CatEvA(e) for e in first_events] + [CatPunc()] + rest_events

    else:
        raise ValueError(f"Unknown stream type: {stream_type}")

class EmitOp(StreamOp):
    """
    Streams out the value from a BufferOp after all sources complete.

    Three-phase execution model:
    - Phase 1 (PULLING): Pull on sources one at a time until all complete
    - Phase 2 (SERIALIZING): Evaluate BufferOp and serialize value to events
    - Phase 3 (EMITTING): Emit events one at a time
    """
    def __init__(self, buffer_op):
        # Output type is the same as the BufferOp's type
        super().__init__(buffer_op.stream_type)
        self.buffer_op = buffer_op
        self.sources = list(buffer_op.get_sources())  # Convert set to list for indexing

        # Phase tracking
        self.phase = 'PULLING'  # 'PULLING', 'SERIALIZING', 'EMITTING'
        self.source_index = 0   # Which source we're currently pulling
        self.event_buffer = None  # Events to emit in phase 3
        self.emit_index = 0     # Which event to emit next
    
    @property
    def vars(self):
        return set().union(*[source.vars for source in self.sources])

    def _pull(self):
        if self.phase == 'PULLING':
            if self.source_index >= len(self.sources):
                self.phase = 'SERIALIZING'
                return None

            current_source = self.sources[self.source_index]

            if current_source.buffer.is_complete():
                self.source_index += 1
                return None

            v = current_source._pull()
            if v is DONE:
                self.source_index += 1
            return None

        elif self.phase == 'SERIALIZING':
            value = self.buffer_op.eval()

            self.event_buffer = value_to_events(value, self.stream_type)
            self.emit_index = 0

            self.phase = 'EMITTING'
            return None

        elif self.phase == 'EMITTING':
            if self.emit_index < len(self.event_buffer):
                event = self.event_buffer[self.emit_index]
                self.emit_index += 1
                return event
            else:
                return DONE

        else:
            raise ValueError(f"Unknown phase: {self.phase}")

    def reset(self):
        self.phase = 'PULLING'
        self.source_index = 0
        self.event_buffer = None
        self.emit_index = 0
        # Also need to reset all source WaitOps
        for waitop in self.sources:
            waitop.reset()

    @property
    def id(self):
        return hash(("EmitOp", hash(tuple(self.sources))))

    def _compile_stmts(self, ctx, dst):
        """Compile three-phase emit operation."""
        from python_delta.compilation import StateVar
        import ast

        phase_var = ctx.state_var(self, 'phase')
        source_index_var = ctx.state_var(self, 'source_index')
        event_buffer_var = ctx.state_var(self, 'event_buffer')
        emit_index_var = ctx.state_var(self, 'emit_index')

        # Phase 1: PULLING
        # Generate code for pulling from each source
        pull_stmts = []
        for i, source in enumerate(self.sources):
            source_tmp = ctx.allocate_temp()
            source_stmts = source._compile_stmts(ctx, source_tmp)

            pull_stmts.append(
                ast.If(
                    test=ast.Compare(
                        left=source_index_var.rvalue(),
                        ops=[ast.Eq()],
                        comparators=[ast.Constant(value=i)]
                    ),
                    body=source_stmts + [
                        ast.If(
                            test=ast.Compare(
                                left=source_tmp.rvalue(),
                                ops=[ast.Is()],
                                comparators=[ast.Name(id='DONE', ctx=ast.Load())]
                            ),
                            body=[
                                source_index_var.assign(
                                    ast.BinOp(
                                        left=source_index_var.rvalue(),
                                        op=ast.Add(),
                                        right=ast.Constant(value=1)
                                    )
                                )
                            ],
                            orelse=[]
                        ),
                        dst.assign(ast.Constant(value=None))
                    ],
                    orelse=[]
                )
            )

        # Check if all sources done
        pull_check = ast.If(
            test=ast.Compare(
                left=source_index_var.rvalue(),
                ops=[ast.GtE()],
                comparators=[ast.Constant(value=len(self.sources))]
            ),
            body=[
                phase_var.assign(ast.Constant(value='SERIALIZING')),
                dst.assign(ast.Constant(value=None))
            ],
            orelse=pull_stmts if pull_stmts else [dst.assign(ast.Constant(value=None))]
        )

        # Phase 2: SERIALIZING
        # Evaluate buffer_op and convert to events
        serialize_stmts = [
            # value = buffer_op.eval()
            ast.Assign(
                targets=[ast.Name(id='_emit_value', ctx=ast.Store())],
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Name(id='self', ctx=ast.Load()),
                            attr='outputs',
                            ctx=ast.Load()
                        ),
                        attr='buffer_op',
                        ctx=ast.Load()
                    ),
                    args=[],
                    keywords=[]
                )
            ) if hasattr(self, 'buffer_op') else ast.Pass(),
            # event_buffer = value_to_events(value, stream_type)
            event_buffer_var.assign(
                ast.Call(
                    func=ast.Name(id='value_to_events', ctx=ast.Load()),
                    args=[
                        ast.Name(id='_emit_value', ctx=ast.Load()),
                        ast.Attribute(
                            value=ast.Attribute(
                                value=ast.Name(id='self', ctx=ast.Load()),
                                attr='outputs',
                                ctx=ast.Load()
                            ),
                            attr='stream_type',
                            ctx=ast.Load()
                        )
                    ],
                    keywords=[]
                )
            ),
            emit_index_var.assign(ast.Constant(value=0)),
            phase_var.assign(ast.Constant(value='EMITTING')),
            dst.assign(ast.Constant(value=None))
        ]

        # Phase 3: EMITTING
        emit_stmts = [
            ast.If(
                test=ast.Compare(
                    left=emit_index_var.rvalue(),
                    ops=[ast.Lt()],
                    comparators=[
                        ast.Call(
                            func=ast.Name(id='len', ctx=ast.Load()),
                            args=[event_buffer_var.rvalue()],
                            keywords=[]
                        )
                    ]
                ),
                body=[
                    dst.assign(
                        ast.Subscript(
                            value=event_buffer_var.rvalue(),
                            slice=emit_index_var.rvalue(),
                            ctx=ast.Load()
                        )
                    ),
                    emit_index_var.assign(
                        ast.BinOp(
                            left=emit_index_var.rvalue(),
                            op=ast.Add(),
                            right=ast.Constant(value=1)
                        )
                    )
                ],
                orelse=[
                    dst.assign(ast.Name(id='DONE', ctx=ast.Load()))
                ]
            )
        ]

        # Main phase dispatch
        return [
            ast.If(
                test=ast.Compare(
                    left=phase_var.rvalue(),
                    ops=[ast.Eq()],
                    comparators=[ast.Constant(value='PULLING')]
                ),
                body=[pull_check],
                orelse=[
                    ast.If(
                        test=ast.Compare(
                            left=phase_var.rvalue(),
                            ops=[ast.Eq()],
                            comparators=[ast.Constant(value='SERIALIZING')]
                        ),
                        body=serialize_stmts,
                        orelse=emit_stmts
                    )
                ]
            )
        ]

    def _get_state_initializers(self, ctx):
        """Initialize emit state variables."""
        phase_var = ctx.state_var(self, 'phase')
        source_index_var = ctx.state_var(self, 'source_index')
        event_buffer_var = ctx.state_var(self, 'event_buffer')
        emit_index_var = ctx.state_var(self, 'emit_index')

        return [
            (phase_var.name, 'PULLING'),
            (source_index_var.name, 0),
            (event_buffer_var.name, None),
            (emit_index_var.name, 0)
        ]
