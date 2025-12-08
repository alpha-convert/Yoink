from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.typecheck.types import Type, Singleton, TyCat, TyPlus, TyStar, TyEps, TypeVar
from python_delta.event import BaseEvent, CatEvA, CatPunc, PlusPuncA, PlusPuncB
from enum import Enum

def value_to_events(value, stream_type : Type):
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
            first_events = value_to_events(value[0], stream_type.element_type)
            rest_events = value_to_events(value[1:], stream_type)  # Recursive call with TyStar
            return [PlusPuncB()] + [CatEvA(e) for e in first_events] + [CatPunc()] + rest_events

    else:
        raise ValueError(f"Unknown stream type: {stream_type}")



class EmitOpPhase(Enum):
    SERIALIZING = 1
    EMITTING = 2

class EmitOp(StreamOp):
    """
    Streams out the value from a BufferOp after all sources complete.

    Three-phase execution model:
    - Phase 1 (SERIALIZING): Evaluate BufferOp and serialize value to events
    - Phase 2 (EMITTING): Emit events one at a time
    """
    def __init__(self, buffer_op):
        super().__init__(buffer_op.stream_type)
        self.buffer_op = buffer_op

        self.phase = EmitOpPhase.SERIALIZING
        self.event_buffer = None
        self.emit_index = 0
    
    @property
    def vars(self):
        return set().union(*[source.vars for source in self.buffer_op.get_sources()])

    def _pull(self):
        if self.phase == EmitOpPhase.SERIALIZING:
            # When compiling this, we want to pre-allocate a buffer of events
            # that we can write the results into.
            self.event_buffer = self.buffer_op.eval()

            # self.event_buffer = value_to_events(value, self.stream_type)
            self.emit_index = 0

            self.phase = EmitOpPhase.EMITTING
            return None

        elif self.phase == EmitOpPhase.EMITTING:
            assert self.event_buffer is not None
            if self.emit_index < len(self.event_buffer):
                event = self.event_buffer[self.emit_index]
                self.emit_index += 1
                return event
            else:
                return DONE


    def reset(self):
        self.phase = EmitOpPhase.SERIALIZING
        self.source_index = 0
        # self.event_buffer = None
        self.emit_index = 0
        for waitop in self.buffer_op.get_sources():
            waitop.reset()

    @property
    def id(self):
        return hash(("EmitOp", self.buffer_op.id))

    def ensure_legal_recursion(self,is_in_tail : bool):
        for op in self.buffer_op.get_sources():
            op.ensure_legal_recursion(is_in_tail = False)