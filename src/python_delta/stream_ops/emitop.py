from python_delta.compilation.runtime import Runtime, value_to_events
from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.typecheck.types import Type, Singleton, TyCat, TyPlus, TyStar, TyEps, TypeVar
from python_delta.event import BaseEvent, CatEvA, CatPunc, PlusPuncA, PlusPuncB
from enum import Enum

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