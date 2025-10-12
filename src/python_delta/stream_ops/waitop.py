from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE
from python_delta.compilation import StateVar
from python_delta.typecheck.types import TyEps, TyCat, TyPlus, TyStar, Singleton
from python_delta.event import BaseEvent, CatEvA, CatPunc, PlusPuncA, PlusPuncB

# A WaitBuffer is determined by its type.
# A waitbuffer of singleton type is just a value of the underlying python class of the singleton type.
# A waitbuffer of type TyCat(s,t) is a pair of waitbuffers of type s and t
# A waitbuffer of type TyPlus(s,t) is either a waitbuffer of type s, or a waitbuffer of type t
# A waitbuffer of type TyEps contains no data.

# Waitbuffers define a functional operation poke_event : (x : s) -> waitbuffer(s) -> waitbuffer(derivative_x s)
# This should be implemented imperatively -- a waitbuffer also includes a cursor that points to the next base value expected to arrive.

class WaitBuffer:
    """Abstract base class for wait buffers. Each subclass handles a specific type constructor."""

    def __init__(self, stream_type):
        self.stream_type = stream_type

    def poke_event(self, event):
        """
        Imperatively consume an event, updating the buffer state.
        The event must be well-typed for the current state of the buffer.
        """
        raise NotImplementedError("Subclasses must implement poke_event")

    def is_complete(self):
        """Returns True if the buffer has consumed a complete value."""
        raise NotImplementedError("Subclasses must implement is_complete")

    def get_value(self):
        """Extract the buffered value once complete."""
        raise NotImplementedError("Subclasses must implement get_value")


class SingletonWaitBuffer(WaitBuffer):
    """Wait buffer for singleton types - stores a single base value."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.value = None
        self.complete = False

    def poke_event(self, event):
        assert isinstance(event, BaseEvent), f"Expected BaseEvent, got {event}"
        assert isinstance(event.value, self.stream_type.python_class), \
            f"Expected {self.stream_type.python_class}, got {type(event.value)}"
        self.value = event.value
        self.complete = True

    def is_complete(self):
        return self.complete

    def get_value(self):
        return self.value


class EpsWaitBuffer(WaitBuffer):
    """Wait buffer for epsilon type - contains no data."""

    def __init__(self, stream_type):
        super().__init__(stream_type)

    def poke_event(self, event):
        raise ValueError(f"TyEps cannot consume events, got {event}")

    def is_complete(self):
        return True

    def get_value(self):
        return None


class CatWaitBuffer(WaitBuffer):
    """Wait buffer for concatenation type - pair of wait buffers."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.left_buffer = make_buffer(stream_type.left_type)
        self.right_buffer = make_buffer(stream_type.right_type)
        self.seen_punc = False

    def poke_event(self, event):
        if isinstance(event, CatEvA):
            # Event for left side
            self.left_buffer.poke_event(event.value)
        elif isinstance(event, CatPunc):
            # Punctuation marker - left side must be complete
            assert self.left_buffer.is_complete(), "CatPunc received but left side not complete"
            self.seen_punc = True
        else:
            # Event for right side (must be after punctuation)
            assert self.seen_punc, "Right side event before CatPunc"
            self.right_buffer.poke_event(event)

    def is_complete(self):
        return self.seen_punc and self.right_buffer.is_complete()

    def get_value(self):
        return (self.left_buffer.get_value(), self.right_buffer.get_value())


class PlusWaitBuffer(WaitBuffer):
    """Wait buffer for sum type - tagged union of wait buffers."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.tag = None
        self.buffer = None

    def poke_event(self, event):
        if isinstance(event, PlusPuncA):
            # Commit to left branch
            self.tag = 'left'
            self.buffer = make_buffer(self.stream_type.left_type)
        elif isinstance(event, PlusPuncB):
            # Commit to right branch
            self.tag = 'right'
            self.buffer = make_buffer(self.stream_type.right_type)
        else:
            # Forward to the chosen branch buffer
            assert self.buffer is not None, "Plus tag must be chosen before consuming events"
            self.buffer.poke_event(event)

    def is_complete(self):
        return self.buffer is not None and self.buffer.is_complete()

    def get_value(self):
        assert self.buffer is not None, "Cannot get value before tag is chosen"
        return (self.tag, self.buffer.get_value())


class StarWaitBuffer(WaitBuffer):
    """Wait buffer for star type - list of element buffers."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.elements = []
        self.current_element = None
        self.terminated = False

    def poke_event(self, event):
        if isinstance(event, PlusPuncA):
            self.terminated = True
        elif isinstance(event, PlusPuncB):
            self.current_element = make_buffer(self.stream_type.element_type)
        elif isinstance(event, CatEvA):
            assert self.current_element is not None, "StarWaitBuffer: no current element"
            self.current_element.poke_event(event.value)
        elif isinstance(event, CatPunc):
            # Current element is complete
            assert self.current_element is not None and self.current_element.is_complete(), \
                "CatPunc in star but current element not complete"
            self.elements.append(self.current_element.get_value())
            self.current_element = None
        else:
            raise ValueError(f"Unexpected event in StarWaitBuffer: {event}")

    def is_complete(self):
        return self.terminated

    def get_value(self):
        return self.elements


def make_buffer(stream_type):
    if isinstance(stream_type, Singleton):
        return SingletonWaitBuffer(stream_type)
    elif isinstance(stream_type, TyCat):
        return CatWaitBuffer(stream_type)
    elif isinstance(stream_type, TyPlus):
        return PlusWaitBuffer(stream_type)
    elif isinstance(stream_type, TyStar):
        return StarWaitBuffer(stream_type)
    elif isinstance(stream_type, TyEps):
        return EpsWaitBuffer(stream_type)
    else:
        raise ValueError(f"Cannot create WaitBuffer for type: {stream_type}")


class WaitOp(StreamOp):
    """WAIT - waits until an entire value has arrived, buffering it in"""
    def __init__(self, input_stream):
        super().__init__(TyEps)
        self.input_stream = input_stream
        self.buffer = make_buffer(input_stream.stream_type)

    @property
    def id(self):
        return hash(("WaitOp", self.input_stream.id, str(self.stream_type)))

    @property
    def vars(self):
        return self.input_stream.vars

    def _pull(self):
        if self.buffer.is_complete():
            return DONE
        v = self.input_stream._pull()
        if v is DONE:
            assert self.buffer.is_complete()
            return DONE
        elif v is None:
            return None
        else:
            self.buffer.poke_event(v)
            return None

    def reset(self):
        self.buffer = make_buffer(self.input_stream.stream_type)