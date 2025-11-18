"""Typed buffers for accumulating stream events into complete values.

TypedBuffer and its subclasses implement imperative event consumption
for different stream types, buffering events until a complete value is ready.
"""

from __future__ import annotations

from python_delta.typecheck.types import TyEps, TyCat, TyPlus, TyStar, Singleton, TypeVar
from python_delta.event import BaseEvent, CatEvA, CatPunc, PlusPuncA, PlusPuncB


class TypedBuffer:
    """Base class for typed buffers that accumulate stream events into values."""

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


class SingletonTypedBuffer(TypedBuffer):
    """Typed buffer for singleton types - stores a single base value."""

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


class EpsTypedBuffer(TypedBuffer):
    """Typed buffer for epsilon type - consumes no events."""

    def __init__(self, stream_type):
        super().__init__(stream_type)

    def poke_event(self, event):
        raise ValueError(f"TyEps cannot consume events, got {event}")

    def is_complete(self):
        return True

    def get_value(self):
        return None


class CatTypedBuffer(TypedBuffer):
    """Typed buffer for product types - buffers left then right values."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.left_buffer = make_typed_buffer(stream_type.left_type)
        self.right_buffer = make_typed_buffer(stream_type.right_type)
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


class PlusTypedBuffer(TypedBuffer):
    """Typed buffer for sum types - buffers left or right value."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.tag = None
        self.buffer = None

    def poke_event(self, event):
        if isinstance(event, PlusPuncA):
            self.tag = 'left'
            self.buffer = make_typed_buffer(self.stream_type.left_type)
        elif isinstance(event, PlusPuncB):
            self.tag = 'right'
            self.buffer = make_typed_buffer(self.stream_type.right_type)
        else:
            # Forward to the chosen branch buffer
            assert self.buffer is not None, "Plus tag must be chosen before consuming events"
            self.buffer.poke_event(event)

    def is_complete(self):
        return self.buffer is not None and self.buffer.is_complete()

    def get_value(self):
        assert self.buffer is not None, "Cannot get value before tag is chosen"
        return (self.tag, self.buffer.get_value())


class StarTypedBuffer(TypedBuffer):
    """Typed buffer for list types - buffers a sequence of values."""

    def __init__(self, stream_type):
        super().__init__(stream_type)
        self.elements = []
        self.current_element = None
        self.terminated = False

    def poke_event(self, event):
        if isinstance(event, PlusPuncA):
            self.terminated = True
        elif isinstance(event, PlusPuncB):
            self.current_element = make_typed_buffer(self.stream_type.element_type)
        elif isinstance(event, CatEvA):
            assert self.current_element is not None, "StarTypedBuffer: no current element"
            self.current_element.poke_event(event.value)
        elif isinstance(event, CatPunc):
            # Current element is complete
            assert self.current_element is not None and self.current_element.is_complete(), \
                "CatPunc in star but current element not complete"
            self.elements.append(self.current_element.get_value())
            self.current_element = None
        else:
            raise ValueError(f"Unexpected event in StarTypedBuffer: {event}")

    def is_complete(self):
        return self.terminated

    def get_value(self):
        return self.elements


def make_typed_buffer(stream_type):
    """Factory function to create the appropriate TypedBuffer subclass for a stream type."""
    if isinstance(stream_type, TypeVar):
        assert stream_type.link is not None
        return make_typed_buffer(stream_type.link)
    if isinstance(stream_type, Singleton):
        return SingletonTypedBuffer(stream_type)
    elif isinstance(stream_type, TyCat):
        return CatTypedBuffer(stream_type)
    elif isinstance(stream_type, TyPlus):
        return PlusTypedBuffer(stream_type)
    elif isinstance(stream_type, TyStar):
        return StarTypedBuffer(stream_type)
    elif isinstance(stream_type, TyEps):
        return EpsTypedBuffer(stream_type)
    else:
        raise ValueError(f"Cannot create TypedBuffer for type: {stream_type}")
