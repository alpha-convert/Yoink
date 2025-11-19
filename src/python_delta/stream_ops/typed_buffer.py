"""Typed buffers for accumulating stream events into complete values.

TypedBuffer and its subclasses implement imperative event consumption
for different stream types, buffering events until a complete value is ready.
"""

from __future__ import annotations

from python_delta.typecheck.types import TyEps, TyCat, TyPlus, TyStar, Singleton, TypeVar
from python_delta.event import BaseEvent, CatEvA, CatPunc, PlusPuncA, PlusPuncB


class TypedBuffer:
    """Base class for typed buffers that accumulate stream events into values."""

    def __init__(self):
        pass

    def poke_event(self, event):
        raise NotImplementedError("Subclasses must implement poke_event")

    def is_complete(self):
        """Returns True if the buffer has consumed a complete value."""
        raise NotImplementedError("Subclasses must implement is_complete")

    def get_events(self):
        """Extract the buffered value once complete."""
        raise NotImplementedError("Subclasses must implement get_value")


class SingletonTypedBuffer(TypedBuffer):
    """Typed buffer for singleton types - stores a single base value."""

    def __init__(self):
        super().__init__()
        self.value = None
        self.complete = False

    def poke_event(self, event):
        assert isinstance(event, BaseEvent), f"Expected BaseEvent, got {event}"
        self.value = event.value
        self.complete = True

    def is_complete(self):
        return self.complete

    def get_events(self):
        return [BaseEvent(self.value)]


class EpsTypedBuffer(TypedBuffer):
    """Typed buffer for epsilon type - consumes no events."""

    def __init__(self):
        pass

    def poke_event(self, event):
        raise ValueError(f"TyEps cannot consume events, got {event}")

    def is_complete(self):
        return True

    def get_events(self):
        return []


class CatTypedBuffer(TypedBuffer):
    """Typed buffer for product types - buffers left then right values."""

    def __init__(self, left_buf, right_buf):
        self.left_buffer = left_buf
        self.right_buffer = right_buf
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

    def get_events(self):
        return [CatEvA(e) for e in self.left_buffer.get_events()] + [CatPunc()] + self.right_buffer.get_events()


class PlusTypedBuffer(TypedBuffer):
    def __init__(self, left_buf, right_buf):
        self.tag = None
        self.left_buf = left_buf
        self.right_buf = right_buf

    def poke_event(self, event):
        if isinstance(event, PlusPuncA):
            self.tag = 'left'
        elif isinstance(event, PlusPuncB):
            self.tag = 'right'
        else:
            assert self.tag is not None, "Plus tag must be chosen before consuming events"
            if self.tag == 'left':
                self.left_buf.poke_event(event)
            else:
                self.right_buf.poke_event(event)

    def is_complete(self):
        if self.tag == 'left':
            return self.left_buf.is_complete()
        elif self.tag == 'right':
            return self.right_buf.is_complete()
        else:
            return False


    def get_events(self):
        assert self.tag is not None, "Cannot get value before tag is chosen"
        if self.tag == 'left':
            return [PlusPuncA()] + self.left_buf.get_events()
        else:
            return [PlusPuncB()] + self.right_buf.get_events()


# class StarTypedBuffer(TypedBuffer):
#     """Typed buffer for list types - buffers a sequence of values."""

#     def __init__(self, stream_type):
#         super().__init__(stream_type)
#         self.elements = []
#         self.current_element = None
#         self.terminated = False

#     def poke_event(self, event):
#         if isinstance(event, PlusPuncA):
#             self.terminated = True
#         elif isinstance(event, PlusPuncB):
#             self.current_element = make_typed_buffer(self.stream_type.element_type)
#         elif isinstance(event, CatEvA):
#             assert self.current_element is not None, "StarTypedBuffer: no current element"
#             self.current_element.poke_event(event.value)
#         elif isinstance(event, CatPunc):
#             assert self.current_element is not None and self.current_element.is_complete(), \
#                 "CatPunc in star but current element not complete"
#             self.elements.append(self.current_element.get_value())
#             self.current_element = None
#         else:
#             raise ValueError(f"Unexpected event in StarTypedBuffer: {event}")

#     def is_complete(self):
#         return self.terminated

#     def get_value(self):
#         return self.elements


def make_typed_buffer(stream_type):
    """Factory function to create the appropriate TypedBuffer subclass for a stream type."""
    if isinstance(stream_type, TypeVar):
        assert stream_type.link is not None
        return make_typed_buffer(stream_type.link)
    if isinstance(stream_type, Singleton):
        return SingletonTypedBuffer()
    elif isinstance(stream_type, TyCat):
        left_buf = make_typed_buffer(stream_type.left_type)
        right_buf = make_typed_buffer(stream_type.right_type)
        return CatTypedBuffer(left_buf,right_buf)
    elif isinstance(stream_type, TyPlus):
        left_buf = make_typed_buffer(stream_type.left_type)
        right_buf = make_typed_buffer(stream_type.right_type)
        return PlusTypedBuffer(left_buf,right_buf)
    elif isinstance(stream_type, TyEps):
        return EpsTypedBuffer()
    else:
        raise ValueError(f"Cannot create TypedBuffer for type: {stream_type}")
