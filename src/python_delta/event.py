# Event wrapper classes for stream elements

from python_delta.typecheck.has_type import has_type

class Event:
    """Base class for all event wrappers. Ensures all events implement has_type."""

    def has_type(self, type):
        return has_type(self, type)


class CatEvA(Event):
    """Event from left side of concatenation."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"CatEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, CatEvA) and self.value == other.value

class CatPunc(Event):
    """Punctuation marker between A and B in concatenation."""
    def __repr__(self):
        return "CatPunc"
    def __eq__(self, other):
        return isinstance(other, CatPunc)

class ParEvA(Event):
    """Event from left side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvA) and self.value == other.value

class ParEvB(Event):
    """Event from right side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvB({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvB) and self.value == other.value

class PlusPuncA(Event):
    """Tag marker for left injection in sum types."""
    def __repr__(self):
        return "PlusPuncA"
    def __eq__(self, other):
        return isinstance(other, PlusPuncA)

class PlusPuncB(Event):
    """Tag marker for right injection in sum types."""
    def __repr__(self):
        return "PlusPuncB"
    def __eq__(self, other):
        return isinstance(other, PlusPuncB)


class BaseEvent(Event):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f"BaseEvent({self.value})"

    def __eq__(self, other):
        return isinstance(other, BaseEvent) and self.value == other.value
