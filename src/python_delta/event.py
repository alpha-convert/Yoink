# Event wrapper classes for stream elements

from python_delta.typecheck.types import TyCat, TyPar, TyPlus, TyStar


class Event:
    """Base class for all event wrappers. Ensures all events implement has_type."""

    def has_type(self, type):
        raise NotImplementedError(f"{self.__class__.__name__} must implement has_type")


class CatEvA(Event):
    """Event from left side of concatenation."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"CatEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, CatEvA) and self.value == other.value

    def has_type(self, type):
        """CatEvA(x) has type TyCat(s, t) if x has type s."""
        if not isinstance(type, TyCat):
            return False

        # Recursively check if the wrapped value has the left type
        if isinstance(self.value, Event):
            return self.value.has_type(type.left_type)

        # Raw values should be wrapped in BaseEvent for proper type checking
        return False

class CatPunc(Event):
    """Punctuation marker between A and B in concatenation."""
    def __repr__(self):
        return "CatPunc"
    def __eq__(self, other):
        return isinstance(other, CatPunc)

    def has_type(self, type):
        """CatPunc has type TyCat(s, t) if s is nullable."""
        if not isinstance(type, TyCat):
            return False

        # TODO: Implement nullable check for type.left
        # For now, CatPunc always has type TyCat(s, t) for any s, t
        # because we haven't defined what "nullable" means yet
        return True

class ParEvA(Event):
    """Event from left side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvA) and self.value == other.value

    def has_type(self, type):
        """ParEvA(x) has type TyPar(s, t) if x has type s."""
        if not isinstance(type, TyPar):
            return False

        # Recursively check if the wrapped value has the left type
        if isinstance(self.value, Event):
            return self.value.has_type(type.left_type)

        # Raw values should be wrapped in BaseEvent for proper type checking
        return False

class ParEvB(Event):
    """Event from right side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvB({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvB) and self.value == other.value

    def has_type(self, type):
        """ParEvB(x) has type TyPar(s, t) if x has type t."""
        if not isinstance(type, TyPar):
            return False

        # Recursively check if the wrapped value has the right type
        if isinstance(self.value, Event):
            return self.value.has_type(type.right_type)

        # Raw values should be wrapped in BaseEvent for proper type checking
        return False

class PlusPuncA(Event):
    """Tag marker for left injection in sum types."""
    def __repr__(self):
        return "PlusPuncA"
    def __eq__(self, other):
        return isinstance(other, PlusPuncA)

    def has_type(self, type):
        """PlusPuncA has type TyPlus(s, t) for any s, t and type TyStar(s) for any s."""
        return isinstance(type, (TyPlus, TyStar))

class PlusPuncB(Event):
    """Tag marker for right injection in sum types."""
    def __repr__(self):
        return "PlusPuncB"
    def __eq__(self, other):
        return isinstance(other, PlusPuncB)

    def has_type(self, type):
        """PlusPuncB has type TyPlus(s, t) for any s, t and type TyStar(s) for any s."""
        return isinstance(type, (TyPlus, TyStar))


class BaseEvent(Event):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f"BaseEvent({self.value})"

    def __eq__(self, other):
        return isinstance(other, BaseEvent) and self.value == other.value

    def has_type(self, type):
        from python_delta.typecheck.types import Singleton
        return isinstance(type, Singleton) and isinstance(self.value,type.python_class)
