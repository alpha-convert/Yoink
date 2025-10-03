"""
Type checking for events - determines if an event has a given type.

Event type rules:
- CatEvA(x) has type TyCat(s,t) if x has type s
- CatPunc has type TyCat(s,t) if s is nullable
- PlusPuncA/B have type TyPlus(s,t) for any s,t and TyStar(s) for any s
- BaseEvent(v) has type Singleton(C) if v is an instance of C

Sequence type rules:
- A sequence of events has type s if either:
  (1) the sequence is empty, or
  (2) the head x has type s, and the remaining sequence has type deriv(x,s)
"""

from python_delta.typecheck.types import Singleton, TyCat, TyPlus, TyStar


def has_type(event, type):
    """
    Check if an event or sequence of events has the given type.

    For sequences: A sequence has type s if either:
      (1) the sequence is empty, or
      (2) the head x has type s, and the remaining sequence has type deriv(x,s)

    Args:
        event: An Event instance or an iterable of Event instances
        type: A Type instance

    Returns:
        bool: True if the event/sequence has the given type
    """
    from python_delta.event import (
        BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB, Event
    )
    from collections.abc import Iterable

    # Check if it's an iterable (but not an Event itself)
    if isinstance(event, Iterable) and not isinstance(event, (Event, str)):
        from python_delta.typecheck.derivative import derivative

        try:
            it = iter(event)
            head = next(it)
        except StopIteration:
            return True

        if not has_type(head, type):
            return False

        deriv_type = derivative(type, head)

        return has_type(it, deriv_type)

    # Single event type checking
    if isinstance(event, CatEvA):
        if not isinstance(type, TyCat):
            return False

        # Recursively check if the wrapped value has the left type
        if isinstance(event.value, Event):
            return has_type(event.value, type.left_type)

        return False

    elif isinstance(event, CatPunc):
        if not isinstance(type, TyCat):
            return False

        return type.left_type.nullable()

    elif isinstance(event, ParEvA):
        if not isinstance(type, TyPar):
            return False

        if isinstance(event.value, Event):
            return has_type(event.value, type.left_type)

        return False

    elif isinstance(event, ParEvB):
        if not isinstance(type, TyPar):
            return False

        if isinstance(event.value, Event):
            return has_type(event.value, type.right_type)

        return False

    elif isinstance(event, PlusPuncA):
        return isinstance(type, (TyPlus, TyStar))

    elif isinstance(event, PlusPuncB):
        return isinstance(type, (TyPlus, TyStar))

    elif isinstance(event, BaseEvent):
        return isinstance(type, Singleton) and isinstance(event.value, type.python_class)

    else:
        return False
