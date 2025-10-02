"""
Type checking for events - determines if an event has a given type.

Event type rules:
- CatEvA(x) has type TyCat(s,t) if x has type s
- CatPunc has type TyCat(s,t) if s is nullable
- ParEvA(x) has type TyPar(s,t) if x has type s
- ParEvB(x) has type TyPar(s,t) if x has type t
- PlusPuncA/B have type TyPlus(s,t) for any s,t and TyStar(s) for any s
- BaseEvent(v) has type Singleton(C) if v is an instance of C
"""

from python_delta.typecheck.types import Singleton, TyCat, TyPar, TyPlus, TyStar


def has_type(event, type):
    from python_delta.event import (
        BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB, Event
    )

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
