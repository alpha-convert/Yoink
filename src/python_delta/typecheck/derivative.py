"""
Type derivatives - computing residual types after consuming events.

Given a type T and an event e that has type T, the derivative ∂e(T)
represents what remains of type T after consuming event e.
"""

from python_delta.typecheck.types import Singleton, TyCat, TyPar, TyPlus, TyStar, TyEps
from python_delta.event import BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB


class DerivativeError(Exception):
    """Raised when an event doesn't match the expected type for derivatives."""
    def __init__(self, message):
        self.message = message
        super().__init__(message)


def derivative(type, event):
    """
    Compute the derivative of a type with respect to an event.

    Args:
        type: A Type instance
        event: An Event instance that should have the given type

    Returns:
        Type: The residual type after consuming the event

    Raises:
        DerivativeError: If the event doesn't match the type structure
    """

    if isinstance(type, Singleton):
        if isinstance(event, BaseEvent) and isinstance(event.value,type.python_class):
            return TyEps()
        raise DerivativeError(f"Expected BaseEvent with class {type.python_class.__name__}, got {event}")

    elif isinstance(type, TyCat):
        if isinstance(event, CatEvA):
            # Derivative of left side, still need right side
            left_deriv = derivative(type.left_type, event.value)
            return TyCat(left_deriv, type.right_type)
        elif isinstance(event, CatPunc):
            # Left side done, right side remains
            return type.right_type
        else:
            raise DerivativeError(f"Expected CatEvA or CatPunc for TyCat, got {event.__class__.__name__}")

    elif isinstance(type, TyPar):
        if isinstance(event, ParEvA):
            # Consumed from left side
            left_deriv = derivative(type.left_type, event.value)
            return TyPar(left_deriv, type.right_type)
        elif isinstance(event, ParEvB):
            # Consumed from right side
            right_deriv = derivative(type.right_type, event.value)
            return TyPar(type.left_type, right_deriv)
        else:
            raise DerivativeError(f"Expected ParEvA or ParEvB for TyPar, got {event.__class__.__name__}")

    elif isinstance(type, TyPlus):
        if isinstance(event, PlusPuncA):
            # Committed to left branch
            return type.left_type
        elif isinstance(event, PlusPuncB):
            # Committed to right branch
            return type.right_type
        else:
            raise DerivativeError(f"Expected PlusPuncA or PlusPuncB for TyPlus, got {event.__class__.__name__}")

    elif isinstance(type, TyStar):
        if isinstance(event, PlusPuncA):
            # Nil case - star is done
            return TyEps()
        elif isinstance(event, PlusPuncB):
            # Cons case - have S followed by S*
            return TyCat(type.element_type, type)
        else:
            raise DerivativeError(f"Expected PlusPuncA or PlusPuncB for TyStar, got {event.__class__.__name__}")

    elif isinstance(type, TyEps):
        raise DerivativeError(f"TyEps cannot consume any events, got {event.__class__.__name__}")

    else:
        raise DerivativeError(f"Cannot compute derivative for type {type.__class__.__name__}")
