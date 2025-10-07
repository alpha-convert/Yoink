"""
Hypothesis strategies for generating random event sequences of a given type.
"""

from hypothesis import strategies as st
from python_delta.event import BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
from python_delta.typecheck.types import Singleton, TyCat, TyPlus, TyStar, TyEps


def events_of_type(type, max_depth=5):
    """
    Generate a hypothesis strategy for event sequences of the given type.

    Args:
        type: A Type instance
        max_depth: Maximum recursion depth to prevent infinite sequences

    Returns:
        A hypothesis strategy that generates lists of events having the given type
    """
    if max_depth <= 0:
        # At max depth, generate minimal valid sequences
        # Only truly nullable types (TyEps) can be empty
        if isinstance(type, TyEps):
            return st.just([])
        elif isinstance(type, Singleton):
            value_strategy = _strategy_for_python_class(type.python_class)
            return value_strategy.map(lambda v: [BaseEvent(v)])
        elif isinstance(type, TyCat):
            # Cat needs minimal left + punc + minimal right
            # Use max_depth=1 to generate minimal sequences
            left_events = events_of_type(type.left_type, max_depth=1)
            right_events = events_of_type(type.right_type, max_depth=1)
            return st.tuples(left_events, right_events).map(
                lambda lr: [CatEvA(e) for e in lr[0]] + [CatPunc()] + lr[1]
            )
        elif isinstance(type, TyStar):
            # Star: minimal is nil (empty list)
            return st.just([PlusPuncA()])
        else:
            # For other types, return empty (may be invalid but we're at depth limit)
            return st.just([])

    if isinstance(type, TyEps):
        return st.just([])

    elif isinstance(type, Singleton):
        value_strategy = _strategy_for_python_class(type.python_class)
        return value_strategy.flatmap(
            lambda v: st.just([BaseEvent(v)])
        )

    elif isinstance(type, TyCat):
        def build_cat_sequence(left_events):
            if left_events:
                # We have left events, so we need CatEvA wrappers and then CatPunc
                wrapped_left = [CatEvA(e) for e in left_events]
                # After left events, add CatPunc and then right events
                return events_of_type(type.right_type, max_depth - 1).map(
                    lambda right_events: wrapped_left + [CatPunc()] + right_events
                )
            else:
                return events_of_type(type.right_type, max_depth - 1).map(
                    lambda right_events: [CatPunc()] + right_events
                )

        # Generate left events
        left_strategy = events_of_type(type.left_type, max_depth - 1)
        return left_strategy.flatmap(build_cat_sequence)

    elif isinstance(type, TyPlus):
        # Sum type: choose left or right branch
        def choose_branch(choice):
            if choice == 'left':
                return events_of_type(type.left_type, max_depth - 1).map(
                    lambda events: [PlusPuncA()] + events
                )
            else:
                return events_of_type(type.right_type, max_depth - 1).map(
                    lambda events: [PlusPuncB()] + events
                )

        return st.sampled_from(['left', 'right']).flatmap(choose_branch)

    elif isinstance(type, TyStar):
        # Kleene star: choose nil or cons
        def choose_star_branch(choice):
            if choice == 'nil':
                # Empty sequence
                return st.just([PlusPuncA()])
            else:
                # One element followed by the rest
                def build_cons(elem_events):
                    # After one element, we have TyCat(elem_type, TyStar(elem_type))
                    # So we need to recursively generate more star events
                    wrapped = [CatEvA(e) for e in elem_events]
                    return events_of_type(TyStar(type.element_type), max_depth - 1).map(
                        lambda rest: [PlusPuncB()] + wrapped + [CatPunc()] + rest
                    )

                return events_of_type(type.element_type, max_depth - 1).flatmap(build_cons)

        # Bias towards terminating (nil) as we get deeper
        nil_weight = max(1, max_depth)
        cons_weight = max(1, 5 - max_depth)
        return st.sampled_from(['nil'] * nil_weight + ['cons'] * cons_weight).flatmap(choose_star_branch)

    else:
        # Unknown type, generate empty sequence
        return st.just([])


def _strategy_for_python_class(python_class):
    if python_class == int:
        return st.integers()
    elif python_class == str:
        return st.text()
    elif python_class == bool:
        return st.booleans()
    elif python_class == float:
        return st.floats(allow_nan=False, allow_infinity=False)
    else:
        return st.none()
