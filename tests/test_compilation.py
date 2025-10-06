"""Tests for compiled StreamOp execution - verify interpreter and compiler agree."""

import pytest
from hypothesis import given, settings
from python_delta.core import Delta, Singleton, TyStar, TyCat, TyPlus, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)
STRING_TY = Singleton(str)


def run_both(program, *inputs):
    """
    Run a program in both interpreted and compiled mode, return (interp_result, compiled_result).

    Args:
        program: A @Delta.jit decorated function (DataflowGraph)
        *inputs: Input iterables for the program

    Returns:
        (interpreted_results, compiled_results) - both as lists with None filtered out

    Raises:
        AssertionError: If inputs or outputs are not well-typed
    """
    # Check that inputs are well-typed
    assert len(inputs) == len(program.input_types), \
        f"Expected {len(program.input_types)} inputs, got {len(inputs)}"

    for i, (input_data, expected_type) in enumerate(zip(inputs, program.input_types)):
        assert has_type(input_data, expected_type), \
            f"Input {i} does not have expected type {expected_type}"

    # Run interpreted version
    interp_output = program(*[iter(inp) for inp in inputs])
    interp_result = [x for x in list(interp_output) if x is not None]

    # Run compiled version
    CompiledClass = program.compile()
    compiled_output = CompiledClass(*[iter(inp) for inp in inputs])
    compiled_result = [x for x in list(compiled_output) if x is not None]

    # Check that outputs are well-typed
    output_type = program.outputs.stream_type
    assert has_type(interp_result, output_type), \
        f"Interpreted output does not have expected type {output_type}"
    assert has_type(compiled_result, output_type), \
        f"Compiled output does not have expected type {output_type}"

    assert interp_result == compiled_result
    return interp_result, compiled_result


def test_compile_var_passthrough():
    """Simplest case: just pass through a var."""
    @Delta.jit
    def passthrough(delta, x: STRING_TY):
        return x

    data = [BaseEvent("x")]
    interp, compiled = run_both(passthrough, data)

    assert interp == compiled == data


def test_compile_catr_simple():
    """Test concatenation compilation."""
    @Delta.jit
    def concat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)

    xs = [BaseEvent("x")]
    ys = [BaseEvent("y")]

    run_both(concat, xs, ys)


# def test_compile_catl_simple():
    # """Test left concatenation (projection)."""
    # @Delta.jit
    # def catl_test(delta, x: TyCat(STRING_TY, STRING_TY)):
    #     return delta.catl(x)

    # xs = [
    #     CatEvA(PlusPuncB()),
    #     CatEvA(CatEvA(BaseEvent(1))),
    #     CatEvA(CatPunc()),
    #     CatEvA(PlusPuncA()),
    #     CatPunc(),
    #     PlusPuncB(),
    #     CatEvA(BaseEvent(2)),
    #     CatPunc(),
    #     PlusPuncA()
    # ]

    # interp, compiled = run_both(catl_test, xs)

    # assert interp == compiled


def test_compile_sum_inl():
    """Test sum injection left."""
    @Delta.jit
    def inl_test(delta, x: STRING_TY):
        return delta.inl(x)

    xs = [BaseEvent("asdf")]

    interp, compiled = run_both(inl_test, xs)

    assert interp == compiled
    assert interp[0] == PlusPuncA()
    assert interp[1] == BaseEvent("asdf")


def test_compile_sum_case():
    """Test case analysis on sum types."""
    @Delta.jit
    def swap(delta, x: TyPlus(STRING_TY, STRING_TY)):
        return delta.case(
            x,
            lambda left: delta.inr(left),
            lambda right: delta.inl(right)
        )

    # Left injection
    xs_left = [PlusPuncA(), BaseEvent("asdf")]
    interp_left, compiled_left = run_both(swap, xs_left)
    assert interp_left == compiled_left
    assert interp_left[0] == PlusPuncB()  # Swapped to right
    assert interp_left[1] == BaseEvent("asdf")

    # Right injection
    xs_right = [PlusPuncB(), BaseEvent("asdf")]
    interp_right, compiled_right = run_both(swap, xs_right)
    assert interp_right == compiled_right
    assert interp_right[0] == PlusPuncA()  # Swapped to left
    assert interp_right[1] == BaseEvent("asdf")


# def test_compile_map_identity():
#     """Test map with identity function."""
#     @Delta.jit
#     def map_id(delta, s: TyStar(INT_TY)):
#         return delta.map(s, lambda x: x)

#     xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(), PlusPuncA()]

#     interp, compiled = run_both(map_id, xs)

#     assert interp == compiled == xs


# def test_compile_concat_strings():
#     """Test concat operation (multiple catr calls)."""
#     @Delta.jit
#     def concat_three(delta, x: STRING_TY, y: STRING_TY, z: STRING_TY):
#         xy = delta.catr(x, y)
#         return delta.catr(xy, z)

#     xs = [BaseEvent(1)]
#     ys = [BaseEvent(2)]
#     zs = [BaseEvent(3)]

#     interp, compiled = run_both(concat_three, xs, ys, zs)
#     assert interp[0] == CatEvA(CatEvA(BaseEvent(1)))
#     assert interp[1] == CatEvA(CatPunc())
#     assert interp[2] == CatEvA(BaseEvent(2))
#     assert interp[3] == CatPunc()
#     assert interp[4] == BaseEvent(3)

#     assert interp == compiled


# # Hypothesis-based property tests

# @given(events_of_type(STRING_TY, max_depth=5))
# @settings(max_examples=20)
# def test_compile_var_preserves_output(input_events):
#     """Property test: var passthrough produces same results compiled vs interpreted."""
#     @Delta.jit
#     def passthrough(delta, x: STRING_TY):
#         return x

#     assert has_type(input_events, STRING_TY)

#     interp, compiled = run_both(passthrough, input_events)

#     assert interp == compiled
#     assert has_type(interp, STRING_TY)


# @given(
#     events_of_type(STRING_TY, max_depth=5),
#     events_of_type(STRING_TY, max_depth=5)
# )
# @settings(max_examples=20)
# def test_compile_catr_preserves_output(xs, ys):
#     """Property test: catr produces same results compiled vs interpreted."""
#     @Delta.jit
#     def concat(delta, x: STRING_TY, y: STRING_TY):
#         return delta.catr(x, y)

#     assert has_type(xs, STRING_TY)
#     assert has_type(ys, STRING_TY)

#     interp, compiled = run_both(concat, xs, ys)

#     assert interp == compiled
#     assert has_type(interp, TyCat(STRING_TY, STRING_TY))


# # @given(events_of_type(TyCat(STRING_TY, STRING_TY), max_depth=5))
# # @settings(max_examples=20)
# # def test_compile_catl_preserves_output(input_events):
# #     """Property test: catl produces same results compiled vs interpreted."""
# #     @Delta.jit
# #     def catl_both(delta, x: TyCat(STRING_TY, STRING_TY)):
# #         return delta.catl(x)

# #     assert has_type(input_events, TyCat(STRING_TY, STRING_TY))

# #     interp, compiled = run_both(catl_both, input_events)

# #     assert interp == compiled


# @given(events_of_type(STRING_TY, max_depth=5))
# @settings(max_examples=20)
# def test_compile_inl_preserves_output(input_events):
#     """Property test: inl produces same results compiled vs interpreted."""
#     @Delta.jit
#     def inl_test(delta, x: STRING_TY):
#         return delta.inl(x)

#     assert has_type(input_events, STRING_TY)

#     interp, compiled = run_both(inl_test, input_events)

#     assert interp == compiled
#     assert has_type(interp, TyPlus(STRING_TY, STRING_TY))


# @given(events_of_type(TyPlus(STRING_TY, STRING_TY), max_depth=5))
# @settings(max_examples=20)
# def test_compile_case_preserves_output(input_events):
#     """Property test: case produces same results compiled vs interpreted."""
#     @Delta.jit
#     def case_id(delta, x: TyPlus(STRING_TY, STRING_TY)):
#         return delta.case(x, lambda l: l, lambda r: r)

#     assert has_type(input_events, TyPlus(STRING_TY, STRING_TY))

#     interp, compiled = run_both(case_id, input_events)

#     assert interp == compiled
#     assert has_type(interp, STRING_TY)


# @given(events_of_type(TyStar(INT_TY), max_depth=5))
# @settings(max_examples=20)
# def test_compile_map_identity_preserves_output(input_events):
#     """Property test: map with identity produces same results compiled vs interpreted."""
#     @Delta.jit
#     def map_id(delta, s: TyStar(INT_TY)):
#         return delta.map(s, lambda x: x)

#     assert has_type(input_events, TyStar(INT_TY))

#     interp, compiled = run_both(map_id, input_events)

#     assert interp == compiled
#     assert has_type(interp, TyStar(INT_TY))


# @given(events_of_type(TyStar(TyCat(INT_TY, INT_TY)), max_depth=5))
# @settings(max_examples=20)
# def test_compile_map_proj1_preserves_output(input_events):
#     """Property test: map with projection produces same results compiled vs interpreted."""
#     @Delta.jit
#     def map_proj1(delta, s: TyStar(TyCat(INT_TY, INT_TY))):
#         def proj1(z):
#             (x, _) = delta.catl(z)
#             return x
#         return delta.map(s, proj1)

#     assert has_type(input_events, TyStar(TyCat(INT_TY, INT_TY)))

#     interp, compiled = run_both(map_proj1, input_events)

#     assert interp == compiled
#     assert has_type(interp, TyStar(INT_TY))
