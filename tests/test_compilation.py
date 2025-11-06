"""Tests for compiled StreamOp execution - verify interpreter and compiler agree."""

import pytest
from hypothesis import given, settings
from python_delta.core import Delta, Singleton, TyStar, TyCat, TyPlus, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)
STRING_TY = Singleton(str)


def run_all(program, *inputs):
    """
    Run a program in interpreted, compiled, and CPS modes, return (interp_result, compiled_result, cps_result).

    Args:
        program: A @Delta.jit decorated function (DataflowGraph)
        *inputs: Input iterables for the program

    Returns:
        (interpreted_results, compiled_results, cps_results) - all as lists with None filtered out

    Raises:
        AssertionError: If inputs or outputs are not well-typed or if results don't match
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

    # Run compiled version (non-CPS)
    CompiledClass = program.compile(cps=False)
    compiled_output = CompiledClass(*[iter(inp) for inp in inputs])
    compiled_result = [x for x in list(compiled_output) if x is not None]

    # Run CPS version
    CpsClass = program.compile(cps=True)
    cps_output = CpsClass(*[iter(inp) for inp in inputs])
    cps_result = [x for x in list(cps_output) if x is not None]

    # Check that outputs are well-typed
    output_type = program.outputs.stream_type
    assert has_type(interp_result, output_type), \
        f"Interpreted output does not have expected type {output_type}"
    assert has_type(compiled_result, output_type), \
        f"Compiled output does not have expected type {output_type}"
    assert has_type(cps_result, output_type), \
        f"CPS output does not have expected type {output_type}"

    # Assert all three match
    assert interp_result == compiled_result == cps_result, \
        f"Results don't match:\nInterpreted: {interp_result}\nCompiled: {compiled_result}\nCPS: {cps_result}"

    return interp_result, compiled_result, cps_result


def test_compile_var_passthrough():
    """Simplest case: just pass through a var."""
    @Delta.jit
    def passthrough(delta, x: STRING_TY):
        return x

    data = [BaseEvent("x")]
    interp, compiled, cps = run_all(passthrough, data)

    assert interp == compiled == cps == data


def test_compile_catr_simple():
    """Test concatenation compilation."""
    @Delta.jit
    def concat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)

    xs = [BaseEvent("x")]
    ys = [BaseEvent("y")]

    run_all(concat, xs, ys)


def test_compile_sum_inl():
    """Test sum injection left."""
    @Delta.jit
    def inl_test(delta, x: STRING_TY):
        return delta.inl(x)

    xs = [BaseEvent("asdf")]

    interp, compiled, cps = run_all(inl_test, xs)

    assert interp == compiled == cps
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
    interp_left, compiled_left, cps_left = run_all(swap, xs_left)
    assert interp_left == compiled_left == cps_left
    assert interp_left[0] == PlusPuncB()  # Swapped to right
    assert interp_left[1] == BaseEvent("asdf")

    # Right injection
    xs_right = [PlusPuncB(), BaseEvent("asdf")]
    interp_right, compiled_right, cps_right = run_all(swap, xs_right)
    assert interp_right == compiled_right == cps_right
    assert interp_right[0] == PlusPuncA()  # Swapped to left
    assert interp_right[1] == BaseEvent("asdf")


def test_compile_map_identity():
    """Test map with identity function."""
    @Delta.jit
    def map_id(delta, s: TyStar(INT_TY)):
        return delta.map(s, lambda x: x)

    xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(), PlusPuncA()]

    interp, compiled, cps = run_all(map_id, xs)

    assert interp == compiled == cps == xs


def test_compile_concat_strings():
    """Test concat operation (multiple catr calls)."""
    @Delta.jit
    def concat_three(delta, x: INT_TY, y: INT_TY, z: INT_TY):
        xy = delta.catr(x, y)
        return delta.catr(xy, z)

    xs = [BaseEvent(1)]
    ys = [BaseEvent(2)]
    zs = [BaseEvent(3)]

    interp, compiled, cps = run_all(concat_three, xs, ys, zs)
    assert interp[0] == CatEvA(CatEvA(BaseEvent(1)))
    assert interp[1] == CatEvA(CatPunc())
    assert interp[2] == CatEvA(BaseEvent(2))
    assert interp[3] == CatPunc()
    assert interp[4] == BaseEvent(3)

    assert interp == compiled == cps


# Hypothesis-based property tests

@given(events_of_type(STRING_TY, max_depth=5))
@settings(max_examples=20)
def test_compile_var_preserves_output(input_events):
    """Property test: var passthrough produces same results compiled vs interpreted."""
    @Delta.jit
    def passthrough(delta, x: STRING_TY):
        return x

    assert has_type(input_events, STRING_TY)

    interp, compiled, cps = run_all(passthrough, input_events)

    assert interp == compiled == cps
    assert has_type(interp, STRING_TY)


@given(
    events_of_type(STRING_TY, max_depth=5),
    events_of_type(STRING_TY, max_depth=5)
)
@settings(max_examples=20)
def test_compile_catr_preserves_output(xs, ys):
    """Property test: catr produces same results compiled vs interpreted."""
    @Delta.jit
    def concat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)

    assert has_type(xs, STRING_TY)
    assert has_type(ys, STRING_TY)

    interp, compiled, cps = run_all(concat, xs, ys)

    assert interp == compiled == cps
    assert has_type(interp, TyCat(STRING_TY, STRING_TY))


# # @given(events_of_type(TyCat(STRING_TY, STRING_TY), max_depth=5))
# # @settings(max_examples=20)
# # def test_compile_catl_preserves_output(input_events):
# #     """Property test: catl produces same results compiled vs interpreted."""
# #     @Delta.jit
# #     def catl_both(delta, x: TyCat(STRING_TY, STRING_TY)):
# #         return delta.catl(x)

# #     assert has_type(input_events, TyCat(STRING_TY, STRING_TY))

# #     interp, compiled = run_all(catl_both, input_events)

# #     assert interp == compiled


@given(events_of_type(STRING_TY, max_depth=5))
@settings(max_examples=20)
def test_compile_inl_preserves_output(input_events):
    """Property test: inl produces same results compiled vs interpreted."""
    @Delta.jit
    def inl_test(delta, x: STRING_TY):
        return delta.inl(x)

    assert has_type(input_events, STRING_TY)

    interp, compiled, cps = run_all(inl_test, input_events)

    assert interp == compiled == cps
    assert has_type(interp, TyPlus(STRING_TY, STRING_TY))


@given(events_of_type(TyPlus(STRING_TY, STRING_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_case_preserves_output(input_events):
    """Property test: case produces same results compiled vs interpreted."""
    @Delta.jit
    def case_id(delta, x: TyPlus(STRING_TY, STRING_TY)):
        return delta.case(x, lambda l: l, lambda r: r)

    assert has_type(input_events, TyPlus(STRING_TY, STRING_TY))

    interp, compiled, cps = run_all(case_id, input_events)

    assert interp == compiled == cps
    assert has_type(interp, STRING_TY)


@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_map_identity_preserves_output(input_events):
    """Property test: map with identity produces same results compiled vs interpreted."""
    @Delta.jit
    def map_id(delta, s: TyStar(INT_TY)):
        return delta.map(s, lambda x: x)

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps = run_all(map_id, input_events)

    assert interp == compiled == cps
    assert has_type(interp, TyStar(INT_TY))


@given(events_of_type(TyStar(TyCat(INT_TY, INT_TY)), max_depth=5))
@settings(max_examples=20)
def test_compile_map_proj1_preserves_output(input_events):
    """Property test: map with projection produces same results compiled vs interpreted."""
    @Delta.jit
    def map_proj1(delta, s: TyStar(TyCat(INT_TY, INT_TY))):
        def proj1(z):
            (x, _) = delta.catl(z)
            return x
        return delta.map(s, proj1)

    assert has_type(input_events, TyStar(TyCat(INT_TY, INT_TY)))

    interp, compiled, cps = run_all(map_proj1, input_events)


@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_inj(input_events):
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.concat_map(s,lambda x : delta.cons(x,delta.nil()))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps = run_all(f, input_events)

@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_one_cons(input_events):
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.concat_map(s,lambda x : delta.cons(delta.singleton(1),delta.cons(x,delta.nil())))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps = run_all(f, input_events)


def test_compile_zip_with_catr():
    """Test zip_with with CatR function - pairs elements together."""
    @Delta.jit
    def zip_pair(delta, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(x, y))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(10)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(20)), CatPunc(),
          PlusPuncA()]

    interp, compiled, cps = run_all(zip_pair, xs, ys)
    assert interp == compiled == cps
    assert has_type(interp, TyStar(TyCat(INT_TY, INT_TY)))


# def test_compile_splitz_nil():
#     """Test splitZ with nil (empty list)."""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.splitZ(s)

#     xs = [PlusPuncA()]
#     interp, compiled = run_all(f, xs)

#     assert interp == compiled
#     assert interp[0] == CatEvA(PlusPuncA())
#     assert interp[1] == CatPunc()
#     assert interp[2] == PlusPuncA()


# def test_compile_splitz_cons_all_nonz():
#     """Test splitZ with all non-zero elements."""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.splitZ(s)

#     xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
#           PlusPuncA()]

#     interp, compiled = run_all(f, xs)
#     assert interp == compiled


# def test_compile_splitz_cons_immediate_z():
#     """Test splitZ with zero as first element."""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.splitZ(s)

#     xs = [PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
#           PlusPuncA()]

#     interp, compiled = run_all(f, xs)
#     assert interp == compiled


# def test_compile_splitz_cons_onez():
#     """Test splitZ with zero in middle of list."""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.splitZ(s)

#     xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
#           PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
#           PlusPuncA()]

#     interp, compiled = run_all(f, xs)
#     assert interp == compiled


def test_compile_concatmap_nil():
    """Test concat_map with nil function."""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.concat_map(s, lambda _: delta.nil())

    xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(),
          PlusPuncA()]

    interp, compiled, cps = run_all(f, xs)
    assert interp == compiled == cps
    assert interp == [PlusPuncA()]


def test_compile_concatmap_id():
    """Test concat_map with identity (cons(x, nil))."""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.concat_map(s, lambda x: delta.cons(x, delta.nil()))

    xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(),
          PlusPuncA()]

    interp, compiled, cps = run_all(f, xs)
    assert interp == compiled == cps
    assert interp == xs


def test_compile_concatmap_cons_one():
    """Test concat_map with cons(1, cons(x, nil))."""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.concat_map(s, lambda x: delta.cons(delta.singleton(1), delta.cons(x, delta.nil())))

    xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(),
          PlusPuncA()]

    interp, compiled, cps = run_all(f, xs)
    assert interp == compiled == cps


@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_nil_preserves_output(input_events):
    """Property test: concat_map with nil compiles correctly."""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.concat_map(s, lambda _: delta.nil())

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps = run_all(f, input_events)

    assert interp == compiled == cps
    assert has_type(interp, TyStar(INT_TY))


@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_id_preserves_output(input_events):
    """Property test: concat_map with identity compiles correctly."""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.concat_map(s, lambda x: delta.cons(x, delta.nil()))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps = run_all(f, input_events)

    assert interp == compiled == cps
    assert has_type(interp, TyStar(INT_TY))


@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_cons_one_preserves_output(input_events):
    """Property test: concat_map with cons(1, cons(x, nil)) compiles correctly."""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.concat_map(s, lambda x: delta.cons(delta.singleton(1), delta.cons(x, delta.nil())))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps = run_all(f, input_events)

    assert interp == compiled == cps
    assert has_type(interp, TyStar(INT_TY))