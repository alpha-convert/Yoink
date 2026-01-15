"""Tests for compiled StreamOp execution - verify interpreter and compiler agree."""

import pytest
from hypothesis import given, settings
from yoink.core import Yoink, Singleton, TyStar, TyCat, TyPlus, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from yoink.util.hypothesis_strategies import events_of_type
from yoink.typecheck.has_type import has_type
from yoink.compilation.direct_compiler import DirectCompiler
from yoink.compilation.cps_compiler import CPSCompiler
from yoink.compilation.generator_compiler import GeneratorCompiler


INT_TY = Singleton(int)
STRING_TY = Singleton(str)


def run_all(program, *inputs, compilers):
    """
    Run a program in interpreted mode and with each specified compiler.

    Args:
        program: A @Yoink.jit decorated function (DataflowGraph)
        *inputs: Input iterables for the program
        compilers: List of compiler classes to test

    Returns:
        Tuple of (interpreted_result, *compiled_results) - all as lists with None filtered out

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

    # Run each compiler
    compiled_results = []
    for compiler in compilers:
        CompiledClass = program.compile(compiler)
        compiled_output = CompiledClass(*[iter(inp) for inp in inputs])
        compiled_result = [x for x in list(compiled_output) if x is not None]
        compiled_results.append(compiled_result)

    # Check that outputs are well-typed
    output_type = program.outputs.stream_type
    assert has_type(interp_result, output_type), \
        f"Interpreted output does not have expected type {output_type}"

    # All results should match
    all_results = [interp_result] + compiled_results
    assert all(result == interp_result for result in all_results), \
        f"Results don't match! Interpreted: {interp_result}" + \
        " ... ".join(f"{compiler.__name__}: {result}" for compiler, result in zip(compilers, compiled_results))

    return tuple(all_results)


def test_compile_var_passthrough():
    """Simplest case: just pass through a var."""
    @Yoink.jit
    def passthrough(yoink, x: STRING_TY):
        return x

    data = [BaseEvent("x")]
    run_all(passthrough, data, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])


def test_compile_catr_simple():
    @Yoink.jit
    def f(yoink, x: STRING_TY, y: STRING_TY):
        return yoink.catr(x, y)

    xs = [BaseEvent("x")]
    ys = [BaseEvent("y")]

    run_all(f, xs, ys, compilers=[DirectCompiler, CPSCompiler,GeneratorCompiler])


def test_compile_sum_inl():
    """Test sum injection left."""
    @Yoink.jit
    def inl_test(yoink, x: STRING_TY):
        return yoink.inl(x)

    xs = [BaseEvent("asdf")]

    run_all(inl_test, xs, compilers=[DirectCompiler, CPSCompiler,GeneratorCompiler])


def test_compile_sum_case():
    """Test case analysis on sum types."""
    @Yoink.jit
    def swap(yoink, x: TyPlus(STRING_TY, STRING_TY)):
        return yoink.case(
            x,
            lambda left: yoink.inr(left),
            lambda right: yoink.inl(right)
        )

    # Left injection
    xs_left = [PlusPuncA(), BaseEvent("asdf")]
    run_all(swap, xs_left, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    # Right injection
    xs_right = [PlusPuncB(), BaseEvent("asdf")]
    run_all(swap, xs_right, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

def test_compile_map_identity():
    """Test map with identity function."""
    @Yoink.jit
    def map_id(yoink, s: TyStar(INT_TY)):
        return yoink.map(s, lambda x: x)

    xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(), PlusPuncA()]

    run_all(map_id, xs, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])


def test_compile_3catr_strings():
    @Yoink.jit
    def catr3(yoink, x: INT_TY, y: INT_TY, z: INT_TY):
        xy = yoink.catr(x, y)
        return yoink.catr(xy, z)

    xs = [BaseEvent(1)]
    ys = [BaseEvent(2)]
    zs = [BaseEvent(3)]

    interp, compiled, cps, generator = run_all(catr3, xs, ys, zs, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])
    assert interp[0] == CatEvA(CatEvA(BaseEvent(1)))
    assert interp[1] == CatEvA(CatPunc())
    assert interp[2] == CatEvA(BaseEvent(2))
    assert interp[3] == CatPunc()
    assert interp[4] == BaseEvent(3)


# Hypothesis-based property tests

@given(events_of_type(STRING_TY, max_depth=5))
@settings(max_examples=20)
def test_compile_var_preserves_output(input_events):
    """Property test: var passthrough produces same results compiled vs interpreted."""
    @Yoink.jit
    def passthrough(yoink, x: STRING_TY):
        return x

    assert has_type(input_events, STRING_TY)

    run_all(passthrough, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])


@given(
    events_of_type(STRING_TY, max_depth=5),
    events_of_type(STRING_TY, max_depth=5)
)
@settings(max_examples=20)
def test_compile_catr_preserves_output(xs, ys):
    """Property test: catr produces same results compiled vs interpreted."""
    @Yoink.jit
    def concat(yoink, x: STRING_TY, y: STRING_TY):
        return yoink.catr(x, y)

    assert has_type(xs, STRING_TY)
    assert has_type(ys, STRING_TY)

    run_all(concat, xs, ys, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

@given(
    events_of_type(TyStar(STRING_TY), max_depth=5),
    events_of_type(TyStar(STRING_TY), max_depth=5)
)
@settings(max_examples=20)
def test_compile_catr_stars_preserves_output(xs, ys):
    """Property test: catr produces same results compiled vs interpreted."""
    @Yoink.jit
    def concat(yoink, x: TyStar(STRING_TY), y: TyStar(STRING_TY)):
        return yoink.catr(x, y)

    assert has_type(xs, TyStar(STRING_TY))
    assert has_type(ys, TyStar(STRING_TY))

    run_all(concat, xs, ys, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])


@given(events_of_type(STRING_TY, max_depth=5))
@settings(max_examples=20)
def test_compile_inl_preserves_output(input_events):
    """Property test: inl produces same results compiled vs interpreted."""
    @Yoink.jit
    def inl_test(yoink, x: STRING_TY):
        return yoink.inl(x)

    assert has_type(input_events, STRING_TY)

    run_all(inl_test, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

@given(events_of_type(TyCat(TyStar(INT_TY), TyStar(INT_TY)), max_depth=5))
@settings(max_examples=20)
def test_compile_catproj_position0(input_events):
    """Property test: CatProj position 0 (first element of cat) compiles correctly."""
    @Yoink.jit
    def proj0(yoink, z: TyCat(TyStar(INT_TY), TyStar(INT_TY))):
        (x, _) = yoink.catl(z)
        return x

    assert has_type(input_events, TyCat(TyStar(INT_TY), TyStar(INT_TY)))

    interp, compiled, cps, generator = run_all(proj0, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(INT_TY))


@given(events_of_type(TyCat(TyStar(INT_TY), TyStar(INT_TY)), max_depth=5))
@settings(max_examples=20)
def test_compile_catproj_position1(input_events):
    """Property test: CatProj position 1 (second element of cat) compiles correctly."""
    @Yoink.jit
    def proj1(yoink, z: TyCat(TyStar(INT_TY), TyStar(INT_TY))):
        (_, y) = yoink.catl(z)
        return y

    assert has_type(input_events, TyCat(TyStar(INT_TY), TyStar(INT_TY)))

    interp, compiled, cps, generator = run_all(proj1, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(INT_TY))



@given(events_of_type(TyPlus(STRING_TY, STRING_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_case_preserves_output(input_events):
    """Property test: case produces same results compiled vs interpreted."""
    @Yoink.jit
    def case_id(yoink, x: TyPlus(STRING_TY, STRING_TY)):
        return yoink.case(x, lambda l: l, lambda r: r)

    assert has_type(input_events, TyPlus(STRING_TY, STRING_TY))

    run_all(case_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])


@given(events_of_type(TyStar(INT_TY), max_depth=10))
@settings(max_examples=20)
def test_compile_map_identity_preserves_output(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(INT_TY)):
        return yoink.map(s, lambda x: x)

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(INT_TY))

@given(events_of_type(TyStar(INT_TY), max_depth=10))
@settings(max_examples=20)
def test_compile_map_inl(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(INT_TY)):
        return yoink.map(s, lambda x: yoink.inl(x))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(TyPlus(INT_TY,INT_TY)))

@given(events_of_type(TyStar(TyStar(INT_TY)), max_depth=10))
@settings(max_examples=20)
def test_compile_map_concat_nil(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(TyStar(INT_TY))):
        return yoink.map(s, lambda x: yoink.concat(x,yoink.nil()))

    assert has_type(input_events, TyStar(TyStar(INT_TY)))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(TyStar(INT_TY)))

@given(events_of_type(TyStar(TyStar(INT_TY)), max_depth=10))
@settings(max_examples=20)
def test_compile_map_concat_backwards_nil(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(TyStar(INT_TY))):
        return yoink.map(s, lambda x: yoink.concat(yoink.nil(),x))

    assert has_type(input_events, TyStar(TyStar(INT_TY)))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(TyStar(INT_TY)))

@given(events_of_type(TyStar(TyCat(TyStar(INT_TY),TyStar(INT_TY))), max_depth=10))
@settings(max_examples=20)
def test_compile_map_concat_catl(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(TyCat(TyStar(INT_TY),TyStar(INT_TY)))):
        def body(xy):
            x,y = yoink.catl(xy)
            return yoink.concat(x,y)
        return yoink.map(s, body)

    assert has_type(input_events, TyStar(TyCat(TyStar(INT_TY),TyStar(INT_TY))))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(TyStar(INT_TY)))




@given(
        events_of_type(TyStar(INT_TY), max_depth=10),
        events_of_type(TyStar(INT_TY), max_depth=10),
)
@settings(max_examples=20)
def test_compile_concat(xs,ys):
    @Yoink.jit
    def f(yoink, s1 : TyStar(INT_TY), s2 : TyStar(INT_TY)):
        return yoink.concat(s1,s2)

    assert has_type(xs, TyStar(INT_TY))
    assert has_type(ys, TyStar(INT_TY))

    interp, compiled, cps, generator = run_all(f, xs,ys, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(INT_TY))

@given(
        events_of_type(TyStar(INT_TY), max_depth=1),
)
@settings(max_examples=1)
def test_compile_concat_const(xs):
    @Yoink.jit
    def f(yoink, _ : TyStar(INT_TY)):
        r = yoink.cons(yoink.singleton(0),yoink.nil())
        return yoink.concat(r,yoink.nil())

    assert has_type(xs, TyStar(INT_TY))

    interp,_,_ = run_all(f, xs,compilers=[DirectCompiler, CPSCompiler])

    assert interp == [PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),PlusPuncA()]


@given(
        events_of_type(TyCat(TyStar(INT_TY),TyStar(INT_TY)), max_depth=10)
)
@settings(max_examples=20)
def test_compile_concat_cat(xsys):
    @Yoink.jit
    def f(yoink, s : TyCat(TyStar(INT_TY),TyStar(INT_TY))):
        x,y = yoink.catl(s)
        return yoink.concat(x,y)

    assert has_type(xsys, TyCat(TyStar(INT_TY),TyStar(INT_TY)))

    interp, compiled, cps, generator = run_all(f, xsys, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(INT_TY))

@given(events_of_type(TyStar(INT_TY), max_depth=10))
@settings(max_examples=20)
def test_compile_map_zeroes(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(INT_TY)):
        return yoink.map(s, lambda x: yoink.singleton(0))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(INT_TY))


@given(events_of_type(TyStar(INT_TY), max_depth=10))
@settings(max_examples=20)
def test_compile_map_lift(input_events):
    @Yoink.jit
    def map_id(yoink, s: TyStar(INT_TY)):
        return yoink.map(s, lambda x: yoink.cons(x,yoink.nil()))

    assert has_type(input_events, TyStar(INT_TY))

    interp, compiled, cps, generator = run_all(map_id, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert interp == compiled == cps == generator
    assert has_type(interp, TyStar(TyStar(INT_TY)))


@given(events_of_type(TyStar(TyCat(INT_TY, INT_TY)), max_depth=5))
@settings(max_examples=20)
def test_compile_map_proj1_preserves_output(input_events):
    """Property test: map with projection produces same results compiled vs interpreted."""
    @Yoink.jit
    def map_proj1(yoink, s: TyStar(TyCat(INT_TY, INT_TY))):
        def proj1(z):
            (x, _) = yoink.catl(z)
            return x
        return yoink.map(s, proj1)

    assert has_type(input_events, TyStar(TyCat(INT_TY, INT_TY)))

    run_all(map_proj1, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])


@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_nil_preserves_output(input_events):
    """Property test: concat_map with nil compiles correctly."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.concat_map(s, lambda _: yoink.nil())

    assert has_type(input_events, TyStar(INT_TY))

    run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

@given(events_of_type(TyStar(TyStar(INT_TY)), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_flatten(input_events):
    @Yoink.jit
    def f(yoink, s: TyStar(TyStar(INT_TY))):
        return yoink.concat_map(s, lambda x: x)

    assert has_type(input_events, TyStar(TyStar(INT_TY)))

    interp, compiled, cps, generator = run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

    assert has_type(interp, TyStar(INT_TY))
    assert interp == compiled == cps == generator

def test_compile_zip_with_catr():
    """Test zip_with with CatR function - pairs elements together."""
    @Yoink.jit
    def zip_pair(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(10)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(20)), CatPunc(),
          PlusPuncA()]

    run_all(zip_pair, xs, ys, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

@given(
    events_of_type(TyStar(INT_TY), max_depth=5),
    events_of_type(TyStar(INT_TY), max_depth=5)
)
@settings(max_examples=20)
def test_zipwith_catr(xs_inps,ys_inps):
    @Yoink.jit
    def zip_pair(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

    run_all(zip_pair, xs_inps,ys_inps, compilers=[DirectCompiler, CPSCompiler])

def test_compile_splitz_nil():
    """Test splitZ with nil (empty list)."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.splitZ(s)

    xs = [PlusPuncA()]
    run_all(f, xs, compilers=[DirectCompiler, CPSCompiler])
    
def test_compile_splitz_cons_all_nonz():
    """Test splitZ with all non-zero elements."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.splitZ(s)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncA()]

    run_all(f, xs, compilers=[DirectCompiler, CPSCompiler])


def test_compile_splitz_cons_immediate_z():
    """Test splitZ with zero as first element."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.splitZ(s)

    xs = [PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
          PlusPuncA()]

    run_all(f, xs, compilers=[DirectCompiler, CPSCompiler])


def test_compile_splitz_cons_onez():
    """Test splitZ with zero in middle of list."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.splitZ(s)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
          PlusPuncA()]

    run_all(f, xs, compilers=[DirectCompiler])

def test_compile_concatmap_nil():
    """Test concat_map with nil function."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.concat_map(s, lambda _: yoink.nil())

    xs = [PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
          PlusPuncA()]

    run_all(f, xs, compilers=[DirectCompiler, CPSCompiler, GeneratorCompiler])

@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_id_preserves_output(input_events):
    """Property test: concat_map with identity compiles correctly."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.concat_map(s, lambda x: yoink.cons(x, yoink.nil()))

    assert has_type(input_events, TyStar(INT_TY))

    run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler])

@given(events_of_type(TyStar(INT_TY), max_depth=5))
@settings(max_examples=20)
def test_compile_concatmap_cons_one_preserves_output(input_events):
    """Property test: concat_map with cons(1, cons(x, nil)) compiles correctly."""
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.concat_map(s, lambda x: yoink.cons(yoink.singleton(1), yoink.cons(x, yoink.nil())))

    assert has_type(input_events, TyStar(INT_TY))

    run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler])



@given(events_of_type(INT_TY, max_depth=10))
@settings(max_examples=20)
def test_compile_wait_emit(input_events):
    @Yoink.jit
    def f(yoink, s: INT_TY):
        return yoink.emit(yoink.wait(s))

    assert has_type(input_events, INT_TY)

    run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler])



@given(events_of_type(TyStar(INT_TY), max_depth=20))
@settings(max_examples=20)
def test_compile_splitz(input_events):
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.splitZ(s)

    assert has_type(input_events, TyStar(INT_TY))

    run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler])

@given(events_of_type(TyStar(INT_TY), max_depth=20))
@settings(max_examples=20)
def test_compile_runs_of_nonz(input_events):
    @Yoink.jit
    def f(yoink, s: TyStar(INT_TY)):
        return yoink.runsOfNonZ(s)

    assert has_type(input_events, TyStar(INT_TY))

    run_all(f, input_events, compilers=[DirectCompiler, CPSCompiler])

