import pytest
from hypothesis import given
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)

def test_concat_emp0_emp():
    @Delta.jit
    def f(delta,xs : TyStar(INT_TY), ys : TyStar(INT_TY)):
        return delta.concat(xs,ys)

    output = f(iter([]),iter([PlusPuncA()]))
    expected_result = []
    result = [x for x in list(output) if x is not None]
    assert result == expected_result

def test_concat_emp_emp():
    @Delta.jit
    def f(delta,xs : TyStar(INT_TY), ys : TyStar(INT_TY)):
        return delta.concat(xs,ys)

    output = f(iter([PlusPuncA()]),iter([PlusPuncA()]))
    expected_result = [PlusPuncA()]
    result = [x for x in list(output) if x is not None]
    assert result == expected_result

def test_concat_emp_cons():
    @Delta.jit
    def f(delta,xs : TyStar(INT_TY), ys : TyStar(INT_TY)):
        return delta.concat(xs,ys)

    xs = [PlusPuncA()]
    ys = [PlusPuncB(),CatEvA(3),CatPunc(),PlusPuncB(),CatEvA(4),CatPunc()]
    output = f(iter(xs),iter(ys))
    expected_result = ys
    result = [x for x in list(output) if x is not None]
    assert result == expected_result

def test_concat_cons_cons():
    @Delta.jit
    def f(delta,xs : TyStar(INT_TY), ys : TyStar(INT_TY)):
        return delta.concat(xs,ys)

    xs = [PlusPuncB(),CatEvA(1),CatPunc(),PlusPuncB(),CatEvA(2),CatPunc(),PlusPuncA()]
    ys = [PlusPuncB(),CatEvA(3),CatPunc(),PlusPuncB(),CatEvA(4),CatPunc(),PlusPuncA()]
    output = f(iter(xs),iter(ys))
    expected_result = [PlusPuncB(),CatEvA(1),CatPunc(),PlusPuncB(),CatEvA(2),CatPunc(),PlusPuncB(),CatEvA(3),CatPunc(),PlusPuncB(),CatEvA(4),CatPunc(),PlusPuncA()]
    result = [x for x in list(output) if x is not None]
    assert result == expected_result


@given(
    events_of_type(TyStar(INT_TY), max_depth=5),
    events_of_type(TyStar(INT_TY), max_depth=5)
)
def test_concat_preserves_types(xs, ys):
    """Property test: concat preserves types and produces valid output."""
    @Delta.jit
    def concat_test(delta, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return delta.concat(xs, ys)

    assert has_type(xs, TyStar(INT_TY))
    assert has_type(ys, TyStar(INT_TY))

    # Run the function with generated inputs
    output = concat_test(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Check that output has the expected type
    assert has_type(result, TyStar(INT_TY)), f"Output does not have type TyStar(INT_TY)"

