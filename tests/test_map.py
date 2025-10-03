import pytest
from hypothesis import given
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)


def test_map_id_nil():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.map(s,lambda x : x)

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == PlusPuncA()

def test_map_id_cons():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.map(s,lambda x : x)

    xs = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]
    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]
    assert result == xs

def test_map_proj1_cons():
    @Delta.jit
    def f(delta,s : TyStar(TyCat(INT_TY,INT_TY))):
        def proj1(z):
            (x,y) = delta.catl(z)
            return x
        return delta.map(s,proj1)

    xs = [PlusPuncB(),CatEvA(CatEvA(BaseEvent(0))),CatEvA(CatPunc()),CatEvA(BaseEvent(0)),CatPunc(),PlusPuncA()]
    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]
    expected_result = [PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),PlusPuncA()]
    for i in range(max(len(expected_result),len(result))):
        assert result[i] == expected_result[i]


@given(events_of_type(TyStar(TyCat(INT_TY, INT_TY)), max_depth=3))
def test_map_proj1_preserves_types(input_events):
    @Delta.jit
    def f(delta, s: TyStar(TyCat(INT_TY, INT_TY))):
        def proj1(z):
            (x, y) = delta.catl(z)
            return x
        return delta.map(s, proj1)

    # Run the function with generated input
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    # Check that output has the expected type
    output_type = TyStar(INT_TY)
    assert has_type(result, output_type), f"Output does not have type {output_type}"

