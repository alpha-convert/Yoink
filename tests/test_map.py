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

    xs = [PlusPuncB(),CatEvA(CatEvA(BaseEvent(0))),CatEvA(CatPunc()),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncA()]
    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]
    expected_result = [PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),PlusPuncA()]
    for i in range(max(len(expected_result),len(result))):
        assert result[i] == expected_result[i]


@given(events_of_type(TyStar(TyCat(INT_TY, INT_TY)), max_depth=5))
def test_map_proj1_preserves_types(input_events):
    output_type = TyStar(INT_TY)
    input_type = TyStar(TyCat(INT_TY, INT_TY))
    @Delta.jit
    def f(delta, s: input_type):
        def proj1(z):
            (x, y) = delta.catl(z)
            return x
        return delta.map(s, proj1)

    assert has_type(input_events,input_type)
    # Run the function with generated input
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    # Check that output has the expected type
    assert has_type(result, output_type), f"Output does not have type {output_type}"



def test_map_proj2_cons():
    @Delta.jit
    def f(delta,s : TyStar(TyCat(INT_TY,INT_TY))):
        def proj1(z):
            (_,y) = delta.catl(z)
            return y
        return delta.map(s,proj1)

    xs = [PlusPuncB(),CatEvA(CatEvA(BaseEvent(0))),CatEvA(CatPunc()),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncA()]
    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]
    expected_result = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncA()]
    for i in range(max(len(expected_result),len(result))):
        assert result[i] == expected_result[i]


@given(events_of_type(TyStar(TyCat(INT_TY, INT_TY)), max_depth=5))
def test_map_proj2_preserves_types(input_events):
    output_type = TyStar(INT_TY)
    input_type = TyStar(TyCat(INT_TY, INT_TY))
    @Delta.jit
    def f(delta, s: input_type):
        def proj1(z):
            (_, y) = delta.catl(z)
            return y
        return delta.map(s, proj1)

    assert has_type(input_events,input_type)
    # Run the function with generated input
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    # Check that output has the expected type
    assert has_type(result, output_type), f"Output does not have type {output_type}"


@given(events_of_type(TyStar(TyStar(TyCat(INT_TY, INT_TY))), max_depth=5))
def test_map_map_proj1_preserves_types(input_events):
    output_type = TyStar(TyStar(INT_TY))
    input_type = TyStar(TyStar(TyCat(INT_TY, INT_TY)))
    @Delta.jit
    def f(delta, s: input_type):
        def proj1(z):
            (x, _) = delta.catl(z)
            return x
        def map_proj1(inner_list):
            return delta.map(inner_list, proj1)
        return delta.map(s, map_proj1)

    assert has_type(input_events, input_type)
    # Run the function with generated input
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    # Check that output has the expected type
    assert has_type(result, output_type), f"Output does not have type {output_type}"

