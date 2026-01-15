import pytest
from hypothesis import given
from yoink.core import Yoink, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from yoink.util.hypothesis_strategies import events_of_type
from yoink.typecheck.has_type import has_type


INT_TY = Singleton(int)

def test_concatmap_nil_nil():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.concat_map(s,lambda _ : yoink.nil())

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == PlusPuncA()

def test_concatmap_nil_cons():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.concat_map(s,lambda _ : yoink.nil())

    output = f(iter([PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == PlusPuncA()

def test_concatmap_id_nil():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.concat_map(s,lambda x : yoink.cons(x,yoink.nil()))

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == PlusPuncA()

def test_concatmap_inj():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.concat_map(s,lambda x : yoink.cons(x,yoink.nil()))

    input = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    assert result == input


def test_concatmap_one():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.concat_map(s,lambda x : yoink.cons(yoink.singleton(1),yoink.cons(x,yoink.nil())))

    input = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncA()]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    assert result == [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncA()]

def test_concatmap_one_another():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.concat_map(s,lambda x : yoink.cons(yoink.singleton(1),yoink.cons(x,yoink.nil())))

    input = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    assert result == [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
                      PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
                      PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
                      PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),
                      PlusPuncA()]


@given(events_of_type(TyStar(INT_TY), max_depth=5))
def test_concatmap_cons_one_preserves_types(input_events):
    """Property test: concat_map with cons(1, cons(x, nil)) preserves types."""
    input_type = TyStar(INT_TY)
    @Yoink.jit
    def f(yoink,s : input_type):
        return yoink.concat_map(s,lambda x : yoink.cons(yoink.singleton(1),yoink.cons(x,yoink.nil())))

    assert has_type(input_events,input_type)
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, input_type), f"Output does not have type {input_type}"


@given(events_of_type(TyStar(INT_TY), max_depth=5))
def test_concatmap_nil_preserves_types(input_events):
    """Property test: concat_map with nil always produces empty list."""
    input_type = TyStar(INT_TY)
    @Yoink.jit
    def f(yoink, s: input_type):
        return yoink.concat_map(s, lambda _: yoink.nil())

    assert has_type(input_events, input_type)
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, input_type), f"Output does not have type {input_type}"
    # concat_map with nil should always produce nil
    assert result == [PlusPuncA()]


@given(events_of_type(TyStar(INT_TY), max_depth=5))
def test_concatmap_id_preserves_types(input_events):
    """Property test: concat_map with identity (cons(x, nil)) preserves input."""
    input_type = TyStar(INT_TY)
    @Yoink.jit
    def f(yoink, s: input_type):
        return yoink.concat_map(s, lambda x: yoink.cons(x, yoink.nil()))

    assert has_type(input_events, input_type)
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, input_type), f"Output does not have type {input_type}"

@given(events_of_type(TyStar(INT_TY), max_depth=5))
def test_concatmap_const_preserves_types(input_events):
    input_type = TyStar(INT_TY)
    @Yoink.jit
    def f(yoink, s: input_type):
        return yoink.concat_map(s, lambda x: yoink.cons(yoink.singleton(0), yoink.nil()))

    assert has_type(input_events, input_type)
    output = f(iter(input_events))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, input_type), f"Output does not have type {input_type}"