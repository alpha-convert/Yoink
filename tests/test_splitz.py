import pytest
from hypothesis import given
from yoink.core import Yoink, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from yoink.util.hypothesis_strategies import events_of_type
from yoink.typecheck.has_type import has_type


INT_TY = Singleton(int)

def test_splitz_nil():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.splitZ(s)

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == CatEvA(PlusPuncA())
    assert result[1] == CatPunc()
    assert result[2] == PlusPuncA()

def test_splitz_cons_all_nonz():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.splitZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
        CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_immediate_z():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.splitZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncB(), CatEvA(BaseEvent(5)),CatPunc(),
        PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_onez():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.splitZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
        CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncB(), CatEvA(BaseEvent(5)),CatPunc(),
        PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_endz():
    @Yoink.jit
    def f(yoink,s : TyStar(INT_TY)):
        return yoink.splitZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(5))),CatEvA(CatPunc()),
        CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(6))),CatEvA(CatPunc()),
        CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result