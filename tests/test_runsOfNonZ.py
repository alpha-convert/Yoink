import pytest
from hypothesis import given
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)

def test_ronz_nil():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        PlusPuncB(), CatEvA(PlusPuncA()), CatPunc(),
        PlusPuncA()
    ]

    assert expected_result == result


def test_ronz_none():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_immediate_z():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(5))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(6))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_onez():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

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
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(5))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(6))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_twoz():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(5))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(6))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

def test_splitz_cons_twoz_endz():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
             PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]
    expected_result = [
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncB(),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(5))),CatEvA(CatPunc()),
            CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(6))),CatEvA(CatPunc()),
            CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncB(), CatEvA(PlusPuncA()),
        CatPunc(),
        PlusPuncA()
    ]
    assert result == expected_result

# def test_ronz_zero_end():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.runsOfNonZ(s)

#     input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]
#     expected_result = [
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(5))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(6))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncA()),
#         CatPunc(),
#         PlusPuncA()
#     ]
#     assert result == expected_result
