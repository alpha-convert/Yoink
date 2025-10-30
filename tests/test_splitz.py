import pytest
from hypothesis import given
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)

def test_splitz_nil():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.splitZ(s)

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == CatEvA(PlusPuncA())
    assert result[1] == CatPunc()
    assert result[2] == PlusPuncA()

def test_splitz_cons_all_nonz():
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.splitZ(s)

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
    @Delta.jit
    def f(delta,s : TyStar(INT_TY)):
        return delta.splitZ(s)

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

# def test_splitz_cons_onez():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.splitZ(s)

#     input = [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(2)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(0)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(5)),CatPunc(),
#              PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]
#     expected_result = [
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(1))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(2))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncB()),CatEvA(CatEvA(BaseEvent(3))),CatEvA(CatPunc()),
#         CatEvA(PlusPuncA()),
#         CatPunc(),
#         PlusPuncB(), CatEvA(BaseEvent(5)),CatPunc(),
#         PlusPuncB(),CatEvA(BaseEvent(6)),CatPunc(),
#         PlusPuncA()
#     ]
#     assert result == expected_result


# def test_concatmap_nil_cons():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.concat_map(s,lambda _ : delta.nil())

#     output = f(iter([PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]))
#     result = [x for x in list(output) if x is not None]
#     assert result[0] == PlusPuncA()

# def test_concatmap_id_nil():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.concat_map(s,lambda x : delta.cons(x,delta.nil()))

#     output = f(iter([PlusPuncA()]))
#     result = [x for x in list(output) if x is not None]
#     assert result[0] == PlusPuncA()

# def test_concatmap_inj():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.concat_map(s,lambda x : delta.cons(x,delta.nil()))

#     input = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]
#     assert result == input


# def test_concatmap_one():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.concat_map(s,lambda x : delta.cons(delta.singleton(1),delta.cons(x,delta.nil())))

#     input = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncA()]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]
#     assert result == [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncA()]

# def test_concatmap_one_another():
#     @Delta.jit
#     def f(delta,s : TyStar(INT_TY)):
#         return delta.concat_map(s,lambda x : delta.cons(delta.singleton(1),delta.cons(x,delta.nil())))

#     input = [PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]
#     assert result == [PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(3)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(1)),CatPunc(),PlusPuncB(),CatEvA(BaseEvent(4)),CatPunc(),PlusPuncA()]


# @given(events_of_type(TyStar(INT_TY), max_depth=5))
# def test_map_proj1_preserves_types(input_events):
#     input_type = TyStar(INT_TY)
#     @Delta.jit
#     def f(delta,s : input_type):
#         return delta.concat_map(s,lambda x : delta.cons(delta.singleton(1),delta.cons(x,delta.nil())))

#     assert has_type(input_events,input_type)
#     output = f(iter(input_events))
#     result = [x for x in list(output) if x is not None]

#     assert has_type(result, input_type), f"Output does not have type {input_type}"