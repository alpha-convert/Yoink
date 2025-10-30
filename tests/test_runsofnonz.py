import pytest
from hypothesis import given
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)

def test_runsofnonz_nil():
    """Test runsOfNonZ on an empty list"""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    output = f(iter([PlusPuncA()]))
    result = [x for x in list(output) if x is not None]
    # Empty list should return empty list of runs
    assert result[0] == PlusPuncA()


def test_runsofnonz_all_nonz():
    """Test runsOfNonZ on a list with all non-zero values - should be one run"""
    @Delta.jit
    def f(delta, s: TyStar(INT_TY)):
        return delta.runsOfNonZ(s)

    input = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
             PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
             PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
             PlusPuncA()
             ]
    output = f(iter(input))
    result = [x for x in list(output) if x is not None]

    # Should be a single run containing [1, 2, 3]
    expected_result = [
        PlusPuncB(),  # cons
        CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(1))), CatEvA(CatPunc()),  # head: 1
        CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(2))), CatEvA(CatPunc()),  # 2
        CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(3))), CatEvA(CatPunc()),  # 3
        CatEvA(PlusPuncA()),  # end of list
        CatPunc(),
        PlusPuncA()  # no more runs
    ]
    assert result == expected_result


# def test_runsofnonz_immediate_z():
#     """Test runsOfNonZ starting with zero - should skip it and continue"""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.runsOfNonZ(s)

#     input = [PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]

#     # Should skip 0 and return one run: [5, 6]
#     expected_result = [
#         PlusPuncB(),  # cons
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(5))), CatEvA(CatPunc()),  # head: 5
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(6))), CatEvA(CatPunc()),  # 6
#         CatEvA(PlusPuncA()),  # end of list
#         CatPunc(),
#         PlusPuncA()  # no more runs
#     ]
#     assert result == expected_result


# def test_runsofnonz_one_z_in_middle():
#     """Test runsOfNonZ with a zero in the middle - should split into two runs"""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.runsOfNonZ(s)

#     input = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]

#     # Should be two runs: [1, 2, 3] and [5, 6]
#     expected_result = [
#         PlusPuncB(),  # cons - first run
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(1))), CatEvA(CatPunc()),  # 1
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(2))), CatEvA(CatPunc()),  # 2
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(3))), CatEvA(CatPunc()),  # 3
#         CatEvA(PlusPuncA()),  # end of first run
#         CatPunc(),
#         PlusPuncB(),  # cons - second run
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(5))), CatEvA(CatPunc()),  # 5
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(6))), CatEvA(CatPunc()),  # 6
#         CatEvA(PlusPuncA()),  # end of second run
#         CatPunc(),
#         PlusPuncA()  # no more runs
#     ]
#     assert result == expected_result


# def test_runsofnonz_end_z():
#     """Test runsOfNonZ ending with zero - should have one run then skip the zero"""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.runsOfNonZ(s)

#     input = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]

#     # Should be one run: [1, 2, 3], then skip the 0
#     expected_result = [
#         PlusPuncB(),  # cons
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(1))), CatEvA(CatPunc()),  # 1
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(2))), CatEvA(CatPunc()),  # 2
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(3))), CatEvA(CatPunc()),  # 3
#         CatEvA(PlusPuncA()),  # end of run
#         CatPunc(),
#         PlusPuncA()  # no more runs
#     ]
#     assert result == expected_result


# def test_runsofnonz_multiple_zeros():
#     """Test runsOfNonZ with multiple consecutive zeros"""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.runsOfNonZ(s)

#     input = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]

#     # Should be two runs: [1] and [5]
#     expected_result = [
#         PlusPuncB(),  # cons - first run
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(1))), CatEvA(CatPunc()),  # 1
#         CatEvA(PlusPuncA()),  # end of first run
#         CatPunc(),
#         PlusPuncB(),  # cons - second run
#         CatEvA(PlusPuncB()), CatEvA(CatEvA(BaseEvent(5))), CatEvA(CatPunc()),  # 5
#         CatEvA(PlusPuncA()),  # end of second run
#         CatPunc(),
#         PlusPuncA()  # no more runs
#     ]
#     assert result == expected_result


# def test_runsofnonz_all_zeros():
#     """Test runsOfNonZ with all zeros - should return empty list"""
#     @Delta.jit
#     def f(delta, s: TyStar(INT_TY)):
#         return delta.runsOfNonZ(s)

#     input = [PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncB(), CatEvA(BaseEvent(0)), CatPunc(),
#              PlusPuncA()
#              ]
#     output = f(iter(input))
#     result = [x for x in list(output) if x is not None]

#     # Should be empty list
#     expected_result = [PlusPuncA()]
#     assert result == expected_result
