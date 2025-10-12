"""Tests for zip_with operation."""

import pytest
from hypothesis import given, settings
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)
STRING_TY = Singleton(str)


def test_zip_with_catr_empty():
    """Test zip_with with CatR on empty lists."""
    @Delta.jit
    def zip_pair(delta, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(x, y))

    xs = [PlusPuncA()]
    ys = [PlusPuncA()]

    output = zip_pair(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncA()]


def test_zip_with_catr_single():
    """Test zip_with with CatR on single element lists."""
    @Delta.jit
    def zip_pair(delta, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(x, y))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(10)), CatPunc(), PlusPuncA()]

    output = zip_pair(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [(1, 10)]
    # Structure: PlusPuncB, CatEvA(CatEvA(1)), CatEvA(CatPunc), CatEvA(10), CatPunc, PlusPuncA
    expected = [PlusPuncB(),
                CatEvA(CatEvA(BaseEvent(1))),
                CatEvA(CatPunc()),
                CatEvA(BaseEvent(10)),
                CatPunc(),
                PlusPuncA()]
    assert result == expected
    assert has_type(result, TyStar(TyCat(INT_TY, INT_TY)))


def test_zip_with_catr_multiple():
    """Test zip_with with CatR on multiple element lists."""
    @Delta.jit
    def zip_pair(delta, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(x, y))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(10)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(20)), CatPunc(),
          PlusPuncA()]

    output = zip_pair(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [(1, 10), (2, 20)]
    expected = [PlusPuncB(),
                CatEvA(CatEvA(BaseEvent(1))),
                CatEvA(CatPunc()),
                CatEvA(BaseEvent(10)),
                CatPunc(),
                PlusPuncB(),
                CatEvA(CatEvA(BaseEvent(2))),
                CatEvA(CatPunc()),
                CatEvA(BaseEvent(20)),
                CatPunc(),
                PlusPuncA()]
    assert result == expected
    assert has_type(result, TyStar(TyCat(INT_TY, INT_TY)))



def test_zip_with_fst_single():
    """Test zip_with using only first argument - projection on fst."""
    @Delta.jit
    def zip_fst(delta, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return delta.zip_with(xs, ys, lambda x, y: x)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(), PlusPuncA()]

    output = zip_fst(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    assert has_type(result, TyStar(INT_TY))


def test_zip_with_fst_multiple():
    @Delta.jit
    def zip_fst(delta, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return delta.zip_with(xs, ys, lambda x, y: x)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent("b")), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent("c")), CatPunc(),
          PlusPuncA()]

    output = zip_fst(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [1, 2, 3]
    expected = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
                PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
                PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
                PlusPuncA()]
    assert result == expected
    assert has_type(result, TyStar(INT_TY))


def test_zip_with_snd_single():
    @Delta.jit
    def zip_snd(delta, xs: TyStar(STRING_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: y)

    xs = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(), PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]

    output = zip_snd(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [1] - just the second element
    assert result == [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    assert has_type(result, TyStar(INT_TY))


def test_zip_with_snd_multiple():
    """Test zip_with using only second argument - multiple elements."""
    @Delta.jit
    def zip_snd(delta, xs: TyStar(STRING_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: y)

    xs = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent("b")), CatPunc(),
          PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(10)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(20)), CatPunc(),
          PlusPuncA()]

    output = zip_snd(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [10, 20]
    expected = [PlusPuncB(), CatEvA(BaseEvent(10)), CatPunc(),
                PlusPuncB(), CatEvA(BaseEvent(20)), CatPunc(),
                PlusPuncA()]
    assert result == expected
    assert has_type(result, TyStar(INT_TY))


def test_zip_with_swap():
    """Test zip_with that swaps the order - catr(y, x) instead of catr(x, y)."""
    @Delta.jit
    def zip_swap(delta, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(y, x))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(), PlusPuncA()]

    output = zip_swap(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [("a", 1)] - swapped order
    expected = [PlusPuncB(),
                CatEvA(CatEvA(BaseEvent("a"))),
                CatEvA(CatPunc()),
                CatEvA(BaseEvent(1)),
                CatPunc(),
                PlusPuncA()]
    assert result == expected
    assert has_type(result, TyStar(TyCat(STRING_TY, INT_TY)))


def test_zip_with_nested_pair():
    """Test zip_with with nested pairs."""
    input_type = TyStar(TyCat(INT_TY, INT_TY))

    @Delta.jit
    def zip_nested(delta, xs: input_type, ys: input_type):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(x, y))

    # Input: [(1, 2), (3, 4)]
    xs = [PlusPuncB(),
          CatEvA(CatEvA(BaseEvent(1))), CatEvA(CatPunc()), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncB(),
          CatEvA(CatEvA(BaseEvent(3))), CatEvA(CatPunc()), CatEvA(BaseEvent(4)), CatPunc(),
          PlusPuncA()]

    # Input: [(10, 20), (30, 40)]
    ys = [PlusPuncB(),
          CatEvA(CatEvA(BaseEvent(10))), CatEvA(CatPunc()), CatEvA(BaseEvent(20)), CatPunc(),
          PlusPuncB(),
          CatEvA(CatEvA(BaseEvent(30))), CatEvA(CatPunc()), CatEvA(BaseEvent(40)), CatPunc(),
          PlusPuncA()]

    output = zip_nested(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [((1, 2), (10, 20)), ((3, 4), (30, 40))]
    expected_type = TyStar(TyCat(TyCat(INT_TY, INT_TY), TyCat(INT_TY, INT_TY)))
    assert has_type(result, expected_type)


# Hypothesis property tests

@given(
    events_of_type(TyStar(INT_TY), max_depth=3),
    events_of_type(TyStar(INT_TY), max_depth=3)
)
@settings(max_examples=20)
def test_zip_with_catr_preserves_types(xs, ys):
    """Property test: zip_with with catr preserves types."""
    @Delta.jit
    def zip_pair(delta, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: delta.catr(x, y))

    assert has_type(xs, TyStar(INT_TY))
    assert has_type(ys, TyStar(INT_TY))

    output = zip_pair(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, TyStar(TyCat(INT_TY, INT_TY)))


@given(
    events_of_type(TyStar(INT_TY), max_depth=3),
    events_of_type(TyStar(STRING_TY), max_depth=3)
)
@settings(max_examples=20)
def test_zip_with_fst_preserves_types(xs, ys):
    """Property test: zip_with with fst projection preserves types."""
    @Delta.jit
    def zip_fst(delta, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return delta.zip_with(xs, ys, lambda x, y: x)

    assert has_type(xs, TyStar(INT_TY))
    assert has_type(ys, TyStar(STRING_TY))

    output = zip_fst(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, TyStar(INT_TY))


@given(
    events_of_type(TyStar(STRING_TY), max_depth=3),
    events_of_type(TyStar(INT_TY), max_depth=3)
)
@settings(max_examples=20)
def test_zip_with_snd_preserves_types(xs, ys):
    """Property test: zip_with with snd projection preserves types."""
    @Delta.jit
    def zip_snd(delta, xs: TyStar(STRING_TY), ys: TyStar(INT_TY)):
        return delta.zip_with(xs, ys, lambda x, y: y)

    assert has_type(xs, TyStar(STRING_TY))
    assert has_type(ys, TyStar(INT_TY))

    output = zip_snd(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, TyStar(INT_TY))
