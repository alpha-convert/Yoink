"""Tests for zip_with operation."""

import pytest
from hypothesis import given, settings
from yoink.core import Yoink, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent
from yoink.util.hypothesis_strategies import events_of_type
from yoink.typecheck.has_type import has_type


INT_TY = Singleton(int)
STRING_TY = Singleton(str)


def test_zip_with_catr_empty():
    """Test zip_with with CatR on empty lists."""
    @Yoink.jit
    def zip_pair(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

    xs = [PlusPuncA()]
    ys = [PlusPuncA()]

    output = zip_pair(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncA()]


def test_zip_with_catr_single():
    """Test zip_with with CatR on single element lists."""
    @Yoink.jit
    def zip_pair(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

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
    @Yoink.jit
    def zip_pair(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

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
    @Yoink.jit
    def zip_fst(yoink, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: x)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(), PlusPuncA()]

    output = zip_fst(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    assert has_type(result, TyStar(INT_TY))


def test_zip_with_fst_multiple():
    @Yoink.jit
    def zip_fst(yoink, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: x)

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
    @Yoink.jit
    def zip_snd(yoink, xs: TyStar(STRING_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: y)

    xs = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(), PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]

    output = zip_snd(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    # Expected: [1] - just the second element
    assert result == [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncA()]
    assert has_type(result, TyStar(INT_TY))


def test_zip_with_snd_multiple():
    """Test zip_with using only second argument - multiple elements."""
    @Yoink.jit
    def zip_snd(yoink, xs: TyStar(STRING_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: y)

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
    @Yoink.jit
    def zip_swap(yoink, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(y, x))

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

    @Yoink.jit
    def zip_nested(yoink, xs: input_type, ys: input_type):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

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
    @Yoink.jit
    def zip_pair(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.catr(x, y))

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
    @Yoink.jit
    def zip_fst(yoink, xs: TyStar(INT_TY), ys: TyStar(STRING_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: x)

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
    @Yoink.jit
    def zip_snd(yoink, xs: TyStar(STRING_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: y)

    assert has_type(xs, TyStar(STRING_TY))
    assert has_type(ys, TyStar(INT_TY))

    output = zip_snd(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert has_type(result, TyStar(INT_TY))
