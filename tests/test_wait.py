
import pytest
from hypothesis import given, settings
from python_delta.core import Delta, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent, TyPlus
from python_delta.util.hypothesis_strategies import events_of_type
from python_delta.typecheck.has_type import has_type


INT_TY = Singleton(int)
STRING_TY = Singleton(str)


def test_wait_emit_int():
    @Delta.jit
    def f(delta, x: INT_TY):
        y = delta.wait(x)
        return delta.emit(y)

    xs = [BaseEvent(1)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent(1)]

def test_wait_emit_cat():
    @Delta.jit
    def f(delta, x: TyCat(INT_TY,INT_TY)):
        y = delta.wait(x)
        return delta.emit(y)

    xs = [CatEvA(BaseEvent(1)),CatPunc(),BaseEvent(2)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == xs

def test_wait_emit_plus():
    @Delta.jit
    def f(delta, x: TyPlus(INT_TY,INT_TY)):
        y = delta.wait(x)
        return delta.emit(y)

    xs = [PlusPuncA(), BaseEvent(1)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == xs

def test_wait_emit_int_plus_one():
    @Delta.jit
    def f(delta, x: INT_TY):
        y = delta.wait(x)
        return delta.emit(y + 1)

    xs = [BaseEvent(1)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent(2)]

def test_wait_emit_int_plus():
    @Delta.jit
    def f(delta, x: INT_TY, y : INT_TY):
        x = delta.wait(x)
        y = delta.wait(y)
        return delta.emit(x + y)

    xs = [BaseEvent(1)]
    ys = [BaseEvent(2)]

    output = f(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent(3)]