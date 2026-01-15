
import pytest
from hypothesis import given, settings
from yoink.core import Yoink, Singleton, TyStar, TyCat, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent, TyPlus
from yoink.util.hypothesis_strategies import events_of_type
from yoink.typecheck.has_type import has_type

INT_TY = Singleton(int)
STRING_TY = Singleton(str)

def test_wait_emit_int():
    @Yoink.jit
    def f(yoink, x: INT_TY):
        y = yoink.wait(x)
        return yoink.emit(y)

    xs = [BaseEvent(1)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent(1)]

def test_wait_emit_cat():
    @Yoink.jit
    def f(yoink, x: TyCat(INT_TY,INT_TY)):
        y = yoink.wait(x)
        return yoink.emit(y)

    xs = [CatEvA(BaseEvent(1)),CatPunc(),BaseEvent(2)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == xs

def test_wait_emit_plus():
    @Yoink.jit
    def f(yoink, x: TyPlus(INT_TY,INT_TY)):
        y = yoink.wait(x)
        return yoink.emit(y)

    xs = [PlusPuncA(), BaseEvent(1)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == xs

def test_wait_emit_int_plus_one():
    @Yoink.jit
    def f(yoink, x: INT_TY):
        y = yoink.wait(x)
        return yoink.emit(y + 1)

    xs = [BaseEvent(1)]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent(2)]

def test_wait_emit_int_plus():
    @Yoink.jit
    def f(yoink, x: INT_TY, y : INT_TY):
        x = yoink.wait(x)
        y = yoink.wait(y)
        return yoink.emit(x + y)

    xs = [BaseEvent(1)]
    ys = [BaseEvent(2)]

    output = f(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent(3)]

def test_map_plus_one():
    @Yoink.jit
    def f(yoink, xs: TyStar(INT_TY)):
        return yoink.map(xs,lambda x : yoink.emit(yoink.wait(x) + 1))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncA()]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(), PlusPuncA()]

def test_zip_with_sum():
    @Yoink.jit
    def zip_sum(yoink, xs: TyStar(INT_TY), ys: TyStar(INT_TY)):
        return yoink.zip_with(xs, ys, lambda x, y: yoink.emit(yoink.wait(x) + yoink.wait(y)))

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(3)), CatPunc(),
          PlusPuncA()]
    ys = [PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
          PlusPuncB(), CatEvA(BaseEvent(6)), CatPunc(),
          PlusPuncA()]

    output = zip_sum(iter(xs), iter(ys))
    result = [x for x in list(output) if x is not None]

    expected = [PlusPuncB(), CatEvA(BaseEvent(5)), CatPunc(),
                PlusPuncB(), CatEvA(BaseEvent(7)), CatPunc(),
                PlusPuncB(), CatEvA(BaseEvent(9)), CatPunc(),
                PlusPuncA()]
    assert result == expected

def test_map_double():
    @Yoink.jit

    def f(yoink, xs: TyStar(INT_TY)):
        def body(x):
            y = yoink.wait(x)
            return yoink.emit(y + y)
        return yoink.map(xs,body)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncA()]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(4)), CatPunc(), PlusPuncA()]

def test_wait_emit_cat_parallel_inps():
    @Yoink.jit
    def f(yoink, x: INT_TY, y : INT_TY):
        z = yoink.wait(x)
        return yoink.catr(yoink.emit(z + 2),y)

    x = [BaseEvent(0)]
    y = [BaseEvent(1)]

    output = f(iter(x),iter(y))
    result = [x for x in list(output) if x is not None]

    assert result == [CatEvA(BaseEvent(2)), CatPunc(), BaseEvent(1)]


# TODO: test backwards inputs! ensure that fails
def test_wait_emit_cat_cat_inps():
    @Yoink.jit
    def f(yoink, xy: TyCat(INT_TY,INT_TY)):
        x,y = yoink.catl(xy)
        z = yoink.wait(x)
        return yoink.catr(yoink.emit(z + 2),y)

    xy = [CatEvA(BaseEvent(0)),CatPunc(), BaseEvent(1)]

    output = f(iter(xy))
    result = [x for x in list(output) if x is not None]

    assert result == [CatEvA(BaseEvent(2)), CatPunc(), BaseEvent(1)]


def test_wait_emit_cat_cat_inps_backwards():
    with pytest.raises(Exception):
        @Yoink.jit
        def f(yoink, xy: TyCat(INT_TY,INT_TY)):
            x,y = yoink.catl(xy)
            z = yoink.wait(y)
            return yoink.catr(yoink.emit(z + 2),x)



def test_wait_head():
    @Yoink.jit
    def f(yoink, xs: TyStar(INT_TY), b : INT_TY):
        def nil_case(_):
            return yoink.catr(b,yoink.nil())
        def cons_case(x,xs):
            y = yoink.wait(x)
            return yoink.catr(yoink.emit(y),xs)
        return yoink.starcase(xs,nil_case,cons_case)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncA()]
    b = [BaseEvent(0)]

    output = f(iter(xs),iter(b))
    result = [x for x in list(output) if x is not None]

    assert result == [CatEvA(BaseEvent(1)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncA()]
