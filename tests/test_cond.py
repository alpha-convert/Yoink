import pytest
from yoink.core import Yoink, Singleton, TyPlus, PlusPuncA, PlusPuncB, TyCat, BaseEvent
# This needs to test cond typing:
# requires that b is Singleton(bool), and the left/rigth have the same types.
# Needs to test that b cannot come after the branches

def test_cond_bool_check():
    with pytest.raises(Exception):
        @Yoink.jit
        def f(yoink, b : Singleton(str), l : Singleton(str), r : Singleton(str)):
            return yoink.cond(b, l, r)

def test_cond_branches_check():
    with pytest.raises(Exception):
        @Yoink.jit
        def f(yoink, b : Singleton(bool), l : Singleton(str), r : Singleton(bool)):
            return yoink.cond(b, l, r)

def test_cond_bad_ordering1():
    with pytest.raises(Exception):
        @Yoink.jit
        def f(yoink, lb : TyCat(Singleton(str),Singleton(bool)), r : Singleton(str)):
            l,b = yoink.catl(lb)
            return yoink.cond(b, l, r)

def test_cond_bad_ordering2():
    with pytest.raises(Exception):
        @Yoink.jit
        def f(yoink, rb : TyCat(Singleton(str),Singleton(bool)), l : Singleton(str)):
            r,b = yoink.catl(rb)
            return yoink.cond(b, l, r)

def test_cond_ok_ordering1():
    @Yoink.jit
    def f(yoink, bl : TyCat(Singleton(bool),Singleton(str)), r : Singleton(str)):
        b,l = yoink.catl(bl)
        return yoink.cond(b, l, r)

def test_cond_ok_ordering2():
    @Yoink.jit
    def f(yoink, br : TyCat(Singleton(bool),Singleton(str)), l : Singleton(str)):
        b,r = yoink.catl(br)
        return yoink.cond(b, l, r)

def test_cond_exec_true():
    """Test cond execution with true branch."""
    @Yoink.jit
    def f(yoink, b: Singleton(bool), l: Singleton(str), r: Singleton(str)):
        return yoink.cond(b, l, r)

    output = f.run(iter([BaseEvent(True)]), iter([BaseEvent("left")]), iter([BaseEvent("right")]))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent("left")]

def test_cond_exec_false():
    """Test cond execution with false branch."""
    @Yoink.jit
    def f(yoink, b: Singleton(bool), l: Singleton(str), r: Singleton(str)):
        return yoink.cond(b, l, r)

    output = f.run(iter([BaseEvent(False)]), iter([BaseEvent("left")]), iter([BaseEvent("right")]))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent("right")]