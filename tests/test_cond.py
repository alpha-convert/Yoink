import pytest
from python_delta.core import Delta, Singleton, TyPlus, PlusPuncA, PlusPuncB, TyCat, BaseEvent
# This needs to test cond typing:
# requires that b is Singleton(bool), and the left/rigth have the same types.
# Needs to test that b cannot come after the branches

def test_cond_bool_check():
    with pytest.raises(Exception):
        @Delta.jit
        def f(delta, b : Singleton(str), l : Singleton(str), r : Singleton(str)):
            return delta.cond(b, l, r)

def test_cond_branches_check():
    with pytest.raises(Exception):
        @Delta.jit
        def f(delta, b : Singleton(bool), l : Singleton(str), r : Singleton(bool)):
            return delta.cond(b, l, r)

def test_cond_bad_ordering1():
    with pytest.raises(Exception):
        @Delta.jit
        def f(delta, lb : TyCat(Singleton(str),Singleton(bool)), r : Singleton(str)):
            l,b = delta.catl(lb)
            return delta.cond(b, l, r)

def test_cond_bad_ordering2():
    with pytest.raises(Exception):
        @Delta.jit
        def f(delta, rb : TyCat(Singleton(str),Singleton(bool)), l : Singleton(str)):
            r,b = delta.catl(rb)
            return delta.cond(b, l, r)

def test_cond_ok_ordering1():
    @Delta.jit
    def f(delta, bl : TyCat(Singleton(bool),Singleton(str)), r : Singleton(str)):
        b,l = delta.catl(bl)
        return delta.cond(b, l, r)

def test_cond_ok_ordering2():
    @Delta.jit
    def f(delta, br : TyCat(Singleton(bool),Singleton(str)), l : Singleton(str)):
        b,r = delta.catl(br)
        return delta.cond(b, l, r)

def test_cond_exec_true():
    """Test cond execution with true branch."""
    @Delta.jit
    def f(delta, b: Singleton(bool), l: Singleton(str), r: Singleton(str)):
        return delta.cond(b, l, r)

    output = f.run(iter([BaseEvent(True)]), iter([BaseEvent("left")]), iter([BaseEvent("right")]))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent("left")]

def test_cond_exec_false():
    """Test cond execution with false branch."""
    @Delta.jit
    def f(delta, b: Singleton(bool), l: Singleton(str), r: Singleton(str)):
        return delta.cond(b, l, r)

    output = f.run(iter([BaseEvent(False)]), iter([BaseEvent("left")]), iter([BaseEvent("right")]))
    result = [x for x in list(output) if x is not None]

    assert result == [BaseEvent("right")]