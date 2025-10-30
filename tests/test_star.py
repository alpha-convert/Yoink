import pytest
from python_delta.core import Delta, Singleton, TyStar, PlusPuncA, PlusPuncB, CatEvA, CatPunc, BaseEvent


STRING_TY = Singleton(str)
INT_TY = Singleton(int)


def test_nil():
    """Test nil creates empty star type."""
    @Delta.jit
    def f(delta):
        return delta.nil()

    output = f()
    result = list(output)

    # nil elaborates to InL(Eps), so we expect: PlusPuncA, then StopIteration
    assert result[0] == PlusPuncA()
    # No more elements after the tag


def test_cons_nil():
    """Test cons with nil creates a single-element list."""
    @Delta.jit
    def f(delta, x: STRING_TY):
        nil_list = delta.nil()
        return delta.cons(x, nil_list)

    output = f(iter(["hello"]))
    result = list(output)

    # cons elaborates to InR(CatR(head, tail))
    # So we expect: PlusPuncB, CatEvA("hello"), CatPunc, PlusPuncA
    assert result[0] == PlusPuncB()
    assert result[1] == CatEvA("hello")
    assert result[2] == CatPunc()
    assert result[3] == PlusPuncA()


def test_cons_cons_nil():
    """Test cons with cons-nil creates a two-element list."""
    @Delta.jit
    def f(delta, x: STRING_TY, y: STRING_TY):
        nil_list = delta.nil()
        one_elem = delta.cons(y, nil_list)
        two_elem = delta.cons(x, one_elem)
        return two_elem

    output = f(iter(["a"]), iter(["b"]))
    result = list(output)

    # Expected structure: InR(CatR(x, InR(CatR(y, InL(Eps)))))
    # PlusPuncB, CatEvA("a"), CatPunc, PlusPuncB, CatEvA("b"), CatPunc, PlusPuncA
    assert result[0] == PlusPuncB()
    assert result[1] == CatEvA("a")
    assert result[2] == CatPunc()
    assert result[3] == PlusPuncB()
    assert result[4] == CatEvA("b")
    assert result[5] == CatPunc()
    assert result[6] == PlusPuncA()


def test_starcase_nil():
    """Test starcase on nil takes the nil branch."""
    @Delta.jit
    def f(delta, x: TyStar(STRING_TY), base_case : STRING_TY):
        return delta.starcase(
            x,
            lambda _: base_case,  # Nil branch
            lambda head, tail: head  # Cons branch (not taken)
        )

    output = f(iter([PlusPuncA()]),iter(["Hi"]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == "Hi"


def test_starcase_cons():
    """Test starcase on nil takes the cons branch."""
    @Delta.jit
    def f(delta, x: TyStar(STRING_TY), base_case : STRING_TY):
        return delta.starcase(
            x,
            lambda _: base_case,  # Nil branch
            lambda head, tail: head  # Cons branch (not taken)
        )

    output = f(iter([PlusPuncB(),CatEvA("World"),CatPunc(),PlusPuncA()]),iter(["Hi"]))
    result = [x for x in list(output) if x is not None]
    assert result[0] == "World"


def test_starcase_eta():
    @Delta.jit
    def f(delta, xs: TyStar(INT_TY)):
        def nil_case(_):
            return delta.nil()
        def cons_case(y,ys):
            return delta.cons(y,ys)
        return delta.starcase(xs,nil_case,cons_case)

    xs = [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncA()]

    output = f(iter(xs))
    result = [x for x in list(output) if x is not None]

    assert result == [PlusPuncB(), CatEvA(BaseEvent(1)), CatPunc(), PlusPuncB(), CatEvA(BaseEvent(2)), CatPunc(), PlusPuncA()]
