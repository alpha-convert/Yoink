import pytest
from yoink.core import Yoink, Singleton, TyPlus, PlusPuncA, PlusPuncB, TyCat,BaseEvent, CatEvA, TyEps, CatPunc


STRING_TY = Singleton(str)
INT_TY = Singleton(int)


def test_inl():
    """Test left injection creates PlusPuncA tag."""
    @Yoink.jit
    def f(yoink, x: STRING_TY):
        return yoink.inl(x)

    output = f.run(iter(["a", "b", "c"]))
    result = list(output)

    assert result[0] == PlusPuncA()
    assert result[1:] == ["a", "b", "c"]


def test_inr():
    """Test right injection creates PlusPuncB tag."""
    @Yoink.jit
    def f(yoink, x: STRING_TY):
        return yoink.inr(x)

    output = f.run(iter(["a", "b", "c"]))
    result = list(output)

    assert result[0] == PlusPuncB()
    assert result[1:] == ["a", "b", "c"]


def test_case_left():
    """Test case analysis takes left branch on PlusPuncA."""
    @Yoink.jit
    def f(yoink, x: TyPlus(STRING_TY, STRING_TY)):
        return yoink.case(
            x,
            lambda left: left,
            lambda right: right
        )

    output = f.run(iter([PlusPuncA(), "a", "b", "c"]))
    result = [x for x in list(output) if x is not None]

    assert result == ["a", "b", "c"]


def test_case_right():
    """Test case analysis takes right branch on PlusPuncB."""
    @Yoink.jit
    def f(yoink, x: TyPlus(STRING_TY, STRING_TY)):
        return yoink.case(
            x,
            lambda left: left,   
            lambda right: right  
        )

    output = f.run(iter([PlusPuncB(), 1, 2, 3]))
    result = [x for x in list(output) if x is not None]

    assert result == [1, 2, 3]


def test_case_with_operations():
    """Test case analysis with operations in each branch."""
    @Yoink.jit
    def f(yoink, x: TyPlus(STRING_TY, STRING_TY), prefix : STRING_TY, suffix : STRING_TY):
        return yoink.case(
            x,
            lambda left: yoink.catr(left, suffix),
            lambda right: yoink.catr(prefix, right)
        )

    # Test left branch
    output_left = f.run(
        iter([PlusPuncA(), "a", "b"]),
        iter(["z", "w"]),
        iter(["x", "y"])
    )
    result_left = []
    for item in output_left:
        result_left.append(item)
    from yoink.core import CatEvA, CatPunc
    assert result_left[0] == None
    assert result_left[1] == CatEvA("a")
    assert result_left[2] == CatEvA("b")
    assert result_left[3] == CatPunc()
    assert result_left[4:] == ["x", "y"]

    # Test left branch
    output_right = f.run(
        iter([PlusPuncB(), "a", "b"]),
        iter(["z", "w"]),
        iter(["x", "y"])
    )
    result_left = list(output_left)
    assert result_left[0] == None
    assert result_left[1] == CatEvA("z")
    assert result_left[2] == CatEvA("w")
    assert result_left[3] == CatPunc()
    assert result_left[4:] == ["a", "b"]

def test_case_ordering_check():
    """Test case analysis takes left branch on PlusPuncA."""
    with pytest.raises(Exception):
        @Yoink.jit
        def f(yoink, x: TyCat(STRING_TY,TyPlus(STRING_TY,STRING_TY))):
            u,v = yoink.catl(x)
            return yoink.case(
                v,
                lambda left: u,
                lambda right: u 
            )



def test_case_eta():
    """Test case analysis with operations in each branch."""
    @Yoink.jit
    def f(yoink, x: TyPlus(STRING_TY, STRING_TY)):
        return yoink.case(
            x,
            lambda left: yoink.inl(left),
            lambda right: yoink.inr(right)
        )

    # Test left branch
    input_left = [PlusPuncA(), BaseEvent("a")]
    output_left = f(iter(input_left))
    result = [x for x in list(output_left) if x is not None]
    assert result == input_left

    # Test right branch
    input_right = [PlusPuncB(), BaseEvent("a")]
    output_right = f(iter(input_right))
    result = [x for x in list(output_right) if x is not None]
    assert result == input_right


# Emulate starcase with pluscase
def test_case_eta_staremu():
    @Yoink.jit
    def f(yoink, x: TyPlus(TyEps(), TyCat(STRING_TY,STRING_TY))):
        def body(right):
            l,r = yoink.catl(right)
            return yoink.inr(yoink.catr(l,r))
        return yoink.case(
            x,
            lambda left: yoink.inl(left),
            body
        )

    input_right = [PlusPuncB(), CatEvA(BaseEvent("a")), CatPunc(), BaseEvent("b"), PlusPuncA()]
    output_right = f(iter(input_right))
    result = [x for x in list(output_right) if x is not None]
    assert result == input_right