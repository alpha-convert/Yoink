import pytest
from python_delta.core import Delta, BaseType, TyPlus, PlusPuncA, PlusPuncB


STRING_TY = BaseType("string")
INT_TY = BaseType("int")


def test_inl():
    """Test left injection creates PlusPuncA tag."""
    @Delta.jit
    def f(delta, x: STRING_TY):
        return delta.inl(x)

    output = f.run(iter(["a", "b", "c"]))
    result = list(output)

    assert result[0] == PlusPuncA()
    assert result[1:] == ["a", "b", "c"]


def test_inr():
    """Test right injection creates PlusPuncB tag."""
    @Delta.jit
    def f(delta, x: STRING_TY):
        return delta.inr(x)

    output = f.run(iter(["a", "b", "c"]))
    result = list(output)

    assert result[0] == PlusPuncB()
    assert result[1:] == ["a", "b", "c"]


def test_case_left():
    """Test case analysis takes left branch on PlusPuncA."""
    @Delta.jit
    def f(delta, x: TyPlus(STRING_TY, STRING_TY)):
        return delta.case(
            x,
            lambda left: left,   # Left branch: identity
            lambda right: right  # Right branch: identity
        )

    # Create input with PlusPuncA tag
    output = f.run(iter([PlusPuncA(), "a", "b", "c"]))
    result = [x for x in list(output) if x is not None]

    assert result == ["a", "b", "c"]


def test_case_right():
    """Test case analysis takes right branch on PlusPuncB."""
    @Delta.jit
    def f(delta, x: TyPlus(STRING_TY, STRING_TY)):
        return delta.case(
            x,
            lambda left: left,   # Left branch: identity
            lambda right: right  # Right branch: identity
        )

    # Create input with PlusPuncB tag
    output = f.run(iter([PlusPuncB(), 1, 2, 3]))
    result = [x for x in list(output) if x is not None]

    assert result == [1, 2, 3]


def test_case_with_operations():
    """Test case analysis with operations in each branch."""
    @Delta.jit
    def f(delta, x: TyPlus(STRING_TY, STRING_TY), prefix : STRING_TY, suffix : STRING_TY):
        return delta.case(
            x,
            lambda left: delta.catr(left, suffix),
            lambda right: delta.catr(prefix, right)
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
    from python_delta.core import CatEvA, CatPunc
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
