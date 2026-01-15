import pytest
from yoink.core import *

STRING_TY = Singleton(str)

def test_jit_basic():
    @Yoink.jit
    def simple_cat(yoink, x: STRING_TY, y: STRING_TY):
        return yoink.catr(x, y)


def test_jit_complex():
    @Yoink.jit
    def cat_and_split(yoink, x: STRING_TY, y: STRING_TY):
        z = yoink.catr(x, y)
        a, b = yoink.catl(z)
        return yoink.catr(a, b)

    
def test_jit_nested_types():
    t = TyCat(STRING_TY, STRING_TY)

    @Yoink.jit
    def nested(yoink, z: t):
        a, b = yoink.catl(z)
        return yoink.catr(a, b)


def test_jit_enforces_constraints():
    @Yoink.jit
    def valid_ordering(yoink, x: STRING_TY, y: STRING_TY):
        z = yoink.catr(x, y)
        a, b = yoink.catl(z)
        return yoink.catr(a, b)


def test_jit_rejects_invalid_constraints():
    with pytest.raises(Exception):
        @Yoink.jit
        def invalid_ordering(yoink, x: STRING_TY, y: STRING_TY):
            z = yoink.catr(x, y)
            a, b = yoink.catl(z)
            return yoink.catr(b, a)  # Should fail: b < a conflicts with a < b

def test_jit_multiple_returns():
    @Yoink.jit
    def multiple_outputs(yoink, x: STRING_TY, y: STRING_TY):
        z = yoink.catr(x, y)
        a, b = yoink.catl(z)
        return (a, b)


def test_jit_composition():
    """Test that jitted functions can be composed by calling one inside another."""
    @Yoink.jit
    def inner(yoink, x: STRING_TY, y : STRING_TY):
        return yoink.catr(x, y)

    @Yoink.jit
    def outer(yoink, z: TyCat(STRING_TY,STRING_TY)):
        a, b = yoink.catl(z)
        return inner(yoink,a,b)

    output = outer(iter([CatEvA("hello"), CatPunc (), "world"]))
    result = [x for x in list(output) if x is not None]

    assert result[0] == CatEvA("hello")
    assert result[1] == CatPunc()
    assert result[2] == "world"

# def test_jit_wait():
#     @Yoink.jit
#     def wait(yoink, x: STRING_TY):
#         x = yoink.wait(x)
#         yoink.singleton(x + 1)

