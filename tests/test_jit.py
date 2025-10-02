import pytest
from python_delta.core import *

STRING_TY = BaseType(str)

def test_jit_basic():
    @Delta.jit
    def simple_cat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)


def test_jit_complex():
    @Delta.jit
    def cat_and_split(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return delta.catr(a, b)

    
def test_jit_nested_types():
    t = TyCat(STRING_TY, STRING_TY)

    @Delta.jit
    def nested(delta, z: t):
        a, b = delta.catl(z)
        return delta.catr(a, b)


def test_jit_enforces_constraints():
    @Delta.jit
    def valid_ordering(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return delta.catr(a, b)


def test_jit_rejects_invalid_constraints():
    with pytest.raises(Exception):
        @Delta.jit
        def invalid_ordering(delta, x: STRING_TY, y: STRING_TY):
            z = delta.catr(x, y)
            a, b = delta.catl(z)
            return delta.catr(b, a)  # Should fail: b < a conflicts with a < b

def test_jit_multiple_returns():
    @Delta.jit
    def multiple_outputs(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return (a, b)


def test_jit_composition():
    """Test that jitted functions can be composed by calling one inside another."""
    @Delta.jit
    def inner(delta, x: STRING_TY, y : STRING_TY):
        return delta.catr(x, y)

    @Delta.jit
    def outer(delta, z: TyCat(STRING_TY,STRING_TY)):
        a, b = delta.catl(z)
        return inner(delta,a,b)

    output = outer(iter([CatEvA("hello"), CatPunc (), "world"]))
    result = [x for x in list(output) if x is not None]

    assert result[0] == CatEvA("hello")
    assert result[1] == CatPunc()
    assert result[2] == "world"