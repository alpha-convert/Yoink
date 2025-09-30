import pytest
from python_delta.core import *

STRING_TY = BaseType("String")

def test_jit_basic():
    @Delta.jit
    def simple_cat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)

    result = simple_cat
    assert isinstance(result, Stream)
    assert isinstance(result.stream_type, TyCat)
    assert result.stream_type.left_type == STRING_TY
    assert result.stream_type.right_type == STRING_TY

def test_jit_complex():
    @Delta.jit
    def cat_and_split(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return delta.catr(a, b)

    result = cat_and_split
    assert isinstance(result, Stream)
    assert isinstance(result.stream_type, TyCat)
    assert result.stream_type.left_type == STRING_TY
    assert result.stream_type.right_type == STRING_TY

def test_jit_nested_types():
    t = TyCat(STRING_TY, STRING_TY)

    @Delta.jit
    def nested(delta, z: t):
        a, b = delta.catl(z)
        return delta.catr(a, b)

    result = nested
    assert isinstance(result, Stream)
    assert isinstance(result.stream_type, TyCat)

def test_jit_enforces_constraints():
    @Delta.jit
    def valid_ordering(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return delta.catr(a, b)

    result = valid_ordering
    assert isinstance(result, Stream)

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

    result = multiple_outputs
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], Stream)
    assert isinstance(result[1], Stream)

def test_jit_single_var():
    @Delta.jit
    def identity(delta, x: STRING_TY):
        return x

    result = identity
    assert isinstance(result, Stream)
    assert result.stream_type == STRING_TY

def test_jit_missing_annotation():
    with pytest.raises(TypeError):
        @Delta.jit
        def no_annotation(delta, x):
            return x