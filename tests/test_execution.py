"""
Tests for stream execution semantics.
"""
import random
from python_delta.core import Delta, Singleton, CatEvA, CatPunc, TyCat

STRING_TY = Singleton(str)

def test_simple_catr():
    """Test basic concatenation execution."""
    @Delta.jit
    def concat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)

    output = concat(iter(['a', 'b']), iter(['c', 'd']))
    results = list(output)
    assert len(results) == 5
    assert results[0] == CatEvA('a')
    assert results[1] == CatEvA('b')
    assert results[2] == CatPunc()
    assert results[3] == 'c'
    assert results[4] == 'd'

def test_catl_projection():
    """Test catl projection execution."""
    @Delta.jit
    def split_concat(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return (a,b)

    a,b = split_concat(iter([1, 2, 3]), iter([4, 5, 6]))

    a_results = [x for x in a if x is not None]
    b_results = [x for x in b if x is not None]

    assert a_results == [1, 2, 3]
    assert b_results == [4, 5, 6]

def test_nested_catr():
    """Test nested concatenation: catr(x, catr(y, z))."""
    @Delta.jit
    def nested(delta, x: STRING_TY, y: STRING_TY, z: STRING_TY):
        return delta.catr(x, delta.catr(y, z))

    output = nested(iter([1]), iter([2]), iter([3]))
    results = list(output)

    assert len(results) == 5
    assert results[0] == CatEvA(1)
    assert results[1] == CatPunc()
    assert results[2] == CatEvA(2)
    assert results[3] == CatPunc()
    assert results[4] == 3

def test_catl():
    t = TyCat(STRING_TY, STRING_TY)
    """Test catr followed by catl (round-trip)."""
    @Delta.jit
    def roundtrip(delta, z : t):
        a, b = delta.catl(z)
        return (a, b)

    a, b = roundtrip(iter([CatEvA(1), CatEvA(2), CatPunc(),3,4]))
    a_results = [x for x in a if x is not None]
    b_results = [x for x in b if x is not None]

    assert a_results == [1, 2]
    assert b_results == [3, 4]