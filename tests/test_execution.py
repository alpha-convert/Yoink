"""
Tests for stream execution semantics.
"""
from python_delta.core import Delta, BaseType, CatEvA, CatPunc

STRING_TY = BaseType("String")

def test_simple_catr():
    """Test basic concatenation execution."""
    @Delta.jit
    def concat(delta, x: STRING_TY, y: STRING_TY):
        return delta.catr(x, y)

    # Run with concrete data
    output = concat.run(iter(['a', 'b']), iter(['c', 'd']))

    # Collect results
    results = list(output)

    # Should be: CatEvA('a'), CatEvA('b'), CatPunc, 'c', 'd' (unwrapped after punc)
    assert len(results) == 5
    assert results[0] == CatEvA('a')
    assert results[1] == CatEvA('b')
    assert results[2] == CatPunc()
    assert results[3] == 'c'  # Unwrapped
    assert results[4] == 'd'  # Unwrapped
    print("✓ test_simple_catr passed")

def test_catl_projection():
    """Test catl projection execution."""
    @Delta.jit
    def split_concat(delta, x: STRING_TY, y: STRING_TY):
        z = delta.catr(x, y)
        a, b = delta.catl(z)
        return (a,b)

    # Run with concrete data
    a,b = split_concat.run(iter([1, 2, 3]), iter([4, 5, 6]))

    # Collect results from each projection, filtering out None values
    a_results = [x for x in a if x is not None]
    b_results = [x for x in b if x is not None]

    assert a_results == [1, 2, 3]
    assert b_results == [4, 5, 6]
    print("✓ test_catl_projection passed")

if __name__ == "__main__":
    test_simple_catr()
    test_catl_projection()
    print("n✅ All execution tests passed!")
