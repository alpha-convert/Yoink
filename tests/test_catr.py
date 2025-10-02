import pytest
from python_delta.core import *

STRING_TY = BaseType(str)

def test_catr():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.catr(x,y)

def test_catr_disj():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    with pytest.raises(Exception):
        delta.catr(x,x)

def test_catr_consistency():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.catr(x,y)
    with pytest.raises(Exception):
        delta.catr(y,x)

def test_nested_catr_1():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("z",STRING_TY)
    delta.catr(x,delta.catr(y,z))

def test_nested_catr_2():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("z",STRING_TY)
    delta.catr(delta.catr(y,z),x)

def forked_cat1():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("z",STRING_TY)
    s1 = delta.catr(x,y)
    s2 = delta.catr(x,z)
    with pytest.raises(Exception):
        delta.catr(s1,s2)

def test_bad_triangle():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("z",STRING_TY)
    s1 = delta.catr(x,y)
    s2 = delta.catr(y,z)
    with pytest.raises(Exception):
        delta.catr(z,z)

def test_good_square():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("z",STRING_TY)
    w = delta.var("w",STRING_TY)
    s1 = delta.catr(x,y)
    s2 = delta.catr(z,w)
    delta.catr(s1,s2)

def test_bad_square():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("z",STRING_TY)
    w = delta.var("w",STRING_TY)
    delta.catr(x,y)
    delta.catr(y,z)
    delta.catr(z,w)
    with pytest.raises(Exception):
        delta.catr(w,x)