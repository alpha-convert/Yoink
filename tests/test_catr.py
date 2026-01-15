import pytest
from yoink.core import *

STRING_TY = Singleton(str)

def test_catr():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    yoink.catr(x,y)

def test_catr_disj():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    with pytest.raises(Exception):
        yoink.catr(x,x)

def test_catr_consistency():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    yoink.catr(x,y)
    with pytest.raises(Exception):
        yoink.catr(y,x)

def test_nested_catr_1():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.var("z",STRING_TY)
    yoink.catr(x,yoink.catr(y,z))

def test_nested_catr_2():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.var("z",STRING_TY)
    yoink.catr(yoink.catr(y,z),x)

def forked_cat1():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.var("z",STRING_TY)
    s1 = yoink.catr(x,y)
    s2 = yoink.catr(x,z)
    with pytest.raises(Exception):
        yoink.catr(s1,s2)

def test_bad_triangle():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.var("z",STRING_TY)
    s1 = yoink.catr(x,y)
    s2 = yoink.catr(y,z)
    with pytest.raises(Exception):
        yoink.catr(z,z)

def test_good_square():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.var("z",STRING_TY)
    w = yoink.var("w",STRING_TY)
    s1 = yoink.catr(x,y)
    s2 = yoink.catr(z,w)
    yoink.catr(s1,s2)

def test_bad_square():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.var("z",STRING_TY)
    w = yoink.var("w",STRING_TY)
    yoink.catr(x,y)
    yoink.catr(y,z)
    yoink.catr(z,w)
    with pytest.raises(Exception):
        yoink.catr(w,x)