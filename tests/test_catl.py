import pytest
from yoink.core import *

STRING_TY = Singleton(str)

def test_catl():
    yoink = Yoink()
    z = yoink.var("z",TyCat(STRING_TY,STRING_TY))
    (x,y) = yoink.catl(z)

def test_catl_wrong_ty():
    yoink = Yoink()
    z = yoink.var("z",STRING_TY)
    with pytest.raises(Exception):
        (x,y) = yoink.catl(z)

def test_catl_ordered_use():
    yoink = Yoink()
    z = yoink.var("z",TyCat(STRING_TY,STRING_TY))
    (x,y) = yoink.catl(z)
    yoink.catr(x,y)

def test_catl_out_of_order_use():
    yoink = Yoink()
    z = yoink.var("z",TyCat(STRING_TY,STRING_TY))
    (x,y) = yoink.catl(z)
    with pytest.raises(Exception):
        yoink.catr(y,x)

def test_double_catl():
    yoink = Yoink()
    z = yoink.var("z",TyCat(STRING_TY,STRING_TY))
    (x1,y1) = yoink.catl(z)
    (x2,y2) = yoink.catl(z)
    yoink.catr(x1,y1)
    yoink.catr(x2,y2)
    yoink.catr(x1,y2)

def test_double_catl_cross():
    yoink = Yoink()
    z = yoink.var("z",TyCat(STRING_TY,STRING_TY))
    (x1,y1) = yoink.catl(z)
    (x2,y2) = yoink.catl(z)
    with pytest.raises(Exception):
        yoink.catr(y2,x1)

def test_nested_catl():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    # All allowable
    yoink.catr(y,u)
    yoink.catr(y,v)
    yoink.catr(x,u)
    yoink.catr(x,v)

def test_nested_catl_bad1():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    with pytest.raises(Exception):
        yoink.catr(u,y)

def test_nested_catl_bad2():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    with pytest.raises(Exception):
        yoink.catr(u,x)

def test_nested_catl_bad3():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    with pytest.raises(Exception):
        yoink.catr(v,x)

def test_nested_catl_bad4():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    with pytest.raises(Exception):
        yoink.catr(v,y)

def test_nested_catl_bad_nested1():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    s1 = yoink.catr(x,v) #allowed
    s2 = yoink.catr(y,u) #allowed
    with pytest.raises(Exception):
        yoink.catr(s1,s2)

def test_nested_catl_bad_nested2():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    s1 = yoink.catr(x,v) #allowed
    s2 = yoink.catr(y,u) #allowed
    with pytest.raises(Exception):
        yoink.catr(s2,s1)
    
def test_nested_catl_bad_nested3():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    s1 = yoink.catr(x,u) #allowed
    s2 = yoink.catr(y,v) #allowed
    with pytest.raises(Exception):
        yoink.catr(s1,s2)

def test_nested_catl_bad_nested4():
    yoink = Yoink()
    t = TyCat(STRING_TY,STRING_TY)
    z = yoink.var("z",TyCat(t,t))
    (z1,z2) = yoink.catl(z)
    (x,y) = yoink.catl(z1)
    (u,v) = yoink.catl(z2)
    s1 = yoink.catr(x,u) #allowed
    s2 = yoink.catr(y,v) #allowed
    with pytest.raises(Exception):
        yoink.catr(s2,s1)

def test_valid_ordering():
    yoink = Yoink()
    x = yoink.var("x",STRING_TY)
    y = yoink.var("y",STRING_TY)
    z = yoink.catr(x, y)
    a, b = yoink.catl(z)
    yoink.catr(a, b)