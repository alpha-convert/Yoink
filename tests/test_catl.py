import pytest
from python_delta.core import *

STRING_TY = BaseType("String")

def test_catl():
    delta = Delta()
    z = delta.var("z",TyCat(STRING_TY,STRING_TY))
    (x,y) = delta.catl(z)

def test_catl_wrong_ty():
    delta = Delta()
    z = delta.var("z",STRING_TY)
    with pytest.raises(Exception):
        (x,y) = delta.catl(z)

def test_catl_ordered_use():
    delta = Delta()
    z = delta.var("z",TyCat(STRING_TY,STRING_TY))
    (x,y) = delta.catl(z)
    delta.catr(x,y)

def test_catl_out_of_order_use():
    delta = Delta()
    z = delta.var("z",TyCat(STRING_TY,STRING_TY))
    (x,y) = delta.catl(z)
    with pytest.raises(Exception):
        delta.catr(y,x)

def test_double_catl():
    delta = Delta()
    z = delta.var("z",TyCat(STRING_TY,STRING_TY))
    (x1,y1) = delta.catl(z)
    (x2,y2) = delta.catl(z)
    delta.catr(x1,y1)
    delta.catr(x2,y2)
    delta.catr(x1,y2)

def test_double_catl_cross():
    delta = Delta()
    z = delta.var("z",TyCat(STRING_TY,STRING_TY))
    (x1,y1) = delta.catl(z)
    (x2,y2) = delta.catl(z)
    with pytest.raises(Exception):
        delta.catr(y2,x1)

def test_nested_catl():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    # All allowable
    delta.catr(y,u)
    delta.catr(y,v)
    delta.catr(x,u)
    delta.catr(x,v)

def test_nested_catl_bad1():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    with pytest.raises(Exception):
        delta.catr(u,y)

def test_nested_catl_bad2():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    with pytest.raises(Exception):
        delta.catr(u,x)

def test_nested_catl_bad3():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    with pytest.raises(Exception):
        delta.catr(v,x)

def test_nested_catl_bad4():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    with pytest.raises(Exception):
        delta.catr(v,y)

def test_nested_catl_bad_nested1():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    s1 = delta.catr(x,v) #allowed
    s2 = delta.catr(y,u) #allowed
    with pytest.raises(Exception):
        delta.catr(s1,s2)

def test_nested_catl_bad_nested2():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    s1 = delta.catr(x,v) #allowed
    s2 = delta.catr(y,u) #allowed
    with pytest.raises(Exception):
        delta.catr(s2,s1)
    
def test_nested_catl_bad_nested3():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    s1 = delta.catr(x,u) #allowed
    s2 = delta.catr(y,v) #allowed
    with pytest.raises(Exception):
        delta.catr(s1,s2)

def test_nested_catl_bad_nested4():
    delta = Delta()
    t = TyCat(STRING_TY,STRING_TY)
    z = delta.var("z",TyCat(t,t))
    (z1,z2) = delta.catl(z)
    (x,y) = delta.catl(z1)
    (u,v) = delta.catl(z2)
    s1 = delta.catr(x,u) #allowed
    s2 = delta.catr(y,v) #allowed
    with pytest.raises(Exception):
        delta.catr(s2,s1)

def test_valid_ordering():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.catr(x, y)
    a, b = delta.catl(z)
    delta.catr(a, b)