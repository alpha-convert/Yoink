import pytest
from python_delta.core import *

STRING_TY = BaseType("String")

def test_parr():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.parr(x,y)

def test_parr_overlap_allowed():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    delta.parr(x,x)

def test_par_disallows_prev_catr1():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.catr(x,y)
    with pytest.raises(Exception):
        delta.parr(x,y)

def test_par_disallows_prev_catr2():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.catr(y,x)
    with pytest.raises(Exception):
        delta.parr(x,y)

def test_par_disallows_fut_catr1():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.parr(x,y)
    with pytest.raises(Exception):
        delta.catr(x,y)

def test_par_disallows_fut_catr2():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    delta.parr(y,x)
    with pytest.raises(Exception):
        delta.catr(x,y)

def test_nested_par():
    delta = Delta()
    x = delta.var("x",STRING_TY)
    y = delta.var("y",STRING_TY)
    z = delta.var("y",STRING_TY)
    delta.parr(x,delta.parr(y,z))