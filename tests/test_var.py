import pytest
from python_delta.core import *

STRING_TY = Singleton(str)

def test_var():
    delta = Delta()
    delta.var("x")

def test_var_typed():
    delta = Delta()
    delta.var("x",STRING_TY)
