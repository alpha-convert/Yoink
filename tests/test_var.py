import pytest
from yoink.core import *

STRING_TY = Singleton(str)

def test_var():
    yoink = Yoink()
    yoink.var("x")

def test_var_typed():
    yoink = Yoink()
    yoink.var("x",STRING_TY)
