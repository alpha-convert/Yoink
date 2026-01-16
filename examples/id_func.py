from yoink.core import Yoink , Singleton, TyStar, PlusPuncA, PlusPuncB, CatEvA, CatPunc
from yoink.compilation.cps_compiler import CPSCompiler


@Yoink.jit
def id(yoink, s : TyStar(Singleton(int))):
    return s

print(id.get_code(CPSCompiler))