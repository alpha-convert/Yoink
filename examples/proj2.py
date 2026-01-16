from yoink.core import Yoink , Singleton, TyCat
from yoink.compilation.cps_compiler import CPSCompiler


@Yoink.jit
def proj2(yoink, s : TyCat(Singleton(int),Singleton(int))):
    (a,b) = yoink.catl(s)
    return b

print(proj2.get_code(CPSCompiler))