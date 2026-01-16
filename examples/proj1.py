from yoink.core import Yoink , Singleton, TyCat
from yoink.compilation.cps_compiler import CPSCompiler


@Yoink.jit
def proj1(yoink, s : TyCat(Singleton(int),Singleton(int))):
    (a,b) = yoink.catl(s)
    return a

print(proj1.get_code(CPSCompiler))