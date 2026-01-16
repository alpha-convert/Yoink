
from yoink.core import Yoink , Singleton, TyCat, TyStar
from yoink.compilation.cps_compiler import CPSCompiler

intss = TyStar(TyStar(Singleton(int)))

@Yoink.jit
def concatmap_adds(yoink, s : intss):
    def add_one(x):
        y = yoink.wait(x)
        return yoink.emit(y+1)
    def map_add_one(xs):
        return yoink.map(xs,add_one)
    return yoink.concat_map(s,map_add_one)

print(concatmap_adds.get_code(CPSCompiler))