
from yoink.core import Yoink , Singleton, TyCat, TyStar
from yoink.compilation.cps_compiler import CPSCompiler

def map(yoink,x,map_fn,result_type):
        def build_body(rec):
            def map_cons_case(x_head,x_tail):
                map_output = map_fn(x_head)
                return yoink.cons(map_output,rec)

            return yoink.starcase(x,lambda _ : yoink.nil(), map_cons_case)

        return yoink.fix(build_body,result_type)

ints = TyStar(Singleton(int))

@Yoink.jit
def map_add_one(yoink, s : ints):
    def add_one(x):
        y = yoink.wait(x)
        return yoink.emit(y + 1)
    return map(yoink,s,add_one,ints)

print(map_add_one.get_code(CPSCompiler))