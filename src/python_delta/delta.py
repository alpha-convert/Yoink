from python_delta.realized_ordering import RealizedOrdering
from python_delta.dataflow_graph import DataflowGraph
from python_delta.types import Type, BaseType, TyCat, TyPar, TyPlus, TyStar, TyEps, TypeVar
from python_delta.stream_op import StreamOp, Var, Eps, CatR, CatProj, ParR, ParProj, ParLCoordinator, SumInj, CaseOp, RecCall, UnsafeCast

class Delta:
    def __init__(self):
        self.ordering = RealizedOrdering()
        self.nodes = set()
        self.current_level = 1

    def _fresh_type_var(self):
        return TypeVar(self.current_level)

    def _register_node(self, node):
        self.nodes.add(node)

    def var(self, v, var_type=None):
        if var_type is None:
            var_type = self._fresh_type_var()
        s = Var(v, var_type)
        self._register_node(s)
        return s

    def eps(self):
        """Create an empty stream that immediately raises StopIteration."""
        s = Eps(TyEps())
        self._register_node(s)
        return s

    def catr(self, s1, s2):
        if s1.vars.intersection(s2.vars):
            raise ValueError("Illegal CatR, overlapping vars")

        self.ordering.add_all_ordered(s1.vars, s2.vars)

        s = CatR(s1, s2, TyCat(s1.stream_type, s2.stream_type))
        self._register_node(s)
        return s

    def catl(self, s):
        left_type = self._fresh_type_var()
        right_type = self._fresh_type_var()
        s.stream_type.unify_with(TyCat(left_type,right_type))

        x = CatProj(s, left_type, 1)
        y = CatProj(s, right_type, 2)

        # x must come before y
        self.ordering.add_ordered(x.id, y.id)

        # Both projections inherit ordering from s.vars
        self.ordering.add_in_place_of(x.id, s.vars)
        self.ordering.add_in_place_of(y.id, s.vars)

        self._register_node(x)
        self._register_node(y)
        return (x, y)

    def parr(self, s1, s2):
        self.ordering.add_all_unordered(s1.vars, s2.vars)

        s = ParR(s1, s2, TyPar(s1.stream_type, s2.stream_type))
        self._register_node(s)
        return s

    def parl(self, s):
        left_type = self._fresh_type_var()
        right_type = self._fresh_type_var()
        s.stream_type.unify_with(TyPar(left_type,right_type))

        coord = ParLCoordinator(s, s.stream_type)
        self._register_node(coord)

        x = ParProj(coord, left_type, 1)
        y = ParProj(coord, right_type, 2)

        self.ordering.add_unordered(x.id, y.id)

        self.ordering.add_in_place_of(x.id, s.vars)
        self.ordering.add_in_place_of(y.id, s.vars)

        self._register_node(x)
        self._register_node(y)
        return (x, y)

    def inl(self, s):
        """Left injection into sum type."""
        right_type = self._fresh_type_var()
        output_type = TyPlus(s.stream_type, right_type)
        z = SumInj(s, output_type, position=0)
        self.ordering.add_in_place_of(z.id, s.vars)
        self._register_node(z)
        return z

    def inr(self, s):
        """Right injection into sum type."""
        left_type = self._fresh_type_var()
        output_type = TyPlus(left_type, s.stream_type)
        z = SumInj(s, output_type, position=1)
        self.ordering.add_in_place_of(z.id, s.vars)
        self._register_node(z)
        return z

    def case(self, x, left_fn, right_fn):
        left_type = self._fresh_type_var()
        right_type = self._fresh_type_var()
        x.stream_type.unify_with(TyPlus(left_type,right_type))

        # Why is this safe/correct?
        # By the time we're pulling on these, the remainder of x will either be of
        # the left type or the right type! The initial punctuation will have passed, sending us down
        # the correct path.
        x_left = UnsafeCast(x,left_type)
        x_right = UnsafeCast(x,right_type)

        left_output = left_fn(x_left)
        right_output = right_fn(x_right)

        # Type check: both branches must return same type
        left_output.stream_type.unify_with(right_output.stream_type)

        output_type = left_output.stream_type

        z = CaseOp(x, left_output, right_output, output_type)
        self._register_node(z)

        return z

    def nil(self, element_type = None):
        if element_type is None:
            element_type = self._fresh_type_var()
        eps = Eps(TyEps())
        s = SumInj(eps,TyStar(element_type),position=0)
        self._register_node(s)
        return s

    def cons(self, head, tail):
        """Cons operation - builds InR(CatR(head, tail)) directly."""
        element_type = self._fresh_type_var()
        star_type = TyStar(element_type)
        head.stream_type.unify_with(element_type)
        tail.stream_type.unify_with(star_type)

        if head.vars.intersection(tail.vars):
            raise ValueError("Illegal cons, overlapping vars between head and tail")

        self.ordering.add_all_ordered(head.vars, tail.vars)

        cat = CatR(head, tail, TyCat(element_type, star_type))
        s = SumInj(cat, star_type, position=1)
        self._register_node(s)
        return s

    def starcase(self, x, nil_fn, cons_fn):
        """Star case analysis - builds CaseOp directly for TyStar."""
        element_type = self._fresh_type_var()
        star_type = TyStar(element_type)

        x.stream_type.unify_with(star_type)
        # print(x.stream_type)
        # print(star_type)

        x_nil = UnsafeCast(x,TyEps())
        x_cons = UnsafeCast(x,TyCat(element_type, star_type))

        head = CatProj(x_cons, element_type, 1)
        tail = CatProj(x_cons, star_type, 2)

        self.ordering.add_ordered(head.id, tail.id)

        self.ordering.add_in_place_of(head.id, x.vars)
        self.ordering.add_in_place_of(tail.id, x.vars)

        self._register_node(head)
        self._register_node(tail)

        nil_output : StreamOp = nil_fn(x_nil)
        cons_output : StreamOp = cons_fn(head, tail)

        nil_output.stream_type.unify_with(other=cons_output.stream_type)

        output_type = nil_output.stream_type

        z = CaseOp(x, nil_output, cons_output, output_type)
        self._register_node(z)

        return z
    
   
    @staticmethod
    def jit(func):
        """
        Tracing JIT decorator.

        Creates a fresh Delta instance and traces the function with symbolic inputs.
        The traced Delta is passed as the first argument to the function.
        Input types are read from the function's type annotations.

        Returns a DataflowGraph that can be executed by calling it.

        Example:
            @Delta.jit
            def my_func(delta, x: STRING_TY, y: STRING_TY):
                z = delta.catr(x, y)
                a, b = delta.catl(z)
                return delta.catr(a, b)

            # Run with concrete data
            output = my_func(iter([1, 2, 3]), iter([4, 5, 6]))
        """
        import inspect

        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        # TODO jcutler: type inference and generalization!
        # We should do the following:
        # (1) check the output against the written output type (if one exists)
        # (2) allow for function types to be *not written*, and use type variables if there were none
        # (3) record the inferred type, and generalize as appropreate
        # (4) Ensure that we correctly typecheck at function call sites!
        # (5) Let people write down ordered contexts as the initial realized
        #     ordering --- ensure we check against these at call sites!
        # (6) Reify the existing partial order into a context??? The least FJ poset consistent with the realized ordering?

        input_types = []
        for param in params[1:]:  # Skip first param which is 'delta'
            if param.annotation == inspect.Parameter.empty:
                raise TypeError(f"Parameter '{param.name}' missing type annotation")
            input_types.append(param.annotation)
        
        # Create traced delta and symbolic inputs
        traced_delta = Delta()
        input_vars = [traced_delta.var(f"arg{i}", ty) for i, ty in enumerate(input_types)]

        graph = DataflowGraph(traced_delta, input_vars, None, func, input_vars)

        return_type = traced_delta._fresh_type_var()

        rechandle = RecHandle(input_types, return_type, traced_delta, graph)
        for i, name in enumerate(func.__code__.co_freevars):
            if name == func.__name__:
                func.__closure__[i].cell_contents = rechandle
                break
        func.__globals__[func.__name__] = rechandle
        outputs = func(traced_delta, *input_vars)

        graph.outputs = outputs

        return graph

class RecHandle():
    def __init__(self, input_types, return_type, delta, dataflow_graph):
        self.input_types = input_types
        self.return_type = return_type
        self.delta = delta
        self.dataflow_graph = dataflow_graph

    def __call__(self,*args):
        if len(args) != len(self.input_types):
            raise "FIXME"
        
        for arg,type in zip(args,self.input_types):
            arg.stream_type.unify_with(type)
        
        s = RecCall(self.dataflow_graph, args, self.return_type)
        self.delta._register_node(s)
        return s
    