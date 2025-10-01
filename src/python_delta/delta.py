from python_delta.realized_ordering import RealizedOrdering
from python_delta.compiled_function import CompiledFunction
from python_delta.types import BaseType, TyCat, TyPar, TyPlus, TyStar, TyEps
from python_delta.stream_op import Var, Eps, CatR, CatProj, ParR, ParProj, ParLCoordinator, InL, InR, CaseOp

class Delta:
    def __init__(self):
        self.ordering = RealizedOrdering()
        self.nodes = {}

    def _register_metadata(self, node_id, name):
        self.ordering.metadata[node_id] = name

    def var(self, v, var_type=None):
        if var_type is None:
            var_type = BaseType(v)
        name = f"var_{v}"
        xid = hash(name)
        s = Var(xid, name, var_type)
        self.nodes[xid] = s
        self._register_metadata(xid, name)
        return s

    def eps(self):
        """Create an empty stream that immediately raises StopIteration."""
        name = f"eps_{id(self)}"
        xid = hash(name)
        s = Eps(xid, TyEps)
        self.nodes[xid] = s
        self._register_metadata(xid, name)
        return s

    def catr(self, s1, s2):
        s1id = s1.id
        s2id = s2.id
        zname = f"catr_{s1id}_{s2id}"
        zid = hash(zname)

        if s1.vars.intersection(s2.vars):
            raise ValueError("Illegal CatR, overlapping vars")

        self.ordering.add_all_ordered(s1.vars, s2.vars)

        s = CatR(zid, s1, s2, s1.vars.union(s2.vars), TyCat(s1.stream_type, s2.stream_type))
        self.nodes[zid] = s
        self._register_metadata(zid, zname)
        return s

    def catl(self, s):
        if not isinstance(s.stream_type, TyCat):
            raise TypeError(f"catl requires TyCat type, got {s.stream_type}")
        left_type = s.stream_type.left_type
        right_type = s.stream_type.right_type

        sid = s.id
        lname = f"catproj1_{sid}"
        rname = f"catproj2_{sid}"
        xid = hash(lname)
        yid = hash(rname)

        # x must come before y
        self.ordering.add_ordered(xid, yid)

        # Both projections inherit ordering from s.vars
        self.ordering.add_in_place_of(xid, s.vars)
        self.ordering.add_in_place_of(yid, s.vars)

        # Both projections pull from the same input stream
        x = CatProj(xid, s, left_type, 1)
        y = CatProj(yid, s, right_type, 2)
        self.nodes[xid] = x
        self.nodes[yid] = y
        self._register_metadata(xid, lname)
        self._register_metadata(yid, rname)
        return (x, y)

    def parr(self, s1, s2):
        s1id = s1.id
        s2id = s2.id
        zname = f"parr_{s1id}_{s2id}"
        zid = hash(zname)

        self.ordering.add_all_unordered(s1.vars, s2.vars)

        s = ParR(zid, s1, s2, s1.vars.union(s2.vars), TyPar(s1.stream_type, s2.stream_type))
        self.nodes[zid] = s
        self._register_metadata(zid, zname)
        return s

    def parl(self, s):
        if not isinstance(s.stream_type, TyPar):
            raise TypeError(f"parl requires TyPar type, got {s.stream_type}")
        left_type = s.stream_type.left_type
        right_type = s.stream_type.right_type

        sid = s.id
        coordname = f"parlcoord_{sid}"
        lname = f"parproj1_{sid}"
        rname = f"parproj2_{sid}"
        coordid = hash(coordname)
        xid = hash(lname)
        yid = hash(rname)

        # TODO jcutler: give this two pulls so you don't have to buffer it?
        coord = ParLCoordinator(coordid, s, s.vars, s.stream_type)
        self.nodes[coordid] = coord
        self._register_metadata(coordid, coordname)

        self.ordering.add_unordered(xid, yid)

        self.ordering.add_in_place_of(xid, s.vars)
        self.ordering.add_in_place_of(yid, s.vars)

        x = ParProj(xid, coord, left_type, 1)
        y = ParProj(yid, coord, right_type, 2)
        self.nodes[xid] = x
        self.nodes[yid] = y
        self._register_metadata(xid, lname)
        self._register_metadata(yid, rname)
        return (x, y)

    def inl(self, s):
        """Left injection into sum type."""
        sid = s.id
        zname = f"inl_{sid}"
        zid = hash(zname)

        output_type = TyPlus(s.stream_type, BaseType("unknown"))
        z = InL(zid, s, s.vars, output_type)
        self.nodes[zid] = z
        self.ordering.add_in_place_of(zid, s.vars)
        self._register_metadata(zid, zname)
        return z

    def inr(self, s):
        """Right injection into sum type."""
        sid = s.id
        zname = f"inr_{sid}"
        zid = hash(zname)

        output_type = TyPlus(BaseType("unkown"), s.stream_type)
        z = InR(zid, s, s.vars, output_type)
        self.nodes[zid] = z
        self.ordering.add_in_place_of(zid, s.vars)
        self._register_metadata(zid, zname)
        return z

    def case(self, x, left_fn, right_fn):
        """Case analysis on sum type."""
        if not isinstance(x.stream_type, TyPlus):
            raise TypeError(f"case requires TyPlus type, got {x.stream_type}")

        left_type = x.stream_type.left_type
        right_type = x.stream_type.right_type

        # Create vars for left and right branches with globally unique names
        left_name = f"case_left_{x.id}"
        left_id = hash(left_name)
        right_name = f"case_right_{x.id}"
        right_id = hash(right_name)
        left_var = Var(left_id, left_name, left_type)
        right_var = Var(right_id, right_name, right_type)

        self.ordering.add_in_place_of(left_id, x.vars)
        self.ordering.add_in_place_of(right_id, x.vars)

        self.nodes[left_var.id] = left_var
        self.nodes[right_var.id] = right_var
        self._register_metadata(left_var.id, left_var.name)
        self._register_metadata(right_var.id, right_var.name)

        # Trace both branches with the same delta instance
        left_output = left_fn(left_var)
        right_output = right_fn(right_var)

        # Type check: both branches must return same type
        if left_output.stream_type != right_output.stream_type:
            raise TypeError(f"case branches must return same type, got {left_output.stream_type} and {right_output.stream_type}")

        output_type = left_output.stream_type

        # Create CaseOp
        xid = x.id
        zname = f"case_{xid}"
        zid = hash(zname)

        all_vars = x.vars.union(left_var.vars).union(right_var.vars)
        z = CaseOp(zid, x, left_output, right_output, left_var, right_var, all_vars, output_type)
        self.nodes[zid] = z
        self._register_metadata(zid, zname)

        return z

    def nil(self, element_type):
        """Create empty star (nil) - elaborates to InL(Eps)."""
        eps = self.eps(BaseType("unit"))
        return self.inl(eps)

    def cons(self, head, tail):
        """Cons operation - elaborates to InR(CatR(head, tail))."""
        # Type check: tail must be TyStar
        if not isinstance(tail.stream_type, TyStar):
            raise TypeError(f"cons requires tail to be TyStar type, got {tail.stream_type}")

        element_type = tail.stream_type.element_type

        # Type check: head must match element type
        if head.stream_type != element_type:
            raise TypeError(f"cons head type {head.stream_type} does not match tail element type {element_type}")

        # Check no overlapping vars
        if head.vars.intersection(tail.vars):
            raise ValueError("Illegal cons, overlapping vars between head and tail")

        # Add ordering: all vars in head before all vars in tail
        self.ordering.add_all_ordered(head.vars, tail.vars)

        # Create elaboration: InR(CatR(head, tail))
        cat = self.catr(head, tail)
        return self.inr(cat)

    def starcase(self, x, nil_fn, cons_fn):
        """Star case analysis - elaborates to case + catl."""
        # Type check: x must be TyStar
        if not isinstance(x.stream_type, TyStar):
            raise TypeError(f"starcase requires TyStar type, got {x.stream_type}")

        element_type = x.stream_type.element_type

        # Elaborate to: case(x, nil_fn, lambda z: let (y, ys) = catl(z) in cons_fn(y, ys))
        def right_branch(z):
            # z has type TyCat(element_type, TyStar(element_type))
            y, ys = self.catl(z)
            # y has type element_type, ys has type TyStar(element_type)
            return cons_fn(y, ys)

        return self.case(x, nil_fn, right_branch)

    @staticmethod
    def jit(func):
        """
        Tracing JIT decorator.

        Creates a fresh Delta instance and traces the function with symbolic inputs.
        The traced Delta is passed as the first argument to the function.
        Input types are read from the function's type annotations.

        Returns a CompiledFunction that can be executed by calling it.

        Example:
            @Delta.jit
            def my_func(delta, x: STRING_TY, y: STRING_TY):
                z = delta.catr(x, y)
                a, b = delta.catl(z)
                return delta.catr(a, b)

            # Run with concrete data
            output = my_func(iter([1, 2, 3]), iter([4, 5, 6]))
            for item in output:
                print(item)
        """
        import inspect

        # Get the function signature
        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        # Skip the first parameter (delta), extract types from the rest
        input_types = []
        for param in params[1:]:  # Skip first param which is 'delta'
            if param.annotation == inspect.Parameter.empty:
                raise TypeError(f"Parameter '{param.name}' missing type annotation")
            input_types.append(param.annotation)

        # Create traced delta and symbolic inputs
        traced_delta = Delta()
        input_vars = [traced_delta.var(f"arg{i}", ty) for i, ty in enumerate(input_types)]

        # Trace the function
        outputs = func(traced_delta, *input_vars)

        # Return a compiled function with original function and types for re-tracing
        return CompiledFunction(traced_delta, input_vars, outputs, func, input_types)
