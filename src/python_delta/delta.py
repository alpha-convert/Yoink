from python_delta.realized_ordering import RealizedOrdering
from python_delta.types import BaseType, TyCat, TyPar
from python_delta.stream import Stream

class Delta:
    def __init__(self):
        self.ordering = RealizedOrdering()
        self.nodes = {}

    # TODO: refactoring requried here. We need to simplify this to have
    # "register x <= y", and "x in place of s", which puts in the implied requried edges
    # between x and vars(s)

    def _register_metadata(self, node_id, name):
        self.ordering.metadata[node_id] = name

    def var(self, v, var_type=None):
        if var_type is None:
            var_type = BaseType(v)
        name = f"var_{v}"
        xid = hash(name)
        s = Stream(xid, name, [], {xid}, var_type)
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

        s = Stream(zid, zname, [s1id, s2id], s1.vars.union(s2.vars), TyCat(s1.stream_type, s2.stream_type))
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

        x = Stream(xid, "catproj1", [sid], {xid}, left_type)
        y = Stream(yid, "catproj2", [sid], {yid}, right_type)
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

        s = Stream(zid, zname, [s1id, s2id], s1.vars.union(s2.vars), TyPar(s1.stream_type, s2.stream_type))
        self.nodes[zid] = s
        self._register_metadata(zid, zname)
        return s

    def parl(self, s):
        if not isinstance(s.stream_type, TyPar):
            raise TypeError(f"parl requires TyPar type, got {s.stream_type}")
        left_type = s.stream_type.left_type
        right_type = s.stream_type.right_type

        sid = s.id
        lname = f"parproj1_{sid}"
        rname = f"parproj2_{sid}"
        xid = hash(lname)
        yid = hash(rname)

        self.ordering.add_unordered(xid, yid)

        self.ordering.add_in_place_of(xid, s.vars)
        self.ordering.add_in_place_of(yid, s.vars)

        x = Stream(xid, "parproj1", [sid], {xid}, left_type)
        y = Stream(yid, "parproj2", [sid], {yid}, right_type)
        self.nodes[xid] = x
        self.nodes[yid] = y
        self._register_metadata(xid, lname)
        self._register_metadata(yid, rname)
        return (x, y)

    @staticmethod
    def jit(func):
        """
        JAX-style tracing JIT decorator.

        Creates a fresh Delta instance and traces the function with symbolic inputs.
        The traced Delta is passed as the first argument to the function.
        Input types are read from the function's type annotations.

        Example:
            @Delta.jit
            def my_func(delta, x: STRING_TY, y: STRING_TY):
                z = delta.catr(x, y)
                a, b = delta.catl(z)
                return delta.catr(a, b)
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
        inputs = [traced_delta.var(f"arg{i}", ty) for i, ty in enumerate(input_types)]

        # Trace the function
        outputs = func(traced_delta, *inputs)
        return outputs
