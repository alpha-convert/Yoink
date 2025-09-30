from python_delta.realized_ordering import RealizedOrdering
from python_delta.types import BaseType, TyCat, TyPar
from python_delta.stream_op import Var, CatR, CatProj, ParR, ParProj, ParLCoordinator


class CompiledFunction:
    """
    A compiled stream function that can be executed with concrete iterators.
    """
    def __init__(self, traced_delta, input_vars, outputs):
        """
        Args:
            traced_delta: The Delta instance containing the traced computation graph
            input_vars: List of Var nodes representing function inputs
            outputs: The output StreamOp(s) from the traced function
        """
        self.traced_delta = traced_delta
        self.input_vars = input_vars
        self.outputs = outputs

    def run(self, *iterators):
        """
        Execute the compiled function with concrete iterators.

        Args:
            *iterators: Concrete iterators to bind to input variables

        Returns:
            The output stream(s), ready to be iterated
        """
        if len(iterators) != len(self.input_vars):
            raise ValueError(f"Expected {len(self.input_vars)} iterators, got {len(iterators)}")

        # Reset all nodes to initial state
        for node in self.traced_delta.nodes.values():
            node.reset()

        # Bind concrete iterators to Var sources
        for var, iterator in zip(self.input_vars, iterators):
            var.source = iterator

        # Return the output stream(s)
        return self.outputs

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

        # Create coordinator that manages buffering between projections
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

    @staticmethod
    def jit(func):
        """
        Tracing JIT decorator.

        Creates a fresh Delta instance and traces the function with symbolic inputs.
        The traced Delta is passed as the first argument to the function.
        Input types are read from the function's type annotations.

        Returns a CompiledFunction that can be executed with .run(*iterators).

        Example:
            @Delta.jit
            def my_func(delta, x: STRING_TY, y: STRING_TY):
                z = delta.catr(x, y)
                a, b = delta.catl(z)
                return delta.catr(a, b)

            # Run with concrete data
            output = my_func.run(iter([1, 2, 3]), iter([4, 5, 6]))
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

        # Return a compiled function
        return CompiledFunction(traced_delta, input_vars, outputs)
