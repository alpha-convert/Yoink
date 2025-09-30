from python_delta.realized_ordering import RealizedOrdering
from python_delta.types import BaseType, TyCat, TyPar, TyPlus
from python_delta.stream_op import Var, Eps, CatR, CatProj, ParR, ParProj, ParLCoordinator, InL, InR, CaseOp


class CompiledFunction:
    """
    A compiled stream function that can be executed with concrete iterators
    or composed with other traced functions.
    """
    def __init__(self, traced_delta, input_vars, outputs, original_func, input_types):
        """
        Args:
            traced_delta: The Delta instance containing the traced computation graph
            input_vars: List of Var nodes representing function inputs
            outputs: The output StreamOp(s) from the traced function
            original_func: The original untraced function
            input_types: List of input types for the function
        """
        self.traced_delta = traced_delta
        self.input_vars = input_vars
        self.outputs = outputs
        self.original_func = original_func
        self.input_types = input_types
        self._tracing = False  # Flag to detect recursive calls

    def __call__(self, *args):
        """
        Call the function with either symbolic (for composition) or concrete arguments.

        If first argument is a Delta instance, re-trace the function for composition.
        Otherwise, treat as concrete iterators and use the pre-compiled graph.
        """
        if len(args) == 0:
            raise ValueError(f"Expected at least 1 argument")

        # Check if first arg is a Delta instance (tracing context)
        if isinstance(args[0], Delta):
            # Symbolic execution: inline the trace into caller's delta
            if len(args) != len(self.input_types) + 1:
                raise ValueError(f"Expected {len(self.input_types) + 1} arguments (delta + {len(self.input_types)} streams), got {len(args)}")

            if self._tracing:
                # Recursive call detected: create RecCall node instead of inlining
                delta = args[0]
                input_streams = args[1:]
                return self._create_rec_call(delta, input_streams)

            # Normal composition: inline by tracing
            self._tracing = True
            try:
                result = self.original_func(*args)
            finally:
                self._tracing = False
            return result
        else:
            # Concrete execution: use pre-compiled graph
            return self.run(*args)

    def _create_rec_call(self, delta, input_streams):
        """Create a RecCall node for recursive function call."""
        from python_delta.stream_op import RecCall

        # Generate unique ID for this recursive call
        stream_ids = "_".join(str(s.id) for s in input_streams)
        rec_name = f"rec_{self.original_func.__name__}_{stream_ids}"
        rec_id = hash(rec_name)

        # Collect all vars from input streams
        all_vars = set()
        for stream in input_streams:
            all_vars = all_vars.union(stream.vars)

        # Get output type from the original traced outputs
        output_type = self.outputs.stream_type

        # Create RecCall node
        rec_call = RecCall(rec_id, self, input_streams, all_vars, output_type)
        delta.nodes[rec_id] = rec_call
        delta._register_metadata(rec_id, rec_name)

        return rec_call

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

    def eps(self, stream_type=None):
        """Create an empty stream that immediately raises StopIteration."""
        if stream_type is None:
            stream_type = BaseType("eps")
        name = f"eps_{id(self)}"
        xid = hash(name)
        s = Eps(xid, stream_type)
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
