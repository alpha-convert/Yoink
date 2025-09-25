class Type:
    """Base class for all types"""
    def __str__(self):
        return self.__class__.__name__

class BaseType(Type):
    """Base type for individual variables"""
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return isinstance(other, BaseType) and self.name == other.name

    def __hash__(self):
        return hash(("BaseType", self.name))

class TyCat(Type):
    """Sequential composition type: TyCat s t"""
    def __init__(self, left_type, right_type):
        self.left_type = left_type
        self.right_type = right_type

    def __str__(self):
        return f"TyCat({self.left_type}, {self.right_type})"

    def __eq__(self, other):
        return (isinstance(other, TyCat) and
                self.left_type == other.left_type and
                self.right_type == other.right_type)

    def __hash__(self):
        return hash(("TyCat", self.left_type, self.right_type))

class TyPar(Type):
    """Parallel composition type: TyPar s t"""
    def __init__(self, left_type, right_type):
        self.left_type = left_type
        self.right_type = right_type

    def __str__(self):
        return f"TyPar({self.left_type}, {self.right_type})"

    def __eq__(self, other):
        return (isinstance(other, TyPar) and
                self.left_type == other.left_type and
                self.right_type == other.right_type)

    def __hash__(self):
        return hash(("TyPar", self.left_type, self.right_type))

class PartialOrder:
    def __init__(self):
        self.edges = set()  # Set of (x, y) where x <= y (maintains transitive closure, non-reflexive)
        self.variables = set()

    def add_variable(self, var):
        self.variables.add(var)

    def _ensure_transitive_closure(self):
        changed = True
        while changed:
            changed = False
            new_edges = set()
            for (a, b) in self.edges:
                for (c, d) in self.edges:
                    if b == c and (a, d) not in self.edges:
                        new_edges.add((a, d))
                        changed = True
            self.edges.update(new_edges)

    def add_edge(self, x, y):
        if x == y:
            return

        # Add x and y as variables if not present
        if x not in self.variables:
            self.add_variable(x)
        if y not in self.variables:
            self.add_variable(y)

        self.edges.add((x, y))
        self._ensure_transitive_closure()

    def add_all_edges(self, set1, set2):
        for x in set1:
            for y in set2:
                self.add_edge(x, y)

    def has_edge(self, x, y):
        # Since we maintain transitive closure, just check directly
        return (x, y) in self.edges

    def predecessors(self, x):
        # Return all variables y such that y < x (not including x itself)
        return {y for (y, z) in self.edges if z == x and y != x}

    def successors(self, x):
        # Return all variables y such that x < y (not including x itself)
        return {z for (y, z) in self.edges if y == x and z != x}

    def overlaps_with(self, other):
        """Check if this partial order has any edges that overlap with another"""
        return bool(self.edges.intersection(other.edges))

class Delta:
    def __init__(self):
        self.required = PartialOrder()
        self.forbidden = PartialOrder()
    
    def _check_consistency(self):
        if self.required.overlaps_with(self.forbidden):
            conflicting = self.required.edges.intersection(self.forbidden.edges)
            raise ValueError(f"Inconsistent constraints: edges both required and forbidden: {conflicting}")

    # TODO: refactoring requried here. We need to simplify this to have
    # "register x <= y", and "x in place of s", which puts in the implied requried edges
    # between x and vars(s)

    def var(self, v, var_type=None):
        if var_type is None:
            var_type = BaseType(v)
        name = f"var_{v}"
        xid = hash(name)
        self.required.add_variable(xid)
        self.forbidden.add_variable(xid)
        return Stream(xid, name, [], {xid}, var_type)
    
    def catr(self, s1, s2):
        s1id = s1.id
        s2id = s2.id
        zname = f"catr_{s1id}_{s2id}"
        zid = hash(zname)

        if s1.vars.intersection(s2.vars):
            raise ValueError("Illegal CatR, overlapping vars")

        self.required.add_all_edges(s1.vars, s2.vars)
        self.forbidden.add_all_edges(s2.vars, s1.vars)
        self._check_consistency()

        return Stream(zid, zname, [s1id, s2id], s1.vars.union(s2.vars), TyCat(s1.stream_type, s2.stream_type))

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
        self.required.add_variable(xid)
        self.required.add_variable(yid)
        self.forbidden.add_variable(xid)
        self.forbidden.add_variable(yid)

        self.required.add_edge(xid, yid)
        self.forbidden.add_edge(yid, xid)

        for var in s.vars:
            preds = self.required.predecessors(var)
            self.required.add_all_edges(preds, {xid})
            self.required.add_all_edges(preds, {yid})

            succs = self.required.successors(var)
            self.required.add_all_edges({xid}, succs)
            self.required.add_all_edges({yid}, succs)

        self._check_consistency()

        x = Stream(xid, "catproj1", [sid], {xid}, left_type)
        y = Stream(yid, "catproj2", [sid], {yid}, right_type)
        return (x, y)

    def parr(self, s1, s2):
        s1id = s1.id
        s2id = s2.id
        zname = f"parr_{s1id}_{s2id}"
        zid = hash(zname)

        self.forbidden.add_all_edges(s1.vars, s2.vars)
        self.forbidden.add_all_edges(s2.vars, s1.vars)  # Make it symmetric
        self._check_consistency()

        return Stream(zid, zname, [s1id, s2id], s1.vars.union(s2.vars), TyPar(s1.stream_type, s2.stream_type))

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
        self.required.add_variable(xid)
        self.required.add_variable(yid)
        self.forbidden.add_variable(xid)
        self.forbidden.add_variable(yid)
        self.forbidden.add_edge(xid, yid)
        self.forbidden.add_edge(yid, xid)

        for var in s.vars:
            preds = self.required.predecessors(var)
            self.required.add_all_edges(preds, {xid})
            self.required.add_all_edges(preds, {yid})

            succs = self.required.successors(var)
            self.required.add_all_edges({xid}, succs)
            self.required.add_all_edges({yid}, succs)

        self._check_consistency()

        x = Stream(xid, "parproj1", [sid], {xid}, left_type)
        y = Stream(yid, "parproj2", [sid], {yid}, right_type)
        return (x, y)

class Stream:
    def __init__(self, id, op, inps, vars, stream_type):
        self.id = id
        self.op = op
        self.inps = inps
        self.vars = vars
        self.stream_type = stream_type

    def __str__(self):
        return f"Stream({self.op}: {self.stream_type})"

delta = Delta()

x = delta.var("x")
y = delta.var("y")

# z2 = delta.catr(x,y)  # This correctly fails now!
# z = delta.parr(x, y)
