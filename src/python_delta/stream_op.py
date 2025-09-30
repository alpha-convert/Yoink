class StreamOp:
    """Base class for stream operations."""
    def __init__(self, id, inps, vars, stream_type):
        self.id = id
        self.inps = inps
        self.vars = vars
        self.stream_type = stream_type

    def __str__(self):
        return f"{self.__class__.__name__}({self.stream_type})"


class Var(StreamOp):
    """Variable stream operation."""
    def __init__(self, id, name, stream_type):
        super().__init__(id, [], {id}, stream_type)
        self.name = name

    def __str__(self):
        return f"Var({self.name}: {self.stream_type})"


class CatR(StreamOp):
    """Concatenation (right) - ordered composition."""
    def __init__(self, id, s1_id, s2_id, vars, stream_type):
        super().__init__(id, [s1_id, s2_id], vars, stream_type)


class CatProj(StreamOp):
    """Projection from a TyCat stream."""
    def __init__(self, id, s_id, stream_type, position):
        super().__init__(id, [s_id], {id}, stream_type)
        self.position = position  # 1 or 2

    def __str__(self):
        return f"CatProj{self.position}({self.stream_type})"


class ParR(StreamOp):
    """Parallel composition (right)."""
    def __init__(self, id, s1_id, s2_id, vars, stream_type):
        super().__init__(id, [s1_id, s2_id], vars, stream_type)


class ParProj(StreamOp):
    """Projection from a TyPar stream."""
    def __init__(self, id, s_id, stream_type, position):
        super().__init__(id, [s_id], {id}, stream_type)
        self.position = position  # 1 or 2

    def __str__(self):
        return f"ParProj{self.position}({self.stream_type})"
