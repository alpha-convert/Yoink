class Stream:
    def __init__(self, id, op, inps, vars, stream_type):
        self.id = id
        self.op = op
        self.inps = inps
        self.vars = vars
        self.stream_type = stream_type

    def __str__(self):
        return f"Stream({self.op}: {self.stream_type})"
