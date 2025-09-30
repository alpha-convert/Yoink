class PartialOrder:
    def __init__(self):
        self.edges = set()  # Set of (x, y) where x <= y (maintains transitive closure, non-reflexive)

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

        self.edges.add((x, y))
        self._ensure_transitive_closure()

    def add_all_edges(self, set1, set2):
        for x in set1:
            for y in set2:
                self.add_edge(x, y)

    def has_edge(self, x, y):
        return (x, y) in self.edges

    def predecessors(self, x):
        return {y for (y, z) in self.edges if z == x and y != x}

    def successors(self, x):
        return {z for (y, z) in self.edges if y == x and z != x}

    def overlaps_with(self, other):
        return bool(self.edges.intersection(other.edges))

    def __str__(self):
        if not self.edges:
            return "PartialOrder({})"
        edges_str = ", ".join(f"{x} < {y}" for x, y in sorted(self.edges))
        return f"PartialOrder({{{edges_str}}})"
