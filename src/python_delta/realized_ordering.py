from python_delta.partial_order import PartialOrder

class RealizedOrdering:
    """
    A realized ordering consists of two partial orders:
    - required: edges that must exist
    - forbidden: edges that cannot exist

    Both partial orders share the same metadata dictionary for human-readable names.
    """

    def __init__(self):
        self.metadata = {}  # Shared metadata dict for both partial orders
        self.required = PartialOrder(self.metadata)
        self.forbidden = PartialOrder(self.metadata)

    def check_consistency(self):
        """Check if any required edge conflicts with forbidden edges."""
        if self.required.overlaps_with(self.forbidden):
            conflicting = self.required.edges.intersection(self.forbidden.edges)
            print(self.required)
            print(self.forbidden)
            raise ValueError(f"Inconsistent constraints: edges both required and forbidden: {conflicting}")

    def add_ordered(self, x, y):
        """
        Add an ordered constraint: x must come before y.
        Adds required edge x -> y and forbidden edge y -> x.
        """
        self.required.add_edge(x, y)
        self.forbidden.add_edge(y, x)
        self.check_consistency()

    def add_all_ordered(self, set1, set2):
        """
        Add ordered constraints: all elements in set1 must come before all elements in set2.
        For each pair (x, y) where x ∈ set1 and y ∈ set2:
          - Adds required edge x -> y
          - Adds forbidden edge y -> x
        """
        self.required.add_all_edges(set1, set2)
        self.forbidden.add_all_edges(set2, set1)
        self.check_consistency()

    def add_unordered(self, x, y):
        """
        Add an unordered constraint: x and y are mutually exclusive.
        Adds forbidden edges x -> y and y -> x.
        """
        self.forbidden.add_edge(x, y)
        self.forbidden.add_edge(y, x)
        self.check_consistency()

    def add_all_unordered(self, set1, set2):
        """
        Add unordered constraints: all pairs from set1 and set2 are mutually exclusive.
        For each pair (x, y) where x ∈ set1 and y ∈ set2:
          - Adds forbidden edges x <-> y (both directions)
        """
        self.forbidden.add_all_edges(set1, set2)
        self.forbidden.add_all_edges(set2, set1)
        self.check_consistency()

    def add_in_place_of(self, x, vars):
        """
        Add x in place of vars: x inherits all ordering constraints from vars.
        Finds the intersection of predecessors/successors across all vars,
        then adds edges from common predecessors to x and from x to common successors.
        """
        if len(vars) > 0:
          common_preds = set.intersection(*[self.required.predecessors(var) for var in vars])
          common_succs = set.intersection(*[self.required.successors(var) for var in vars])

          self.required.add_all_edges(common_preds, {x})

          self.required.add_all_edges({x}, common_succs)

          self.check_consistency()

    def __str__(self):
        return f"RealizedOrdering(\n  required={self.required},\n  forbidden={self.forbidden}\n)"
