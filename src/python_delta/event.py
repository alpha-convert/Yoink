# Event wrapper classes for stream elements

class CatEvA:
    """Event from left side of concatenation."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"CatEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, CatEvA) and self.value == other.value

class CatPunc:
    """Punctuation marker between A and B in concatenation."""
    def __repr__(self):
        return "CatPunc"
    def __eq__(self, other):
        return isinstance(other, CatPunc)

class ParEvA:
    """Event from left side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvA) and self.value == other.value

class ParEvB:
    """Event from right side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvB({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvB) and self.value == other.value

class PlusPuncA:
    """Tag marker for left injection in sum types."""
    def __repr__(self):
        return "PlusPuncA"
    def __eq__(self, other):
        return isinstance(other, PlusPuncA)

class PlusPuncB:
    """Tag marker for right injection in sum types."""
    def __repr__(self):
        return "PlusPuncB"
    def __eq__(self, other):
        return isinstance(other, PlusPuncB)
