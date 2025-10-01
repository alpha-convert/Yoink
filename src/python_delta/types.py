class Type:
    def __str__(self):
        return self.__class__.__name__

class BaseType(Type):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return isinstance(other, BaseType) and self.name == other.name

    def __hash__(self):
        return hash(("BaseType", self.name))

class TyCat(Type):
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

class TyPlus(Type):
    def __init__(self, left_type, right_type):
        self.left_type = left_type
        self.right_type = right_type

    def __str__(self):
        return f"TyPlus({self.left_type}, {self.right_type})"

    def __eq__(self, other):
        return (isinstance(other, TyPlus) and
                self.left_type == other.left_type and
                self.right_type == other.right_type)

    def __hash__(self):
        return hash(("TyPlus", self.left_type, self.right_type))

class TyStar(Type):
    def __init__(self, element_type):
        self.element_type = element_type

    def __str__(self):
        return f"TyStar({self.element_type})"

    def __eq__(self, other):
        return isinstance(other, TyStar) and self.element_type == other.element_type

    def __hash__(self):
        return hash(("TyStar", self.element_type))

class TyEps(Type):
    def __init__(self):
        pass

    def __str__(self):
        return f"TyEps"

    def __eq__(self, other):
        return isinstance(other, TyEps)

    def __hash__(self):
        return hash("TyEps")
