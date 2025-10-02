class UnificationError(Exception):
    def __init__(self,ty1,ty2):
        self.ty1 = ty1
        self.ty2 = ty2

class OccursCheckFail(Exception):
    def __init__(self):
        pass

class NullabilityError(Exception):
    """Raised when trying to check nullability on non-concrete types."""
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class Type:
    def __str__(self):
        return self.__class__.__name__

    def unify_with(self,other):
        """ Unify these two types """
        raise NotImplementedError("Instances must specify unification")

    def occurs_var(self,var):
        """ Does this type variable occur in this type? """
        raise NotImplementedError("Instances must specify occurs check")

    def nullable(self):
        """
        Check if this type is nullable (can produce zero elements).

        Raises:
            NullabilityError: If called on a type variable or other non-concrete type
        """
        raise NotImplementedError("Instances must specify nullability")

    def derivative(self, event):
        from python_delta.typecheck.derivative import derivative
        return derivative(self, event)


class NullaryType(Type):
    """Base class for nullary type constructors without parameters."""

    def __str__(self):
        return self.__class__.__name__

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __hash__(self):
        return hash(self.__class__.__name__)

    def occurs_var(self, var):
        return None

    def unify_with(self, other):
        if isinstance(other, TypeVar):
            if other.link is None:
                self.occurs_var(other)
                other.link = self
            else:
                self.unify_with(other.link)
        elif isinstance(other, self.__class__):
            return
        else:
            raise UnificationError(self, other)

    def nullable(self):
        """NullaryType subclasses must override this."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement nullable")


class UnaryType(Type):
    """Base class for unary type constructors (TyStar)."""

    def __init__(self, element_type):
        self.element_type = element_type

    def __str__(self):
        return f"{self.__class__.__name__}({self.element_type})"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.element_type == other.element_type

    def __hash__(self):
        return hash((self.__class__.__name__, self.element_type))

    def occurs_var(self, var):
        self.element_type.occurs_var(var)

    def unify_with(self, other):
        if isinstance(other, TypeVar):
            if other.link is None:
                self.occurs_var(other)
                other.link = self
            else:
                self.unify_with(other.link)
        elif not isinstance(other, self.__class__):
            raise UnificationError(self, other)
        else:
            self.element_type.unify_with(other.element_type)

    def nullable(self):
        """UnaryType subclasses must override this."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement nullable")

class BinaryType(Type):
    """Base class for binary type constructors (TyCat, TyPar, TyPlus)."""

    def __init__(self, left_type, right_type):
        self.left_type = left_type
        self.right_type = right_type

    def __str__(self):
        return f"{self.__class__.__name__}({self.left_type}, {self.right_type})"

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.left_type == other.left_type and
                self.right_type == other.right_type)

    def __hash__(self):
        return hash((self.__class__.__name__, self.left_type, self.right_type))

    def occurs_var(self, var):
        self.left_type.occurs_var(var)
        self.right_type.occurs_var(var)

    def unify_with(self, other):
        if isinstance(other, TypeVar):
            if other.link is None:
                self.occurs_var(other)
                other.link = self
            else:
                self.unify_with(other.link)
        elif not isinstance(other, self.__class__):
            raise UnificationError(self, other)
        else:
            self.left_type.unify_with(other.left_type)
            self.right_type.unify_with(other.right_type)

    def nullable(self):
        """BinaryType subclasses must override this."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement nullable")

class TypeVar(Type):
    next_unif_id = 0

    def __str__(self):
        if self.link is None:
            return f"TypeVar({self.id})"
        else:
            return f"{self.link}"

    @staticmethod
    def fresh_unif_id():
        TypeVar.next_unif_id += 1
        return TypeVar.next_unif_id

    def occurs_var(self,var):
        if self.link is not None:
            return self.link.occurs_var(var)
        elif self.id == var.id:
            raise OccursCheckFail()
        else:
            self.level = min(self.level, var.level)
            return False

    # A type variable is either (1) a unique ID and a "level", or (2) a reference to another type.
    def __init__(self, level):
        self.id = TypeVar.fresh_unif_id()
        self.level = level
        self.link = None

    def unify_with(self,other):
        if self.link is not None:
            self.link.unify_with(other)
        else:
            other.occurs_var(var=self)  # Raises OccursCheckFail if check fails
            self.link = other

    def nullable(self):
        """Type variables cannot be checked for nullability."""
        if self.link is not None:
            return self.link.nullable()
        raise NullabilityError("only concrete types can be nullable or not nullable")


class Singleton(Type):
    """A stream consisting of exactly one element"""

    def __init__(self, python_class):
        self.python_class = python_class

    def __str__(self):
        return self.python_class.__name__

    def __eq__(self, other):
        return isinstance(other, Singleton) and self.python_class == other.python_class

    def __hash__(self):
        return hash(("Singleton", self.python_class))

    def occurs_var(self, var):
        return None

    def unify_with(self, other):
        if isinstance(other, TypeVar):
            if other.link is None:
                self.occurs_var(other)
                other.link = self
            else:
                self.unify_with(other.link)
        elif isinstance(other, Singleton) and other.python_class == self.python_class:
            return
        else:
            raise UnificationError(self, other)

    def nullable(self):
        """Singleton types are not nullable."""
        return False

class TyCat(BinaryType):
    """Concatenation type."""
    def nullable(self):
        """Cat is not nullable."""
        return False


class TyPar(BinaryType):
    """Parallel composition type."""
    def nullable(self):
        """Par is nullable if both its args are nullable."""
        return self.left_type.nullable() and self.right_type.nullable()

class TyPlus(BinaryType):
    """Sum type."""
    def nullable(self):
        """Plus is not nullable."""
        return False

class TyStar(UnaryType):
    """Star type (Kleene star)."""
    def nullable(self):
        """Star is not nullable."""
        return False

class TyEps(NullaryType):
    """Empty stream type."""
    def nullable(self):
        """Eps is nullable."""
        return True