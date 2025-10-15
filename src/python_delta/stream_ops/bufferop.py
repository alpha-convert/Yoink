
from python_delta.typecheck.types import Type, Singleton, TyCat, TyPlus, TyStar, TyEps, TypeVar

class BufferOp:
    """
    Base class for operations on buffered values.
    Implements magic methods for type-driven operations.
    All constants are auto-promoted to ConstantOp.

    Each BufferOp tracks its source WaitOps so that when you "pull" on it,
    it can pull on all sources until they're complete before evaluating.
    """
    def __init__(self, stream_type):
        self.stream_type = stream_type

    def get_sources(self):
        raise NotImplementedError("Subclasses must implement get_sources")

    def eval(self):
        raise NotImplementedError("Subclasses must implement eval")

    def __add__(self, other):
        if not isinstance(other, BufferOp):
            other = ConstantOp(other, Singleton(type(other)))

        self.stream_type.unify_with(other.stream_type)
        return BinaryOp(self, '+', other)

    def __radd__(self, other):
        if not isinstance(other, BufferOp):
            other = ConstantOp(other, Singleton(type(other)))
        self.stream_type.unify_with(other.stream_type)
        return BinaryOp(other, '+', self)

class ConstantOp(BufferOp):
    """Wraps a constant Python value as a BufferOp."""
    def __init__(self, value, stream_type):
        super().__init__(stream_type)
        self.value = value

    def get_sources(self):
        return set()

    def eval(self):
        return self.value


class SourceBuffer(BufferOp):
    """
    Root operation - points to the WaitOp that provides the buffered value.
    This is always the root of any BufferOp tree.
    """
    def __init__(self, wait_op):
        super().__init__(wait_op.stream_type)
        self.wait_op = wait_op

    def get_sources(self):
        """Return the single WaitOp this depends on."""
        return {self.wait_op}

    def eval(self):
        """Read the buffered value from the WaitOp's buffer."""
        return self.wait_op.buffer.get_value()

class BinaryOp(BufferOp):
    """Binary arithmetic operation on Singleton."""
    def __init__(self, left, op, right):
        left.stream_type.unify_with(right.stream_type)
        super().__init__(left.stream_type)
        self.left = left
        self.op = op
        self.right = right

    def eval(self):
        """Evaluate parent, operand, and apply operator."""
        left = self.left.eval()
        right = self.right.eval()

        if self.op == '+':
            return left + right
        elif self.op == '-':
            return left - right
        elif self.op == '*':
            return left * right
        elif self.op == '/':
            return left / right
        elif self.op == '//':
            return left // right
        elif self.op == '%':
            return left % right
        elif self.op == '**':
            return left ** right
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
        
    def get_sources(self):
        return self.left.get_sources() | self.right.get_sources()

class UnaryOp(BufferOp):
    """Unary operation on Singleton."""
    def __init__(self, parent_op, operator):
        self.parent_op = parent_op
        self.operator = operator  # '-', '+', '~', 'not'

    def eval(self):
        """Evaluate parent and apply unary operator."""
        value = self.parent_op.eval()

        if self.operator == '-':
            return -value
        elif self.operator == '+':
            return +value
        elif self.operator == '~':
            return ~value
        elif self.operator == 'not':
            return not value
        else:
            raise ValueError(f"Unknown operator: {self.operator}")

class ComparisonOp(BufferOp):
    """Comparison operation on Singleton."""
    def __init__(self, parent_op, operator, operand):
        self.parent_op = parent_op
        self.operator = operator  # '<', '<=', '>', '>=', '==', '!='
        self.operand = operand  # Can be a constant or another BufferOp

    def eval(self):
        """Evaluate parent, operand, and apply comparison."""
        left = self.parent_op.eval()
        right = self.operand.eval() if isinstance(self.operand, BufferOp) else self.operand

        if self.operator == '<':
            return left < right
        elif self.operator == '<=':
            return left <= right
        elif self.operator == '>':
            return left > right
        elif self.operator == '>=':
            return left >= right
        elif self.operator == '==':
            return left == right
        elif self.operator == '!=':
            return left != right
        else:
            raise ValueError(f"Unknown operator: {self.operator}")