
from python_delta.typecheck.types import Type, Singleton, TyCat, TyPlus, TyStar, TyEps, TypeVar
from python_delta.stream_ops.typed_buffer import SingletonTypedBuffer

class BufferOp:
    """
    Base class for operations on buffered values.
    Implements magic methods for type-driven operations.
    All constants are auto-promoted to ConstantOp.

    Each BufferOp tracks its source WaitOps so that when you "pull" on it,
    it can pull on all sources until they're complete before evaluating.
    """
    # TODO: should probalby be a way to  go from a bufferop to the slice of the graph that it must sink. this way we can typecheck them more reasonably!
    def __init__(self, stream_type):
        self.stream_type = stream_type

    def get_sources(self):
        raise NotImplementedError("Subclasses must implement get_sources")

    def eval(self):
        raise NotImplementedError("Subclasses must implement eval")

    def __add__(self, other):
        if not isinstance(other, BufferOp):
            other = ConstantOp(other, Singleton(type(other)))
        return BinaryOp(self, '+', other)

    def __radd__(self, other):
        if not isinstance(other, BufferOp):
            other = ConstantOp(other, Singleton(type(other)))
        return BinaryOp(other, '+', self)

    def __eq__(self, other):
        if not isinstance(other, BufferOp):
            other = ConstantOp(other, Singleton(type(other)))
        return ComparisonOp(self, '==', other)

    def __ne__(self, other):
        if not isinstance(other, BufferOp):
            other = ConstantOp(other, Singleton(type(other)))
        return ComparisonOp(self, '!=', other)

class ConstantOp(BufferOp):
    """Wraps a constant Python value as a BufferOp."""
    def __init__(self, value, stream_type):
        super().__init__(stream_type)
        self.value = value

    def get_sources(self):
        return set()

    def eval(self):
        return self.value

class RegisterBuffer(BufferOp):
    def __init__(self,init_buffer_val,klass):
        stream_type = Singleton(klass)
        super().__init__(stream_type=stream_type)
        self.buffer = SingletonTypedBuffer(stream_type)
        self.buffer.value = init_buffer_val
        self.buffer.complete = True
    
    def get_sources(self):
        return {}
    
    def eval(self):
        return self.buffer.get_value()
    
    def update_value(self,new_val):
        self.buffer.value = new_val

class WaitOpBuffer(BufferOp):
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
        assert self.wait_op.buffer.is_complete()
        return self.wait_op.buffer.get_value()

class BinaryOp(BufferOp):
    """Binary arithmetic operation on Singleton."""
    def __init__(self, left, op, right):
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
        super().__init__(Singleton(bool))
        self.parent_op = parent_op
        self.operator = operator  # '<', '<=', '>', '>=', '==', '!='
        self.operand = operand  # Can be a constant or another BufferOp

    def get_sources(self):
        return self.parent_op.get_sources() | self.operand.get_sources()

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