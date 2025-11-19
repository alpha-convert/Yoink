
from python_delta.event import BaseEvent
from python_delta.typecheck.types import Type, Singleton, TyCat, TyPlus, TyStar, TyEps, TypeVar
from python_delta.stream_ops.typed_buffer import SingletonTypedBuffer

class BufferOp:
    """
    Base class for operations on buffered values.
    Implements magic methods for type-driven operations.
    All constants are auto-promoted to ConstantOp.

    Each BufferOp tracks its sources so we can traverse back through the graph
    to figure out which waits must be sunk beforehand.
    """
    def __init__(self, stream_type):
        self.stream_type = stream_type

    def get_sources(self):
        raise NotImplementedError("Subclasses must implement get_sources")

    def eval(self):
        raise NotImplementedError("Subclasses must implement eval")

    @property
    def id(self):
        raise NotImplementedError("Subclasses must implement id")

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

    @property
    def id(self):
        return hash(("ConstantOp", id(self.value)))

    def get_sources(self):
        return set()

    def eval(self):
        return [BaseEvent(self.value)]

class RegisterBuffer(BufferOp):
    def __init__(self,init_buffer_val,klass):
        stream_type = Singleton(klass)
        super().__init__(stream_type=stream_type)
        self.buffer = SingletonTypedBuffer()
        self.buffer.value = init_buffer_val
        self.buffer.complete = True

    @property
    def id(self):
        return hash(("RegisterBuffer", id(self.buffer)))

    def get_sources(self):
        return {}

    def eval(self):
        return self.buffer.get_events()

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

    @property
    def id(self):
        return hash(("WaitOpBuffer", self.wait_op.id))

    def get_sources(self):
        """Return the single WaitOp this depends on."""
        return {self.wait_op}

    def eval(self):
        """Read the buffered value from the WaitOp's buffer."""
        assert self.wait_op.buffer.is_complete()
        return self.wait_op.buffer.get_events()

class BinaryOp(BufferOp):
    """Binary arithmetic operation on Singleton."""
    def __init__(self, left, op, right):
        super().__init__(left.stream_type)
        self.left = left
        self.op = op
        self.right = right

    @property
    def id(self):
        return hash(("BinaryOp", self.left.id, self.op, self.right.id))

    def eval(self):
        """Evaluate parent, operand, and apply operator."""
        left = self.left.eval()[0].value
        right = self.right.eval()[0].value

        if self.op == '+':
            v = left + right
        elif self.op == '-':
            v = left - right
        elif self.op == '*':
            v = left * right
        elif self.op == '/':
            v = left / right
        elif self.op == '//':
            v = left // right
        elif self.op == '%':
            v = left % right
        elif self.op == '**':
            v = left ** right
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
        
        return [BaseEvent(v)]

    def get_sources(self):
        return self.left.get_sources() | self.right.get_sources()

class UnaryOp(BufferOp):
    """Unary operation on Singleton."""
    def __init__(self, parent_op, operator):
        super().__init__(parent_op.stream_type)
        self.parent_op = parent_op
        self.operator = operator  # '-', '+', '~', 'not'

    @property
    def id(self):
        return hash(("UnaryOp", self.operator, self.parent_op.id))

    def get_sources(self):
        return self.parent_op.get_sources()

    def eval(self):
        """Evaluate parent and apply unary operator."""
        value = self.parent_op.eval()[0].value

        if self.operator == '-':
            res = -value
        elif self.operator == '+':
            res = +value
        elif self.operator == '~':
            res = ~value
        elif self.operator == 'not':
            res = not value
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
        
        return [BaseEvent(res)]

class ComparisonOp(BufferOp):
    """Comparison operation on Singleton."""
    def __init__(self, parent_op, operator, operand):
        super().__init__(Singleton(bool))
        self.parent_op = parent_op
        self.operator = operator  # '<', '<=', '>', '>=', '==', '!='
        self.operand = operand  # Can be a constant or another BufferOp

    @property
    def id(self):
        return hash(("ComparisonOp", self.parent_op.id, self.operator, self.operand.id))

    def get_sources(self):
        return self.parent_op.get_sources() | self.operand.get_sources()

    def eval(self):
        """Evaluate parent, operand, and apply comparison."""
        left = self.parent_op.eval()[0].value
        right = self.operand.eval()[0].value

        if self.operator == '<':
            res = left < right
        elif self.operator == '<=':
            res = left <= right
        elif self.operator == '>':
            res = left > right
        elif self.operator == '>=':
            res = left >= right
        elif self.operator == '==':
            res = left == right
        elif self.operator == '!=':
            res = left != right
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
        
        return [BaseEvent(res)]