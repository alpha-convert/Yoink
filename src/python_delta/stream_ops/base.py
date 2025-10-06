"""Base classes for stream operations."""

from __future__ import annotations

class Done:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "Done"


DONE = Done()


class StreamOp:
    """Base class for stream operations."""
    def __init__(self, stream_type):
        self.stream_type = stream_type

    @property
    def id(self):
        """Compute structural ID from operation structure. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement id property")

    @property
    def vars(self):
        """Compute vars set from operation structure. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement vars property")

    def __str__(self):
        return f"{self.__class__.__name__}({self.stream_type})"

    def __iter__(self):
        return self

    def __next__(self):
        result = self._pull()
        if result is DONE:
            raise StopIteration
        return result

    def _pull(self):
        """Pull next element from stream. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement _pull")

    def reset(self):
        """Reset stream to initial state. Subclasses should override if stateful."""
        pass

    def _compile_stmts(self, ctx, dst: str):
        raise NotImplementedError(f"{self.__class__.__name__} must implement _compile_stmts")

    def _get_state_initializers(self, ctx):
        return []

    def _get_reset_stmts(self, ctx):
        return []
