"""Base classes for stream operations."""

from __future__ import annotations

from typing import List, Callable
import ast


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

    def accept(self, visitor) -> List[ast.stmt]:
        """Accept a visitor for compilation (visitor pattern).

        This is the entry point for the visitor pattern. The visitor
        dispatches to the appropriate visit_* method based on node type.

        Args:
            visitor: A CompilerVisitor instance

        Returns:
            List of AST statements compiled by the visitor
        """
        return visitor.visit(self)

    def _compile_stmts_cps(
        self,
        ctx,
        done_cont: List[ast.stmt],
        skip_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        """Compile using continuation-passing style.

        Args:
            ctx: Compilation context
            done_cont: Statements to execute instead of setting dst = DONE
            skip_cont: Statements to execute instead of setting dst = None
            yield_cont: Function taking a value expression and returning statements
                       to execute instead of setting dst = <expr>

        Returns:
            List of AST statements implementing the stream operation
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement _compile_stmts_cps")

    def _compile_stmts_generator(
        self,
        ctx,
        done_cont: List[ast.stmt],
        yield_cont: Callable[[ast.expr], List[ast.stmt]]
    ) -> List[ast.stmt]:
        """Compile to generator code .
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement _compile_stmts_generator")

    def _get_state_initializers(self, ctx):
        return []

    def _get_reset_stmts(self, ctx):
        return []
