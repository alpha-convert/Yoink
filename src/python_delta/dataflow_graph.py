from __future__ import annotations

import ast
from python_delta.compilation import CompilationContext


class DataflowGraph:
    """
    A dataflow graph representing a traced stream function that can be executed
    with concrete iterators or composed with other traced functions.
    """
    def __init__(self, nodes, input_vars, outputs, original_func, input_types):
        """
        Args:
            nodes: The traced computation graph
            input_vars: List of Var nodes representing function inputs
            outputs: The output StreamOp(s) from the traced function
            original_func: The original untraced function
            input_types: List of input types for the function
        """
        self.nodes = nodes
        self.input_vars = input_vars
        self.outputs = outputs
        self.original_func = original_func
        self.input_types = input_types

    def __call__(self, *args):
        """
        Call the function with either symbolic (for composition) or concrete arguments.

        If first argument is a Delta instance, re-trace the function for composition.
        Otherwise, treat as concrete iterators and use the pre-compiled graph.
        """
        if len(args) == 0:
            return self.run(*args)

        from python_delta.delta import Delta
        # Check if first arg is a Delta instance
        if isinstance(args[0], Delta):
            if len(args) != len(self.input_types) + 1:
                raise ValueError(f"Expected {len(self.input_types) + 1} arguments (delta + {len(self.input_types)} streams), got {len(args)}")
            return self.original_func(*args)
        else:
            return self.run(*args)

    def run(self, *iterators):
        """
        Execute the dataflow graph with concrete iterators.

        Args:
            *iterators: Concrete iterators to bind to input variables

        Returns:
            The output stream(s), ready to be iterated
        """
        if len(iterators) != len(self.input_vars):
            raise ValueError(f"Expected {len(self.input_vars)} iterators, got {len(iterators)}")

        # Reset all nodes to initial state
        for node in self.nodes:
            node.reset()

        # Bind concrete iterators to Var sources
        for var, iterator in zip(self.input_vars, iterators):
            var.source = iterator

        # Return the output stream(s)
        return self.outputs

    def to_graphviz(self):
        from python_delta.util.viz_builder import VizBuilder
        return VizBuilder(self).to_graphviz()

    def save_graphviz(self, filename):
        from python_delta.util.viz_builder import VizBuilder
        return VizBuilder(self).save(filename)

    def compile(self, compiler) -> type:
        if isinstance(self.outputs, tuple):
            raise NotImplementedError("Compilation of tuple outputs not yet supported")

        return compiler.compile(self)

    def get_code(self, compiler) -> str:
        if isinstance(self.outputs, tuple):
            raise NotImplementedError("Compilation of tuple outputs not yet supported")

        return compiler.get_code(self)

    def print_code(self, compiler):
        print(self.get_code(compiler))