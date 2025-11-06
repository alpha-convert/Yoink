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
        """
        Generate a graphviz DOT representation of the computation graph.

        Returns:
            str: A DOT format string suitable for rendering with graphviz

        Example:
            >>> @Delta.jit
            >>> def concat(delta, x: STRING_TY, y: STRING_TY):
            ...     return delta.catr(x, y)
            >>> print(concat.to_graphviz())
        """
        from python_delta.util.viz_builder import VizBuilder
        return VizBuilder(self).to_graphviz()

    def save_graphviz(self, filename):
        """
        Save the graphviz DOT representation to a file.

        Args:
            filename (str): Path to save the DOT file. If it ends with .png, .pdf, or .svg,
                          will attempt to render using graphviz (if available).

        Returns:
            str: Path to the saved file

        Example:
            >>> @Delta.jit
            >>> def concat(delta, x: STRING_TY, y: STRING_TY):
            ...     return delta.catr(x, y)
            >>> concat.save_graphviz('graph.png')  # Requires graphviz installed
            >>> concat.save_graphviz('graph.dot')  # Just save DOT file
        """
        from python_delta.util.viz_builder import VizBuilder
        return VizBuilder(self).save(filename)

    def compile(self, compiler) -> type:
        """
        Compile the StreamOp graph into a single flat iterator class.

        Args:
            compiler: Compiler class to use (DirectCompiler, CPSCompiler, or GeneratorCompiler).

        Returns the compiled class (not an instance).
        """
        # Handle tuple outputs (e.g., from catl which returns (x, y))
        # For now, only compile single outputs
        if isinstance(self.outputs, tuple):
            raise NotImplementedError("Compilation of tuple outputs not yet supported")

        return compiler.compile(self)

    def get_code(self, compiler) -> str:
        """
        Get the compiled Python code for this dataflow graph as a string.

        Args:
            compiler: Compiler class to use (DirectCompiler, CPSCompiler, or GeneratorCompiler).

        Returns:
            str: The generated Python code
        """
        # Handle tuple outputs (e.g., from catl which returns (x, y))
        if isinstance(self.outputs, tuple):
            raise NotImplementedError("Compilation of tuple outputs not yet supported")

        return compiler.get_code(self)

    def print_code(self, compiler):
        print(self.get_code(compiler))