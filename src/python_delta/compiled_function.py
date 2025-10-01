class CompiledFunction:
    """
    A compiled stream function that can be executed with concrete iterators
    or composed with other traced functions.
    """
    def __init__(self, traced_delta, input_vars, outputs, original_func, input_types):
        """
        Args:
            traced_delta: The Delta instance containing the traced computation graph
            input_vars: List of Var nodes representing function inputs
            outputs: The output StreamOp(s) from the traced function
            original_func: The original untraced function
            input_types: List of input types for the function
        """
        self.traced_delta = traced_delta
        self.input_vars = input_vars
        self.outputs = outputs
        self.original_func = original_func
        self.input_types = input_types
        self._tracing = False  # Flag to detect recursive calls

    def __call__(self, *args):
        """
        Call the function with either symbolic (for composition) or concrete arguments.

        If first argument is a Delta instance, re-trace the function for composition.
        Otherwise, treat as concrete iterators and use the pre-compiled graph.
        """
        if len(args) == 0:
            return self.run(*args)

        # Check if first arg is a Delta instance (tracing context)
        if isinstance(args[0], type(self.traced_delta)):
            if len(args) != len(self.input_types) + 1:
                raise ValueError(f"Expected {len(self.input_types) + 1} arguments (delta + {len(self.input_types)} streams), got {len(args)}")

            if self._tracing:
                # Recursive call detected: create RecCall node instead of inlining
                delta = args[0]
                input_streams = args[1:]
                return self._create_rec_call(delta, input_streams)

            # Normal composition: inline by tracing
            self._tracing = True
            try:
                result = self.original_func(*args)
            finally:
                self._tracing = False
            return result
        else:
            # Concrete execution: use pre-compiled graph
            return self.run(*args)

    def _create_rec_call(self, delta, input_streams):
        """Create a RecCall node for recursive function call."""
        from python_delta.stream_op import RecCall

        # Get output type from the original traced outputs
        output_type = self.outputs.stream_type

        # Create RecCall node (ID is computed automatically from structure)
        rec_call = RecCall(self, input_streams, output_type)
        delta.nodes.add(rec_call)
        delta._register_node(rec_call.id, f"rec_{self.original_func.__name__}_{rec_call.id}", rec_call)

        return rec_call

    def run(self, *iterators):
        """
        Execute the compiled function with concrete iterators.

        Args:
            *iterators: Concrete iterators to bind to input variables

        Returns:
            The output stream(s), ready to be iterated
        """
        if len(iterators) != len(self.input_vars):
            raise ValueError(f"Expected {len(self.input_vars)} iterators, got {len(iterators)}")

        # Reset all nodes to initial state
        for node in self.traced_delta.nodes:
            node.reset()

        # Bind concrete iterators to Var sources
        for var, iterator in zip(self.input_vars, iterators):
            var.source = iterator

        # Return the output stream(s)
        return self.outputs