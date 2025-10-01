from python_delta.event import CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB


class StreamOp:
    """Base class for stream operations."""
    def __init__(self, id, vars, stream_type):
        self.id = id
        self.vars = vars
        self.stream_type = stream_type

    def __str__(self):
        return f"{self.__class__.__name__}({self.stream_type})"

    def __iter__(self):
        """Make StreamOp iterable."""
        return self

    def __Text__(self):
        """Pull the next element from the stream. Raise StopIteration when exhausted."""
        raise NotImplementedError("Subclasses must implement __next__")

    def reset(self):
        """Reset the stream to its initial state for reuse."""
        raise NotImplementedError("Subclasses must implement reset")


class Var(StreamOp):
    """Variable stream operation."""
    def __init__(self, id, name, stream_type):
        super().__init__(id, {id}, stream_type)
        self.name = name
        self.source = None  # Will be bound during .run()

    def __str__(self):
        return f"Var({self.name}: {self.stream_type})"

    def __next__(self):
        """Pull from the source iterator."""
        if self.source is None:
            raise RuntimeError(f"Var '{self.name}' has no source bound")
        return next(self.source)

    def reset(self):
        """Var has no internal state to reset."""
        pass


class Eps(StreamOp):
    """Empty stream - immediately raises StopIteration."""
    def __init__(self, id, stream_type):
        super().__init__(id, set(), stream_type)

    def __str__(self):
        return f"Eps({self.stream_type})"

    def __next__(self):
        """Always raise StopIteration - empty stream has no elements."""
        raise StopIteration

    def reset(self):
        """Eps has no internal state to reset."""
        pass


class CatR(StreamOp):
    """Concatenation (right) - ordered composition."""
    def __init__(self, id, s1, s2, vars, stream_type):
        super().__init__(id, vars, stream_type)
        self.input_streams = [s1, s2]
        self.current_input = 0  # 0=first stream, 1=emit punctuation, 2=second stream

    def __next__(self):
        """Pull from first stream (wrapped in CatEvA), then CatPunc, then second stream (unwrapped)."""
        if self.current_input == 0:
            try:
                val = next(self.input_streams[0])
                if val is None:
                    return None
                return CatEvA(val)
            except StopIteration:
                self.current_input = 1
                return CatPunc()
        elif self.current_input == 1:
            self.current_input = 2
            val = next(self.input_streams[1])
            return val  # Unwrapped (including None skips)
        else:  # current_input == 2
            val = next(self.input_streams[1])
            return val  # Unwrapped (including None skips)

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.current_input = 0
        for stream in self.input_streams:
            stream.reset()


class CatProj(StreamOp):
    """Projection from a TyCat stream."""
    def __init__(self, id, input_stream, stream_type, position):
        super().__init__(id, {id}, stream_type)
        self.input_stream = input_stream
        self.position = position  # 1 or 2
        self.seen_punc = False  # For position 2, track if we've seen CatPunc

    def __str__(self):
        return f"CatProj{self.position}({self.stream_type})"

    def __next__(self):
        event = next(self.input_stream)

        if self.position == 1:
            # Position 1: return CatEvA values, stop at CatPunc
            if isinstance(event, CatEvA):
                return event.value
            elif isinstance(event, CatPunc):
                raise StopIteration
            else:
                return None
        else:
            if isinstance(event, CatEvA):
                return None
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                return None  # Skip the punc itself
            elif event is None:
                return None  # Propagate skip without changing state
            else:
                # After CatPunc, events are unwrapped
                # If we see unwrapped value before punc, position 1 must have consumed the punc
                if not self.seen_punc:
                    self.seen_punc = True
                return event  # Return unwrapped value

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.seen_punc = False
        self.input_stream.reset()


class ParR(StreamOp):
    """Parallel composition (right)."""
    def __init__(self, id, s1, s2, vars, stream_type):
        super().__init__(id, vars, stream_type)
        self.input_streams = [s1, s2]
        self.next_choice = 0  # Alternate between 0 and 1

    def __next__(self):
        """Non-deterministically choose an input stream and pull from it, wrapping in ParEvA or ParEvB."""
        # Simple alternating strategy (could be random instead)
        choice = self.next_choice
        self.next_choice = 1 - self.next_choice  # Alternate

        val = next(self.input_streams[choice])
        if val is None:
            return None
        if choice == 0:
            return ParEvA(val)
        else:
            return ParEvB(val)

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.next_choice = 0
        for stream in self.input_streams:
            stream.reset()


class ParLCoordinator(StreamOp):
    """Coordinator for parl that manages buffering between two ParProj instances."""
    def __init__(self, id, input_stream, vars, stream_type):
        super().__init__(id, vars, stream_type)
        self.input_stream = input_stream
        self.buffer_1 = []  # Buffer for position 1 (ParEvA events)
        self.buffer_2 = []  # Buffer for position 2 (ParEvB events)
        self.exhausted = False

    def pull_for_position(self, position):
        """
        Pull the next event for the given position.

        Returns the unwrapped value if event matches position, None if should skip,
        or raises StopIteration when input is exhausted.
        """
        buffer = self.buffer_1 if position == 1 else self.buffer_2

        # Check buffer first
        if buffer:
            return buffer.pop(0)

        # Buffer empty, need to pull from input
        if self.exhausted:
            raise StopIteration

        try:
            event = next(self.input_stream)
        except StopIteration:
            self.exhausted = True
            raise

        # Route the event
        if isinstance(event, ParEvA):
            if position == 1:
                return event.value
            else:
                # Buffer for position 1, return None for position 2
                self.buffer_1.append(event.value)
                return None
        elif isinstance(event, ParEvB):
            if position == 2:
                return event.value
            else:
                # Buffer for position 2, return None for position 1
                self.buffer_2.append(event.value)
                return None
        else:
            return None  # Unknown event type

    def __next__(self):
        """Coordinators are not directly iterable."""
        raise NotImplementedError("ParLCoordinator should not be iterated directly")

    def reset(self):
        """Reset coordinator state."""
        self.buffer_1.clear()
        self.buffer_2.clear()
        self.exhausted = False
        self.input_stream.reset()


class ParProj(StreamOp):
    """Projection from a TyPar stream."""
    def __init__(self, id, coordinator, stream_type, position):
        super().__init__(id, {id}, stream_type)
        self.coordinator = coordinator  # ParLCoordinator instance
        self.position = position  # 1 or 2

    def __str__(self):
        return f"ParProj{self.position}({self.stream_type})"

    def __next__(self):
        return self.coordinator.pull_for_position(self.position)

    def reset(self):
        """Reset is handled by the coordinator."""
        pass  # Coordinator manages the state


class SumInj(StreamOp):
    """Sum injection - emits PlusPuncA (position=0) or PlusPuncB (position=1) tag followed by input stream values."""
    def __init__(self, id, input_stream, vars, stream_type, position):
        super().__init__(id, vars, stream_type)
        self.input_stream = input_stream
        self.position = position  # 0 for left (PlusPuncA), 1 for right (PlusPuncB)
        self.tag_emitted = False

    def __next__(self):
        """Emit tag first (PlusPuncA if position=0, PlusPuncB if position=1), then pull from input stream."""
        if not self.tag_emitted:
            self.tag_emitted = True
            return PlusPuncA() if self.position == 0 else PlusPuncB()
        return next(self.input_stream)

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.tag_emitted = False
        self.input_stream.reset()


class CaseOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, id, input_stream, left_branch, right_branch, left_var, right_var, vars, stream_type):
        super().__init__(id, vars, stream_type)
        self.input_stream = input_stream
        self.left_branch = left_branch  # StreamOp that produces output
        self.right_branch = right_branch  # StreamOp that produces output
        self.left_var = left_var  # Var node in left branch
        self.right_var = right_var  # Var node in right branch
        self.active_branch = None  # Will be set to left_branch or right_branch after reading tag
        self.tag_read = False

    def __next__(self):
        """Read tag and route to appropriate branch."""
        if not self.tag_read:
            tag = next(self.input_stream)
            if tag is None:
                return None
            self.tag_read = True

            if isinstance(tag, PlusPuncA):
                self.active_branch = self.left_branch
                self.left_var.source = self.input_stream
            elif isinstance(tag, PlusPuncB):
                self.active_branch = self.right_branch
                self.right_var.source = self.input_stream
            else:
                raise RuntimeError(f"Expected PlusPuncA or PlusPuncB tag, got {tag}")
            return None

        return next(self.active_branch)

    def reset(self):
        """Reset state and recursively reset branches."""
        self.tag_read = False
        self.active_branch = None
        self.left_var.source = None
        self.right_var.source = None
        self.input_stream.reset()
        self.left_branch.reset()
        self.right_branch.reset()


class RecCall(StreamOp):
    """Recursive call - executes a compiled function at runtime with input streams."""
    def __init__(self, id, compiled_func, input_streams, vars, stream_type):
        super().__init__(id, vars, stream_type)
        self.compiled_func = compiled_func  # CompiledFunction to call
        self.input_streams = input_streams  # List of input StreamOps
        self.output = None  # Will be set after calling the function

    def __next__(self):
        """Execute the recursive call and pull from its output."""
        if self.output is None:
            # First call: execute the compiled function with input streams
            self.output = self.compiled_func.run(*self.input_streams)
        return next(self.output)

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.output = None
        for stream in self.input_streams:
            stream.reset()


