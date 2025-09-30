# Event wrapper classes for stream elements
class CatEvA:
    """Event from left side of concatenation."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"CatEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, CatEvA) and self.value == other.value

class CatPunc:
    """Punctuation marker between A and B in concatenation."""
    def __repr__(self):
        return "CatPunc"
    def __eq__(self, other):
        return isinstance(other, CatPunc)

class ParEvA:
    """Event from left side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvA({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvA) and self.value == other.value

class ParEvB:
    """Event from right side of parallel composition."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"ParEvB({self.value})"
    def __eq__(self, other):
        return isinstance(other, ParEvB) and self.value == other.value


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

    def __next__(self):
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
                return CatEvA(val)
            except StopIteration:
                self.current_input = 1
                return CatPunc()
        elif self.current_input == 1:
            self.current_input = 2
            val = next(self.input_streams[1])
            return val  # Unwrapped
        else:  # current_input == 2
            val = next(self.input_streams[1])
            return val  # Unwrapped

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.current_input = 0
        for stream in self.input_streams:
            stream.reset()


# TODO: this might be kinda inefficient: in position 2, you can just pull from the input forever
class CatLCoordinator(StreamOp):
    """Coordinator for catl that manages shared state between two CatProj instances."""
    def __init__(self, id, input_stream, vars, stream_type):
        super().__init__(id, vars, stream_type)
        self.input_stream = input_stream
        self.seen_punc = False
        self.exhausted = False

    def pull_for_position(self, position):
        """
        Pull the next event for the given position.

        Returns the unwrapped value if event is relevant, None if should skip,
        or raises StopIteration when done.
        """
        if self.exhausted:
            raise StopIteration

        try:
            event = next(self.input_stream)
        except StopIteration:
            self.exhausted = True
            raise

        if position == 1:
            if isinstance(event, CatEvA):
                return event.value
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                raise StopIteration
            else:
                return None  # Skip
        else:
            # Position 2: skip until CatPunc, then return unwrapped values
            if isinstance(event, CatEvA):
                return None  # Skip (shouldn't happen if sequential)
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                return None  # Skip the punc itself
            else:
                # After CatPunc, events are unwrapped
                if self.seen_punc:
                    return event  # Return unwrapped value
                else:
                    return None  # Still before punc, skip

    def __next__(self):
        """Coordinators are not directly iterable."""
        raise NotImplementedError("CatLCoordinator should not be iterated directly")

    def reset(self):
        """Reset coordinator state."""
        self.seen_punc = False
        self.exhausted = False
        self.input_stream.reset()


class CatProj(StreamOp):
    """Projection from a TyCat stream."""
    def __init__(self, id, coordinator, stream_type, position):
        super().__init__(id, {id}, stream_type)
        self.coordinator = coordinator  # CatLCoordinator instance
        self.position = position  # 1 or 2

    def __str__(self):
        return f"CatProj{self.position}({self.stream_type})"

    def __next__(self):
        """Pull next event for this position from the coordinator.

        Returns the unwrapped value if event is for our side, None if should skip,
        or raises StopIteration when done.
        """
        return self.coordinator.pull_for_position(self.position)

    def reset(self):
        """Reset is handled by the coordinator."""
        pass  # Coordinator manages the state


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
