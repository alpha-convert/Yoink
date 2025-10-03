from python_delta.event import CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
from enum import Enum


class CatRState(Enum):
    """State machine for CatR operation."""
    FIRST_STREAM = 0   # Pulling from first stream (wrapped in CatEvA)
    EMIT_PUNC = 1      # Emit CatPunc separator
    SECOND_STREAM = 2  # Pulling from second stream (unwrapped)


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
    def __init__(self, name, stream_type):
        super().__init__(stream_type)
        self.name = name
        self.source = None  # Will be bound during .run()

    @property
    def id(self):
        return hash(("Var", self.name))

    @property
    def vars(self):
        return {self.id}

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
    def __init__(self, stream_type):
        super().__init__(stream_type)

    @property
    def id(self):
        return hash(("Eps", id(self)))

    @property
    def vars(self):
        return set()

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
    def __init__(self, s1, s2, stream_type):
        super().__init__(stream_type)
        self.input_streams = [s1, s2]
        self.current_state = CatRState.FIRST_STREAM

    @property
    def id(self):
        return hash(("CatR", self.input_streams[0].id, self.input_streams[1].id))

    @property
    def vars(self):
        return self.input_streams[0].vars | self.input_streams[1].vars

    def __next__(self):
        """Pull from first stream (wrapped in CatEvA), then CatPunc, then second stream (unwrapped)."""
        if self.current_state == CatRState.FIRST_STREAM:
            try:
                val = next(self.input_streams[0])
                if val is None:
                    return None
                return CatEvA(val)
            except StopIteration:
                self.current_state = CatRState.EMIT_PUNC
                return CatPunc()
        elif self.current_state == CatRState.EMIT_PUNC:
            self.current_state = CatRState.SECOND_STREAM
            val = next(self.input_streams[1])
            return val  # Unwrapped (including None skips)
        else:  # CatRState.SECOND_STREAM
            val = next(self.input_streams[1])
            return val  # Unwrapped (including None skips)

    def reset(self):
        """Reset state and recursively reset input streams."""
        self.current_state = CatRState.FIRST_STREAM

class CatProjCoordinator(StreamOp):
    """Coordinator for catl that manages shared state between two CatProj instances."""
    def __init__(self, input_stream, stream_type):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.seen_punc = False
        self.input_exhausted = False

    @property
    def id(self):
        return hash(("CatProjCoordinator", self.input_stream.id))

    @property
    def vars(self):
        return self.input_stream.vars

    def pull_for_position(self, position):
        """
        Pull the next event for the given position.

        For position 1: returns unwrapped CatEvA values until CatPunc
        For position 2: skips CatEvA events until CatPunc is seen, then returns unwrapped tail events
        """
        if self.input_exhausted:
            raise StopIteration

        if position == 1 and self.seen_punc:
            raise StopIteration

        try:
            event = next(self.input_stream)
        except StopIteration:
            self.input_exhausted = True
            raise

        if position == 1:
            # Position 1: return CatEvA values, stop at CatPunc
            if isinstance(event, CatEvA):
                return event.value
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                raise StopIteration
            elif event is None:
                return None
            else:
                # Shouldn't happen in position 1 before punc
                return None
        else:
            # Position 2: skip CatEvA events and CatPunc, return tail events
            if isinstance(event, CatEvA):
                return None  # Skip wrapped events
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                return None  # Skip the punc itself
            elif event is None:
                return None
            else:
                # After CatPunc, return unwrapped tail events
                return event

    def __next__(self):
        """Coordinators are not directly iterable."""
        raise NotImplementedError("CatProjCoordinator should not be iterated directly")

    def reset(self):
        self.seen_punc = False
        self.input_exhausted = False


class CatProj(StreamOp):
    """Projection from a TyCat stream."""
    def __init__(self, coordinator, stream_type, position):
        assert isinstance(coordinator,CatProjCoordinator)
        super().__init__(stream_type)
        self.coordinator = coordinator  # CatProjCoordinator instance
        self.position = position  # 1 or 2

    @property
    def id(self):
        return hash(("CatProj", self.coordinator.id, self.position))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"CatProj{self.position}({self.stream_type})"

    def __next__(self):
        return self.coordinator.pull_for_position(self.position)

    def reset(self):
        """Reset is handled by the coordinator."""
        pass  # Coordinator manages the state


class SumInj(StreamOp):
    """Sum injection - emits PlusPuncA (position=0) or PlusPuncB (position=1) tag followed by input stream values."""
    def __init__(self, input_stream, stream_type, position):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.position = position  # 0 for left (PlusPuncA), 1 for right (PlusPuncB)
        self.tag_emitted = False

    @property
    def id(self):
        return hash(("SumInj", self.input_stream.id, self.position))

    @property
    def vars(self):
        return self.input_stream.vars

    def __next__(self):
        """Emit tag first (PlusPuncA if position=0, PlusPuncB if position=1), then pull from input stream."""
        if not self.tag_emitted:
            self.tag_emitted = True
            return PlusPuncA() if self.position == 0 else PlusPuncB()
        return next(self.input_stream)

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.tag_emitted = False



class CaseOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, input_stream, left_branch, right_branch, stream_type):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.left_branch = left_branch  # StreamOp that produces output
        self.right_branch = right_branch  # StreamOp that produces output
        # self.left_var = left_var  # Var node in left branch
        # self.right_var = right_var  # Var node in right branch
        self.active_branch = None  # Will be set to left_branch or right_branch after reading tag
        self.tag_read = False

    @property
    def id(self):
        return hash(("CaseOp", self.input_stream.id, self.left_branch.id, self.right_branch.id))

    @property
    def vars(self):
        return self.input_stream.vars | self.left_branch.vars | self.right_branch.vars

    def __next__(self):
        """Read tag and route to appropriate branch."""
        if not self.tag_read:
            tag = next(self.input_stream)
            if tag is None:
                return None
            self.tag_read = True

            if isinstance(tag, PlusPuncA):
                self.active_branch = self.left_branch
            elif isinstance(tag, PlusPuncB):
                self.active_branch = self.right_branch
            else:
                raise RuntimeError(f"Expected PlusPuncA or PlusPuncB tag, got {tag}")
            return None

        return next(self.active_branch)

    def reset(self):
        """Reset state and recursively reset branches."""
        self.tag_read = False
        self.active_branch = None

# class MapOp(StreamOp):
#     def __init__(self,subgraph,input,stream_type):
#         super().__init__(stream_type)
#         self.subgraph = subgraph
#         self.input = input

#     def __next__(self):
        # first we pull from the input.
        # (1) if it's a pluspuncA, we send pluspuncA and stop!
        # (2) if it's a pluspuncB, we send pluspuncB, and move to "mapping" mode.
        # in mapping mode,
        # pull from the subgraph's output. the subgraph's input is a catevA-peel of the input.
        # when this raises, we send catpunc, and reset the subgraph's state


class SinkThen(StreamOp):
    """Sink operation - pulls from first stream until exhausted, then switches to second stream."""
    def __init__(self, first_stream, second_stream, stream_type):
        super().__init__(stream_type)
        self.input_streams = [first_stream, second_stream]
        self.first_exhausted = False

    @property
    def id(self):
        return hash(("SinkThen", self.input_streams[0].id, self.input_streams[1].id))

    @property
    def vars(self):
        return self.input_streams[0].vars | self.input_streams[1].vars

    def __next__(self):
        """Pull from first stream until exhausted, then switch to second stream."""
        if not self.first_exhausted:
            try:
                # Pull from first stream and drop the value (sink it)
                next(self.input_streams[0])
                return None  # Drop all values from first stream
            except StopIteration:
                # First stream exhausted, switch to second
                self.first_exhausted = True
                # Fall through to pull from second stream

        # Pull from second stream
        return next(self.input_streams[1])

    def reset(self):
        """Reset state."""
        self.first_exhausted = False


class ResetOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, reset_set, stream_type):
        super().__init__(stream_type)
        self.reset_set = reset_set

    @property
    def id(self):
        return hash(("ResetOp", *map(lambda n: id(n),self.reset_set)))

    @property
    def vars(self):
        return set()

    def __next__(self):
        for node in self.reset_set:
            node.reset()

    def reset(self):
        pass

class UnsafeCast(StreamOp):
    """Unsafe cast - forwards data from input stream with a different type annotation."""
    def __init__(self, input_stream, target_type):
        super().__init__(target_type)
        self.input_stream = input_stream

    @property
    def id(self):
        return hash(("UnsafeCast", self.input_stream.id, str(self.stream_type)))

    @property
    def vars(self):
        return self.input_stream.vars

    def __next__(self):
        """Forward data from input stream without modification."""
        return next(self.input_stream)

    def reset(self):
        pass