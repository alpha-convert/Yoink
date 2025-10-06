from python_delta.event import CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
from enum import Enum


class Done:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "Done"


DONE = Done()


class CatRState(Enum):
    """State machine for CatR operation."""
    FIRST_STREAM = 0   # Pulling from first stream (wrapped in CatEvA)
    SECOND_STREAM = 1  # Pulling from second stream (unwrapped)


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

    def _pull(self):
        """Pull the next element from the stream.

        Returns:
            - A value (including None for skips)
            - DONE sentinel when stream is exhausted
        """
        raise NotImplementedError("Subclasses must implement _pull")

    def __next__(self):
        """Pull the next element from the stream. Raise StopIteration when exhausted."""
        result = self._pull()
        if result is DONE:
            raise StopIteration
        return result

    def reset(self):
        """Reset the stream to its initial state for reuse."""
        raise NotImplementedError("Subclasses must implement reset")
    
class Var(StreamOp):
    def __init__(self, name, stream_type):
        super().__init__(stream_type)
        self.name = name
        self.source = None

    @property
    def id(self):
        return hash(("Var", self.name))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"Var({self.name}: {self.stream_type})"

    def _pull(self):
        """Pull from the source iterator."""
        if self.source is None:
            raise RuntimeError(f"Var '{self.name}' has no source bound")
        try:
            return next(self.source)
        except StopIteration:
            return DONE

    def reset(self):
        pass


class Eps(StreamOp):
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

    def _pull(self):
        return DONE

    def reset(self):
        pass

class CatR(StreamOp):
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

    def _pull(self):
        """Pull from first stream (wrapped in CatEvA), then CatPunc, then second stream (unwrapped)."""
        if self.current_state == CatRState.FIRST_STREAM:
            val = self.input_streams[0]._pull()
            if val is DONE:
                self.current_state = CatRState.SECOND_STREAM
                return CatPunc()
            if val is None:
                return None
            return CatEvA(val)
        else:
            return self.input_streams[1]._pull()

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

        For position 0: returns unwrapped CatEvA values until CatPunc
        For position 1: skips CatEvA events until CatPunc is seen, then returns unwrapped tail events
        """
        if self.input_exhausted:
            return DONE

        if position == 0 and self.seen_punc:
            return DONE

        event = self.input_stream._pull()
        if event is DONE:
            self.input_exhausted = True
            return DONE

        if position == 0:
            # Position 0: return CatEvA values, stop at CatPunc
            if isinstance(event, CatEvA):
                return event.value
            elif isinstance(event, CatPunc):
                self.seen_punc = True
                return DONE
            elif event is None:
                return None
            else:
                # Shouldn't happen in position 0 before punc
                return None
        else:
            # Position 1: skip CatEvA events and CatPunc, return tail events
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

    def _pull(self):
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
        self.position = position  # 0 or 1

    @property
    def id(self):
        return hash(("CatProj", self.coordinator.id, self.position))

    @property
    def vars(self):
        return {self.id}

    def __str__(self):
        return f"CatProj{self.position}({self.stream_type})"

    def _pull(self):
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

    def _pull(self):
        """Emit tag first (PlusPuncA if position=0, PlusPuncB if position=1), then pull from input stream."""
        if not self.tag_emitted:
            self.tag_emitted = True
            return PlusPuncA() if self.position == 0 else PlusPuncB()
        return self.input_stream._pull()

    def reset(self):
        """Reset state and recursively reset input stream."""
        self.tag_emitted = False



class CaseOp(StreamOp):
    """Case analysis on sum types - routes based on PlusPuncA/PlusPuncB tag."""
    def __init__(self, input_stream, left_branch, right_branch, stream_type):
        super().__init__(stream_type)
        self.input_stream = input_stream
        self.branches = [left_branch,right_branch] # StreamOp that produces output
        self.active_branch = -1
        self.tag_read = False

    @property
    def id(self):
        return hash(("CaseOp", self.input_stream.id, self.branches[0].id, self.branches[1].id))

    @property
    def vars(self):
        return self.input_stream.vars | self.branches[0].vars | self.branches[1].vars

    def _pull(self):
        """Read tag and route to appropriate branch."""
        if not self.tag_read:
            tag = self.input_stream._pull()
            if tag is None:
                return None
            if tag is DONE:
                return DONE
            self.tag_read = True

            if isinstance(tag, PlusPuncA):
                self.active_branch = 0
            elif isinstance(tag, PlusPuncB):
                self.active_branch = 1
            else:
                raise RuntimeError(f"Expected PlusPuncA or PlusPuncB tag, got {tag}")
            return None

        if self.active_branch == -1:
            raise RuntimeError("CaseOp._pull() called before tag was read")
        return self.branches[self.active_branch]._pull()

    def reset(self):
        """Reset state and recursively reset branches."""
        self.tag_read = False
        self.active_branch = None

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

    def _pull(self):
        """Pull from first stream until exhausted, then switch to second stream."""
        if not self.first_exhausted:
            # Pull from first stream and drop the value (sink it)
            val = self.input_streams[0]._pull()
            if val is DONE:
                # First stream exhausted, switch to second
                self.first_exhausted = True
                # Fall through to pull from second stream
            else:
                return None  # Drop all values from first stream

        # Pull from second stream
        return self.input_streams[1]._pull()

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

    def _pull(self):
        for node in self.reset_set:
            node.reset()
        return None

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

    def _pull(self):
        """Forward data from input stream without modification."""
        return self.input_stream._pull()

    def reset(self):
        pass