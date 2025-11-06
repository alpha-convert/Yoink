"""SinkThen StreamOp - sink one stream then switch to another."""

from __future__ import annotations

from typing import List


from python_delta.stream_ops.base import StreamOp, DONE


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
        # NOTE jcutler: i'm having this NOT depend on the first stream... it
        # just sinks it, presumably after somebody else has already used it.
        # THis could bite me, but it's helpful for the moment.
        return self.input_streams[1].vars

    def _pull(self):
        """Pull from first stream until exhausted, then switch to second stream."""
        if not self.first_exhausted:
            # Pull from first stream and drop the value (sink it)
            val = self.input_streams[0]._pull()
            if val is DONE:
                # First stream exhausted, switch to second
                self.first_exhausted = True
                return None
            else:
                return None  # Drop all values from first stream

        # Pull from second stream
        return self.input_streams[1]._pull()

    def reset(self):
        """Reset state."""
        self.first_exhausted = False

