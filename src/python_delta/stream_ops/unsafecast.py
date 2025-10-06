"""UnsafeCast StreamOp - cast stream to different type without validation."""

from __future__ import annotations

from typing import List
import ast

from python_delta.stream_ops.base import StreamOp, DONE

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

    def _compile_stmts(self, ctx: CompilationContext, dst: str) -> List[ast.stmt]:
        """Passthrough - just compile child to same destination."""
        return self.input_stream._compile_stmts(ctx, dst)