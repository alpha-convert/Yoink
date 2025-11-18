"""Stream operations - each operation in its own file for maintainability."""

from python_delta.stream_ops.base import DONE, Done, StreamOp
from python_delta.stream_ops.var import Var
from python_delta.stream_ops.eps import Eps
from python_delta.stream_ops.catr import CatR, CatRState
from python_delta.stream_ops.catproj import CatProj, CatProjCoordinator
from python_delta.stream_ops.suminj import SumInj
from python_delta.stream_ops.caseop import CaseOp
from python_delta.stream_ops.sinkthen import SinkThen
from python_delta.stream_ops.rec_call import RecCall
from python_delta.stream_ops.unsafecast import UnsafeCast
from python_delta.stream_ops.singletonop import SingletonOp
from python_delta.stream_ops.waitop import WaitOp
from python_delta.stream_ops.typed_buffer import TypedBuffer, make_typed_buffer
from python_delta.stream_ops.bufferop import BufferOp, SourceBuffer, ConstantOp
from python_delta.stream_ops.emitop import EmitOp
from python_delta.stream_ops.condop import CondOp
from python_delta.stream_ops.recursive_section import RecursiveSection

__all__ = [
    'DONE',
    'Done',
    'CatRState',
    'StreamOp',
    'Var',
    'Eps',
    'CatR',
    'CatProj',
    'CatProjCoordinator',
    'SumInj',
    'CaseOp',
    'SinkThen',
    'RecCall',
    'UnsafeCast',
    'SingletonOp',
    'WaitOp',
    'TypedBuffer',
    'make_typed_buffer',
    'BufferOp',
    'ConstantOp',
    'SourceBuffer',
    'EmitOp',
    'CondOp',
    'RecursiveSection',
]
