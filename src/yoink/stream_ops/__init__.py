"""Stream operations - each operation in its own file for maintainability."""

from yoink.stream_ops.base import DONE, Done, StreamOp
from yoink.stream_ops.var import Var
from yoink.stream_ops.eps import Eps
from yoink.stream_ops.catr import CatR, CatRState
from yoink.stream_ops.catproj import CatProj, CatProjCoordinator
from yoink.stream_ops.suminj import SumInj
from yoink.stream_ops.caseop import CaseOp
from yoink.stream_ops.sinkthen import SinkThen
from yoink.stream_ops.rec_call import RecCall
from yoink.stream_ops.unsafecast import UnsafeCast
from yoink.stream_ops.singletonop import SingletonOp
from yoink.stream_ops.waitop import WaitOp
from yoink.stream_ops.typed_buffer import TypedBuffer, make_typed_buffer
from yoink.stream_ops.bufferop import BufferOp, WaitOpBuffer, ConstantOp, RegisterBuffer
from yoink.stream_ops.emitop import EmitOp
from yoink.stream_ops.condop import CondOp
from yoink.stream_ops.recursive_section import RecursiveSection
from yoink.stream_ops.register_update_op import RegisterUpdateOp

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
    'WaitOpBuffer',
    'RegisterBuffer',
    'EmitOp',
    'CondOp',
    'RecursiveSection',
    'RegisterUpdateOp',
]
