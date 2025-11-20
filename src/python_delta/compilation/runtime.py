from python_delta.stream_ops import DONE, CatRState
from python_delta.event import BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
from python_delta.stream_ops.typed_buffer import CatTypedBuffer, EpsTypedBuffer, PlusTypedBuffer, SingletonTypedBuffer,  make_typed_buffer
from python_delta.typecheck.types import Singleton, TyCat, TyEps, TyPlus, TyStar, Type, TypeVar

class Runtime:

    def __init__(self):
        from python_delta.stream_ops.emitop import EmitOpPhase

        self.namespace =  {
            'DONE': DONE,
            'BaseEvent': BaseEvent,
            'CatEvA': CatEvA,
            'CatPunc': CatPunc,
            'ParEvA': ParEvA,
            'ParEvB': ParEvB,
            'PlusPuncA': PlusPuncA,
            'PlusPuncB': PlusPuncB,
            'CatRState': CatRState,
            'EmitOpPhase': EmitOpPhase,
        }
    
    def exec(self,code):
        exec(code,self.namespace)
        return self.namespace['FlattenedIterator']
