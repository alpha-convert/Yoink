from python_delta.stream_ops import DONE, CatRState
from python_delta.event import BaseEvent, CatEvA, CatPunc, ParEvA, ParEvB, PlusPuncA, PlusPuncB
from python_delta.stream_ops.typed_buffer import CatTypedBuffer, EpsTypedBuffer, PlusTypedBuffer, SingletonTypedBuffer,  make_typed_buffer
from python_delta.typecheck.types import Singleton, TyCat, TyEps, TyPlus, TyStar, Type, TypeVar

def value_to_events(value, stream_type : Type):
    if isinstance(stream_type, TypeVar):
        assert stream_type.link is not None
        return value_to_events(value,stream_type.link)

    elif isinstance(stream_type, TyEps):
        return []

    elif isinstance(stream_type, Singleton):
        return [BaseEvent(value)]

    elif isinstance(stream_type, TyCat):
        left_val, right_val = value
        left_events = value_to_events(left_val, stream_type.left_type)
        right_events = value_to_events(right_val, stream_type.right_type)
        # Wrap left events in CatEvA, add CatPunc, then right events
        return [CatEvA(e) for e in left_events] + [CatPunc()] + right_events

    elif isinstance(stream_type, TyPlus):
        tag, tagged_val = value
        if tag == 'left':
            tagged_events = value_to_events(tagged_val, stream_type.left_type)
            return [PlusPuncA()] + tagged_events
        else:
            tagged_events = value_to_events(tagged_val, stream_type.right_type)
            return [PlusPuncB()] + tagged_events

    elif isinstance(stream_type, TyStar):
        # value is a list
        if len(value) == 0:
            return [PlusPuncA()]  # nil
        else:
            first_events = value_to_events(value[0], stream_type.element_type)
            rest_events = value_to_events(value[1:], stream_type)  # Recursive call with TyStar
            return [PlusPuncB()] + [CatEvA(e) for e in first_events] + [CatPunc()] + rest_events

    else:
        raise ValueError(f"Unknown stream type: {stream_type}")

class Runtime:
    
    def __init__(self):
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
            # 'EmitOpPhase': EmitOpPhase,
            'value_to_events': value_to_events,
        }
    
    def exec(self,code):
        exec(code,self.namespace)
        return self.namespace['FlattenedIterator']
