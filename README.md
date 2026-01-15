# Yoink

This repository contains the implementation of the Yoink language from my (Joe Cutler) dissertation. Don't use this for anything real, it's a research prototype.

## Overview

Yoink implements a type system based on ordered stream types (concatenation, sum types, and star). The ordered types enusre that streams are consumed in a valid order. The implementation includes both an interpreter that directly implements the operational semantics, and a compiler that generates fused imperative code.

The surface syntax is a lightweight shim over writing terms directly in the core calculus. Functions are written as Python methods annotated with a `@Yoink.jit` decorator, with the signature `def f(yoink, ...): ...`. The first argument is a special object that serves as the gateway to term constructors. The rest of the arguments are stream arguments to the function, implicitly in parallel.

## Usage

Each term former has a corresponding method on the `yoink` object. For example, `yoink.inl(x)` and `yoink.cons(y, z)` correspond to the `inl` and `cons` constructors, respectively. Elimination forms work similarly: `yoink.catl(z)` implements cat-elimination and returns a tuple of the two stream components, allowing you to write `(x, y) = yoink.catl(z)` in the natural Python style. Case analysis forms are written by passing Python functions that implement the branch bodies, e.g., `yoink.case(x, lambda y: _, lambda z: _)`.

```python
from yoink.core import Yoink, Singleton, TyStar

STRING_TY = Singleton(str)

@Yoink.jit
def cat_and_split(yoink, x: STRING_TY, y: STRING_TY):
    z = yoink.catr(x, y)
    a, b = yoink.catl(z)
    return yoink.catr(a, b)
```

When you define a function decorated with `@Yoink.jit`, the decorator executes the function body with symbolic values for the inputs, tracing the evaluation to build a term. Along the way, the term is typechecked with an algorithmic form of the ordered type system. By collecting a partially ordered set of variables ordered by their usages, we can ensure that no disallowed usages occur. Type annotations can be given to top-level function arguments; these are then checked at intro/elim forms.

After typechecking completes successfully, the result is a pull graph representation of the program. Users can either run the resulting pull graph using the interpreter (which directly implements the operational semantics), or compile the pull graph to a Python iterator.

## Stream Types

| Type | Description |
|------|-------------|
| `Singleton(T)` | A single value of Python type T |
| `TyCat(A, B)` | Concatenation: stream A followed by stream B |
| `TyPlus(A, B)` | Sum type: either stream A or stream B |
| `TyStar(T)` | Kleene star: a list/sequence of elements of type T |
| `TyEps` | Empty stream |

## Operations

**Concatenation:**
- `yoink.catr(s1, s2)` - Concatenate two streams
- `yoink.catl(s)` - Split a concatenated stream into `(left, right)`

**Sum types:**
- `yoink.inl(s)` / `yoink.inr(s)` - Left/right injection
- `yoink.case(x, left_fn, right_fn)` - Case analysis on sum

**Lists:**
- `yoink.nil()` - Empty list
- `yoink.cons(head, tail)` - Prepend element to list
- `yoink.starcase(x, nil_fn, cons_fn)` - Case analysis on list

**Derived operations:**
- `yoink.map(xs, fn)` - Map function over list
- `yoink.concat(xs, ys)` - Concatenate two lists
- `yoink.concat_map(xs, fn)` - Map and flatten
- `yoink.zip_with(xs, ys, fn)` - Zip with combining function

**Buffering:**
- `yoink.wait(x)` - Buffer a value
- `yoink.emit(buffer)` - Emit buffered value

## Testing

The implementation has been tested using the Hypothesis property-based testing framework. For each test program, sequences of input events are generated and checked to ensure that the compiler and interpreter agree on the output events.

```
uv run pytest tests/
```
