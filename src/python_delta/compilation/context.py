"""Compilation context for tracking state allocation during AST generation."""

from __future__ import annotations

import ast
from typing import Dict, Set


class StateVar:
    """Wrapper for a state variable with pre-built AST rvalue/lvalue nodes."""

    def __init__(self, name: str, tmp: bool = False):
        self.name = name
        self.tmp = tmp

    def rvalue(self) -> ast.expr:
        """Get AST node for reading this variable (load context)."""
        if self.tmp:
            # Temporary variable: just use the name directly
            return ast.Name(id=self.name, ctx=ast.Load())
        else:
            # State variable: access via self.name
            return ast.Attribute(
                value=ast.Name(id='self', ctx=ast.Load()),
                attr=self.name,
                ctx=ast.Load()
            )

    def lvalue(self) -> ast.expr:
        """Get AST node for writing to this variable (store context)."""
        if self.tmp:
            # Temporary variable: just use the name directly
            return ast.Name(id=self.name, ctx=ast.Store())
        else:
            # State variable: access via self.name
            return ast.Attribute(
                value=ast.Name(id='self', ctx=ast.Load()),
                attr=self.name,
                ctx=ast.Store()
            )

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"StateVar({self.name})"


class CompilationContext:
    """Tracks state allocation and compiled child destinations during compilation."""

    def __init__(self):
        self.state_vars: Dict[int, Dict[str, StateVar]] = {}  # node.id -> {var_name: StateVar}
        self.type_counters: Dict[str, int] = {}  # StreamOp class name -> counter
        self.var_to_input_idx: Dict[int, int] = {}  # Var.id -> input array index
        self.temp_counter: int = 0
        self.compiled_nodes: Set[int] = set()  # Track which nodes are compiled

    def allocate_state(self, node, var_name: str) -> StateVar:
        """
        Allocate a unique state variable for this node.
        Idempotent: returns same StateVar if called multiple times for same node+var_name.

        Example: catr_0_state, suminj_1_tag_emitted

        Args:
            node: StreamOp node to allocate state for
            var_name: Logical name of the state variable (e.g., 'state', 'tag_emitted')

        Returns:
            StateVar with pre-built load/store AST nodes
        """
        if node.id in self.state_vars and var_name in self.state_vars[node.id]:
            return self.state_vars[node.id][var_name]

        node_type = node.__class__.__name__.lower()

        if node_type not in self.type_counters:
            self.type_counters[node_type] = 0
        idx = self.type_counters[node_type]
        self.type_counters[node_type] += 1

        full_name = f'{node_type}_{idx}_{var_name}'
        state_var = StateVar(full_name)

        if node.id not in self.state_vars:
            self.state_vars[node.id] = {}
        self.state_vars[node.id][var_name] = state_var

        return state_var

    def get_state_var(self, node, var_name: str) -> StateVar:
        return self.state_vars[node.id][var_name]

    def allocate_temp(self) -> StateVar:
        name = f'tmp_{self.temp_counter}'
        self.temp_counter += 1
        return StateVar(name, tmp=True)
