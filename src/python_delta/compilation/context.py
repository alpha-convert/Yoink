"""Compilation context for tracking state allocation during AST generation."""

from __future__ import annotations

import ast
from typing import Dict, Set


class CompilationContext:
    """Tracks state allocation and compiled child destinations during compilation."""

    def __init__(self):
        self.state_vars: Dict[int, Dict[str, str]] = {}  # node.id -> {var_name: allocated_name}
        self.type_counters: Dict[str, int] = {}  # StreamOp class name -> counter
        # TODO jcutler: ?? why do we have a map of dsts here, they should be passed down in the call stack
        self.child_dsts: Dict[int, str] = {}  # node.id -> destination variable name
        self.var_to_input_idx: Dict[int, int] = {}  # Var.id -> input array index
        self.temp_counter: int = 0
        self.compiled_nodes: Set[int] = set()  # Track which nodes are compiled

    def allocate_state(self, node, var_name: str) -> str:
        """
        Allocate a unique state variable name for this node.
        Idempotent: returns same name if called multiple times for same node+var_name.

        Example: catr_0_state, suminj_1_tag_emitted

        Args:
            node: StreamOp node to allocate state for
            var_name: Logical name of the state variable (e.g., 'state', 'tag_emitted')

        Returns:
            Unique allocated state variable name (e.g., 'catr_0_state')
        """
        if node.id in self.state_vars and var_name in self.state_vars[node.id]:
            return self.state_vars[node.id][var_name]

        node_type = node.__class__.__name__.lower()

        if node_type not in self.type_counters:
            self.type_counters[node_type] = 0
        idx = self.type_counters[node_type]
        self.type_counters[node_type] += 1

        full_name = f'{node_type}_{idx}_{var_name}'

        if node.id not in self.state_vars:
            self.state_vars[node.id] = {}
        self.state_vars[node.id][var_name] = full_name

        return full_name

    def get_state_var(self, node , var_name: str) -> str:
        return self.state_vars[node.id][var_name]

    def set_child_dst(self, node, dst: str):
        self.child_dsts[node.id] = dst

    def get_child_dst(self, node) -> str:
        return self.child_dsts[node.id]

    def allocate_temp(self) -> str:
        name = f'tmp_{self.temp_counter}'
        self.temp_counter += 1
        return name
