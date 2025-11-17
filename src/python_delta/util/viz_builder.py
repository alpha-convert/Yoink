"""
Visualization builder for dataflow graphs.
"""


class VizBuilder:
    """
    Builds graphviz visualizations for DataflowGraph computation graphs.
    """

    def __init__(self, dataflow_graph):
        """
        Initialize the VizBuilder with a dataflow graph.

        Args:
            dataflow_graph: A DataflowGraph instance to visualize
        """
        self.dataflow_graph = dataflow_graph
        self.visited = set()
        self.node_labels = {}
        self.type_counters = {}  # Same as CompilationContext - StreamOp class name -> counter

    def to_graphviz(self):
        """
        Generate a graphviz DOT representation of the computation graph.

        Returns:
            str: A DOT format string suitable for rendering with graphviz
        """
        self.visited = set()
        self.node_labels = {}

        lines = ["digraph DataflowGraph {"]
        lines.append("  rankdir=BT;")  # Bottom to top (inputs at bottom, output at top)
        lines.append("  node [shape=box, style=rounded];")
        lines.append("")

        # Start from output node(s)
        if isinstance(self.dataflow_graph.outputs, (list, tuple)):
            for output in self.dataflow_graph.outputs:
                self._visit_node(output, lines)
        else:
            self._visit_node(self.dataflow_graph.outputs, lines)

        # Highlight output node(s)
        if isinstance(self.dataflow_graph.outputs, (list, tuple)):
            for output in self.dataflow_graph.outputs:
                output_label = self._get_node_label(output)
                lines.append(f'  "{output_label}" [peripheries=2];')
        else:
            output_label = self._get_node_label(self.dataflow_graph.outputs)
            lines.append(f'  "{output_label}" [peripheries=2];')

        lines.append("}")
        return "\n".join(lines)

    def _get_node_label(self, node):
        """Generate a readable label for a node, matching CompilationContext naming."""
        if node.id in self.node_labels:
            return self.node_labels[node.id]

        node_type_lower = node.__class__.__name__.lower()
        # Use unsigned hex (mask to 64-bit unsigned) to match CompilationContext
        node_id_hex = f"{node.id & 0xffffffffffffffff:x}"
        label = f"{node_type_lower}_{node_id_hex}"
        self.node_labels[node.id] = label
        return label

    def _visit_node(self, node, lines):
        """Recursively visit nodes and generate DOT nodes and edges."""
        if node.id in self.visited:
            return
        self.visited.add(node.id)

        label = self._get_node_label(node)
        node_type = node.__class__.__name__

        # Color code by type
        colors = {
            "Var": "lightblue",
            "Eps": "lightgray",
            "CatR": "lightgreen",
            "CatProj": "palegreen",
            "ParR": "lightyellow",
            "ParLCoordinator": "gold",
            "ParProj": "khaki",
            "SumInj": "lightcoral",
            "CaseOp": "salmon",
            "Nil": "lavender",
            "Cons": "plum",
            "RecCall": "orange",
            "UnsafeCast": "pink",
            "RecursiveSection": "mistyrose"
        }
        color = colors.get(node_type, "white")

        # Special labels for specific node types
        # Use the same label format as CompilationContext (lowercase)
        if hasattr(node, 'name'):  # Var
            display_label = f"{label}\\n{node.name}\\n{node.stream_type}"
        elif hasattr(node, 'position'):  # CatProj, ParProj, SumInj
            display_label = f"{label}\\npos={node.position}\\n{node.stream_type}"
        else:
            display_label = f"{label}\\n{node.stream_type}"

        lines.append(f'  "{label}" [label="{display_label}", fillcolor={color}, style="rounded,filled"];')

        # Add edges based on node type
        # Check specific node types first before generic hasattr checks
        if node_type == 'RecCall':  # RecCall has reset_set
            # # Draw dashed back-edges to all nodes in reset_set
            # for reset_node in node.reset_set:
            #     child_label = self._get_node_label(reset_node)
            #     lines.append(f'  "{label}" -> "{child_label}" [style=dashed, color=red, label="resets"];')
            #     # Only visit if not already visited (avoid infinite loops on back-edges)
            #     if reset_node.id not in self.visited:
            #         self._visit_node(reset_node, lines)
            pass
        elif node_type == 'CaseOp':  # CaseOp has special structure
            # Input stream
            child_label = self._get_node_label(node.input_stream)
            lines.append(f'  "{child_label}" -> "{label}" [label="input"];')
            self._visit_node(node.input_stream, lines)
            # Left branch
            child_label = self._get_node_label(node.branches[0])
            lines.append(f'  "{child_label}" -> "{label}" [label="evA"];')
            self._visit_node(node.branches[0], lines)
            # Right branch
            child_label = self._get_node_label(node.branches[1])
            lines.append(f'  "{child_label}" -> "{label}" [label="evB"];')
            self._visit_node(node.branches[1], lines)
        elif hasattr(node, 'head') and hasattr(node, 'tail'):  # Cons
            head_label = self._get_node_label(node.head)
            tail_label = self._get_node_label(node.tail)
            lines.append(f'  "{head_label}" -> "{label}" [label="head"];')
            lines.append(f'  "{tail_label}" -> "{label}" [label="tail"];')
            self._visit_node(node.head, lines)
            self._visit_node(node.tail, lines)
        elif hasattr(node, 'input_streams'):  # CatR, ParR, RecCall
            for i, child in enumerate(node.input_streams):
                child_label = self._get_node_label(child)
                lines.append(f'  "{child_label}" -> "{label}" [label="in{i}"];')
                self._visit_node(child, lines)
        elif hasattr(node, 'coordinator'):  # ParProj
            child_label = self._get_node_label(node.coordinator)
            lines.append(f'  "{child_label}" -> "{label}";')
            self._visit_node(node.coordinator, lines)
        elif hasattr(node, 'block_contents'):  # RecursiveSection
            child_label = self._get_node_label(node.block_contents)
            lines.append(f'  "{child_label}" -> "{label}";')
            self._visit_node(node.block_contents, lines)
        elif hasattr(node, 'input_stream'):  # CatProj, ParLCoordinator, SumInj, UnsafeCast
            child_label = self._get_node_label(node.input_stream)
            lines.append(f'  "{child_label}" -> "{label}";')
            self._visit_node(node.input_stream, lines)

    def save(self, filename):
        """
        Save the graphviz DOT representation to a file.

        Args:
            filename (str): Path to save the DOT file. If it ends with .png, .pdf, or .svg,
                          will attempt to render using graphviz (if available).

        Returns:
            str: Path to the saved file
        """
        dot_content = self.to_graphviz()
        print(dot_content)

        # Check if we need to render to an image format
        if filename.endswith(('.png', '.pdf', '.svg')):
            try:
                import subprocess
                import tempfile
                import os

                # Save DOT content to temporary file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.dot', delete=False) as f:
                    f.write(dot_content)
                    dot_file = f.name

                # Determine output format
                fmt = filename.split('.')[-1]

                # Render using dot command
                subprocess.run(['dot', f'-T{fmt}', dot_file, '-o', filename], check=True)

                # Clean up temporary file
                os.unlink(dot_file)

                return filename
            except (ImportError, subprocess.CalledProcessError, FileNotFoundError) as e:
                # Fall back to saving just the DOT file
                print(f"Warning: Could not render to {filename}: {e}")
                print("Saving as .dot file instead. Install graphviz to render images.")
                filename = filename.rsplit('.', 1)[0] + '.dot'

        # Save as DOT file
        with open(filename, 'w') as f:
            f.write(dot_content)

        return filename
