"""
Dependency Graph — builds a directed graph of view→view dependencies,
performs topological sort, and detects cycles.
"""

import logging
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple

from migration_engine.parser.ast_analyzer import StructuralMetrics

logger = logging.getLogger(__name__)


class CyclicDependencyError(Exception):
    """Raised when a cycle is detected in the dependency graph."""
    pass


def build_dependency_graph(
    view_metrics: Dict[str, StructuralMetrics],
) -> Dict[str, Set[str]]:
    """
    Build a dependency graph where edges represent view→view dependencies.

    A view A depends on view B if A references B as a table in its SQL.

    Args:
        view_metrics: Dict of view_name → StructuralMetrics

    Returns:
        Dict of view_name → set of dependency view names
    """
    all_view_names = set(view_metrics.keys())
    graph: Dict[str, Set[str]] = defaultdict(set)

    for view_name, metrics in view_metrics.items():
        for table in metrics.tables:
            table_upper = table.upper()
            if table_upper in all_view_names and table_upper != view_name:
                graph[view_name].add(table_upper)

    # Ensure all views are in the graph (even those with no dependencies)
    for view_name in all_view_names:
        if view_name not in graph:
            graph[view_name] = set()

    dep_count = sum(len(deps) for deps in graph.values())
    logger.info(
        "Dependency graph: %d views, %d edges",
        len(graph), dep_count,
    )

    return dict(graph)


def topological_sort(graph: Dict[str, Set[str]]) -> Tuple[List[str], List[Set[str]]]:
    """
    Perform topological sort on the dependency graph (Kahn's algorithm).

    Returns:
        (sorted_views, cycles)
        - sorted_views: views in order they should be migrated
        - cycles: list of sets of view names involved in cycles (empty if no cycles)
    """
    # Compute in-degrees
    in_degree: Dict[str, int] = {node: 0 for node in graph}
    for node, deps in graph.items():
        for dep in deps:
            if dep in in_degree:
                in_degree[dep] = in_degree.get(dep, 0)  # Already initialized
            # in_degree for the dependency target gets incremented
            # Wait - we need to reverse: if A depends on B, then B must come first.
            # Edge: A → B means "A depends on B"
            # For topological sort: B must come before A
            pass

    # Actually rebuild as adjacency list for topological sort
    # Edge direction: dependency → dependent (B → A means "B must come before A")
    reverse_graph: Dict[str, Set[str]] = defaultdict(set)
    in_deg: Dict[str, int] = {node: 0 for node in graph}

    for dependent, dependencies in graph.items():
        in_deg[dependent] = len(dependencies)
        for dep in dependencies:
            if dep in graph:  # Only count known views
                reverse_graph[dep].add(dependent)

    # Kahn's algorithm
    queue = deque([node for node, deg in in_deg.items() if deg == 0])
    sorted_views: List[str] = []

    while queue:
        node = queue.popleft()
        sorted_views.append(node)

        for dependent in reverse_graph.get(node, set()):
            in_deg[dependent] -= 1
            if in_deg[dependent] == 0:
                queue.append(dependent)

    # Detect cycles
    cycles: List[Set[str]] = []
    remaining = {node for node, deg in in_deg.items() if deg > 0}

    if remaining:
        logger.warning("Cycle detected involving %d views: %s", len(remaining), remaining)
        # Group into connected components for cycle reporting
        cycles = _find_cycle_components(remaining, graph)
        # Add remaining views at the end (they'll be marked PENDING_REVIEW)
        sorted_views.extend(remaining)

    logger.info(
        "Topological sort: %d sorted, %d in cycles",
        len(sorted_views) - len(remaining), len(remaining),
    )

    return sorted_views, cycles


def _find_cycle_components(nodes: Set[str], graph: Dict[str, Set[str]]) -> List[Set[str]]:
    """Find connected components among the cycle nodes."""
    visited: Set[str] = set()
    components: List[Set[str]] = []

    for node in nodes:
        if node in visited:
            continue
        component: Set[str] = set()
        stack = [node]
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            component.add(n)
            for dep in graph.get(n, set()):
                if dep in nodes and dep not in visited:
                    stack.append(dep)
        if component:
            components.append(component)

    return components
