"""Shared local input preparation and independent serial reference."""
from pathlib import Path


def read_graph(path):
    graph = {}
    with Path(path).open(encoding="utf-8-sig") as source:
        for number, raw in enumerate(source, 1):
            line = raw.rstrip("\r\n")
            if not line:
                continue
            if "\t" not in line:
                raise ValueError(f"Line {number}: expected a real TAB between page and links")
            page, links = line.split("\t", 1)
            page = page.strip()
            if not page or any(c in page for c in "\t\r\n,()"):
                raise ValueError(f"Line {number}: invalid page name {page!r}")
            targets = {target.strip() for target in links.split(",") if target.strip()}
            if any(any(c in target for c in "\t\r\n,()") for target in targets):
                raise ValueError(f"Line {number}: invalid target name")
            graph.setdefault(page, set()).update(targets)
            for target in targets:
                graph.setdefault(target, set())
    if not graph:
        raise ValueError("The graph has no pages")
    return {page: sorted(graph[page]) for page in sorted(graph)}


def serial_reference(graph, iterations=10, damping=0.85):
    ranks = {page: 1.0 for page in graph}
    n = len(graph)
    for _ in range(iterations):
        dangling = sum(ranks[page] for page, links in graph.items() if not links)
        next_ranks = {page: (1 - damping) + damping * dangling / n for page in graph}
        for page, links in graph.items():
            if links:
                share = damping * ranks[page] / len(links)
                for target in links:
                    next_ranks[target] += share
        ranks = next_ranks
    return ranks
