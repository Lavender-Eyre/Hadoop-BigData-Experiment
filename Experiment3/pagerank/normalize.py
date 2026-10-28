#!/usr/bin/env python3
"""Prepare the same complete graph for Hadoop Streaming and Spark."""
import argparse
import json
from pathlib import Path
from graph import read_graph


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("output_dir")
    args = parser.parse_args()
    graph = read_graph(args.input)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    with (output / "normalized.tsv").open("w", encoding="utf-8", newline="\n") as normalized, \
         (output / "initial.tsv").open("w", encoding="utf-8", newline="\n") as initial:
        for page, links in graph.items():
            normalized.write(page + "\t" + ",".join(links) + "\n")
            state = {"rank": 1.0, "links": links}
            initial.write(page + "\t" + json.dumps(state, ensure_ascii=True) + "\n")
    (output / "node_count.txt").write_text(str(len(graph)) + "\n", encoding="ascii")
    print(f"Prepared {len(graph)} pages, {sum(map(len, graph.values()))} directed edges")
    print(f"Output: {output.resolve()}")


if __name__ == "__main__":
    main()
