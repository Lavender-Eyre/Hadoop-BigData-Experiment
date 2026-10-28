#!/usr/bin/env python3
"""Verify actual Hadoop/Spark results against each other and a serial oracle."""
import argparse
import math
import re
from pathlib import Path
from graph import read_graph, serial_reference


def read_result(path):
    result = {}
    pattern = re.compile(r"^\(([^,]+),([0-9]+\.[0-9]{10})\)$")
    for number, line in enumerate(Path(path).read_text(encoding="utf-8-sig").splitlines(), 1):
        if not line:
            continue
        match = pattern.fullmatch(line)
        if not match:
            raise ValueError(f"{path}:{number}: expected (page,PR with exactly 10 decimals)")
        page, rank = match.groups()
        if page in result:
            raise ValueError(f"{path}: duplicate page {page}")
        result[page] = float(rank)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data")
    parser.add_argument("mr_result")
    parser.add_argument("spark_result")
    args = parser.parse_args()
    graph = read_graph(args.data)
    expected = serial_reference(graph)
    mr, spark = read_result(args.mr_result), read_result(args.spark_result)
    if set(mr) != set(expected) or set(spark) != set(expected):
        raise AssertionError("Output page set differs from complete input graph")
    error = max(max(abs(mr[p] - spark[p]), abs(mr[p] - expected[p]), abs(spark[p] - expected[p]))
                for p in expected)
    if error > 1e-8:
        raise AssertionError(f"Maximum error {error:.3g} exceeds 1e-8")
    for name, values in [("MapReduce", mr), ("Spark", spark)]:
        total = math.fsum(values.values())
        if abs(total - len(graph)) > max(1e-8, len(graph) * 5.1e-11):
            raise AssertionError(f"{name}: rank sum {total} differs from N={len(graph)}")
    print(f"PASS: {len(graph)} pages, 10 decimal places, max error={error:.3g}")
    print(f"PASS: both implementations agree with the independent 10-iteration serial result")
    print(f"Rank sums: MapReduce={math.fsum(mr.values()):.10f}, Spark={math.fsum(spark.values()):.10f}")
    for page in sorted(expected):
        print(f"({page},{mr[page]:.10f})")


if __name__ == "__main__":
    main()
