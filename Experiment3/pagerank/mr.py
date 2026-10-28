#!/usr/bin/env python3
"""Hadoop Streaming stages; all graph computation runs inside MR tasks."""
import argparse
import itertools
import json
import math
import sys


def states():
    for raw in sys.stdin:
        if raw.strip():
            page, value = raw.rstrip("\r\n").split("\t", 1)
            yield page, json.loads(value)


def stats_map():
    # A local sum produces only one small record per mapper.
    total = math.fsum(state["rank"] for _, state in states() if not state["links"])
    print("D\t" + repr(total))


def stats_reduce():
    total = math.fsum(float(line.rstrip().split("\t", 1)[1]) for line in sys.stdin if line.strip())
    print("D\t" + repr(total))


def rank_map():
    for page, state in states():
        links = state["links"]
        # Carry adjacency even for pages with zero incoming edges.
        print(page + "\t" + json.dumps(["S", links], ensure_ascii=True))
        if links:
            share = state["rank"] / len(links)
            for target in links:
                print(target + "\t" + json.dumps(["C", share]))


def rank_reduce(nodes, dangling, final):
    def records():
        for line in sys.stdin:
            page, payload = line.rstrip("\r\n").split("\t", 1)
            yield page, json.loads(payload)

    for page, records_for_page in itertools.groupby(records(), key=lambda item: item[0]):
        links = None
        received = []
        for _, payload in records_for_page:
            if payload[0] == "S":
                if links is not None:
                    raise ValueError("Duplicate state for page " + page)
                links = payload[1]
            elif payload[0] == "C":
                received.append(float(payload[1]))
            else:
                raise ValueError("Unknown message tag")
        if links is None:
            raise ValueError("Missing normalized state for page " + page)
        rank = 0.15 + 0.85 * (math.fsum(received) + dangling / nodes)
        if final:
            print(f"({page},{rank:.10f})")
        else:
            print(page + "\t" + json.dumps({"rank": rank, "links": links}, ensure_ascii=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["stats-map", "stats-reduce", "map", "reduce"])
    parser.add_argument("--nodes", type=int)
    parser.add_argument("--dangling", type=float)
    parser.add_argument("--final", action="store_true")
    args = parser.parse_args()
    if args.stage == "stats-map":
        stats_map()
    elif args.stage == "stats-reduce":
        stats_reduce()
    elif args.stage == "map":
        rank_map()
    else:
        if args.nodes is None or args.nodes < 1 or args.dangling is None:
            parser.error("reduce requires --nodes N and --dangling MASS")
        rank_reduce(args.nodes, args.dangling, args.final)


if __name__ == "__main__":
    main()
