#!/usr/bin/env python3
"""Greedily balance Go test packages across deterministic CI shards."""

import argparse
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, required=True, help="1-based shard index")
    parser.add_argument("--count", type=int, required=True)
    args = parser.parse_args()
    if args.count < 1 or not 1 <= args.shard <= args.count:
        parser.error("shard must be in [1,count]")

    listing = subprocess.run(
        ["go", "list", "-f", "{{.ImportPath}} {{len .TestGoFiles}} {{len .XTestGoFiles}}", "./internal/...", "./cmd/..."],
        check=True,
        text=True,
        capture_output=True,
    ).stdout
    packages = []
    for line in listing.splitlines():
        fields = line.split()
        if len(fields) != 3:
            print(f"unexpected go list row: {line!r}", file=sys.stderr)
            return 2
        package, internal_tests, external_tests = fields
        weight = max(1, int(internal_tests) + int(external_tests))
        packages.append((package, weight))
    if not packages:
        print("go list returned no packages", file=sys.stderr)
        return 2

    buckets = [[] for _ in range(args.count)]
    loads = [0] * args.count
    for package, weight in sorted(packages, key=lambda item: (-item[1], item[0])):
        target = min(range(args.count), key=lambda index: (loads[index], index))
        buckets[target].append(package)
        loads[target] += weight
    for package in sorted(buckets[args.shard - 1]):
        print(package)
    print(f"shard {args.shard}/{args.count}: {len(buckets[args.shard - 1])} packages, weight {loads[args.shard - 1]}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
