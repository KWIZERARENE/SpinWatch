#!/usr/bin/env python3
"""
SpinWatch - MapReduce Reducer (Heat Monitoring)
Reads sorted key-value pairs (branch, 1) from stdin and aggregates total heat alert counts per branch.
"""

import sys

def reducer():
    current_branch = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            branch, count_str = line.split("\t")
            count = int(count_str)
        except ValueError:
            continue

        if current_branch == branch:
            current_count += count
        else:
            if current_branch:
                print(f"{current_branch}\t{current_count}")
            current_branch = branch
            current_count = count

    if current_branch:
        print(f"{current_branch}\t{current_count}")

if __name__ == "__main__":
    reducer()
