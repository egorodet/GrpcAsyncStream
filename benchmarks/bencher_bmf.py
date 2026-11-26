#!/usr/bin/env python3
# Copyright (c) 2026 Evgeny Gorodetskiy
# SPDX-License-Identifier: MIT
# Licensed under the MIT License. See the LICENSE file in the project root for details.

"""Merges benchmark results into one Bencher Metric Format (BMF) file for `bencher run --adapter json`.

Catch2 XML reports (`-r xml::out=<file>`) become `latency` metrics in nanoseconds, named
"<test case>: <benchmark>" like Bencher's cpp_catch2 adapter: the value is the mean, the bounds are
the mean's confidence interval. BMF files (the heavy-load results) are merged as they are.

    bencher_bmf.py --catch2-xml catch2.xml --bmf load.json --output bmf.json
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ElementTree


def catch2_to_bmf(path: str) -> dict:
    bmf = {}
    for test_case in ElementTree.parse(path).getroot().iter("TestCase"):
        for result in test_case.iter("BenchmarkResults"):
            mean = result.find("mean")
            if mean is None:
                continue
            bmf[f"{test_case.get('name')}: {result.get('name')}"] = {
                "latency": {
                    "value": float(mean.get("value")),
                    "lower_value": float(mean.get("lowerBound")),
                    "upper_value": float(mean.get("upperBound")),
                }
            }
    return bmf


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catch2-xml", action="append", default=[], help="Catch2 XML report")
    parser.add_argument("--bmf", action="append", default=[], help="BMF file to merge")
    parser.add_argument("--output", required=True, help="merged BMF file")
    args = parser.parse_args()

    merged = {}
    sources = [catch2_to_bmf(path) for path in args.catch2_xml]
    for path in args.bmf:
        with open(path, encoding="utf-8") as file:
            sources.append(json.load(file))
    for source in sources:
        for name, measures in source.items():
            if name in merged:
                print(f"error: benchmark '{name}' is reported twice", file=sys.stderr)
                return 1
            merged[name] = measures
    if not merged:
        print("error: no benchmark results found", file=sys.stderr)
        return 1

    with open(args.output, "w", encoding="utf-8") as file:
        json.dump(merged, file, indent=2)
    print(f"{len(merged)} benchmarks written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
