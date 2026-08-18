#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("name")
    parser.add_argument("value")
    args = parser.parse_args()
    path = Path(args.path)
    lines = path.read_text(encoding="utf-8").splitlines()
    prefix = f"{args.name}="
    output: list[str] = []
    replaced = False
    for line in lines:
        if line.startswith(prefix):
            if not replaced:
                output.append(prefix + args.value)
                replaced = True
            continue
        output.append(line)
    if not replaced:
        output.append(prefix + args.value)
    path.write_text("\n".join(output) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
