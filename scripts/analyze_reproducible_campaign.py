#!/usr/bin/env python3
"""Backend-neutral entry point for the shared reproducible campaign analyzer."""

from __future__ import annotations

from analyze_kafka_reproducible_campaign import main


if __name__ == "__main__":
    raise SystemExit(main())
