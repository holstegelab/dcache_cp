#!/usr/bin/env python3
"""dcache_mv — move files to/from dCache after verified transfer."""

from __future__ import annotations

from .cli import _run_entry_point


def entry_point():
    _run_entry_point(prog="dcache_mv", delete_source=True)


if __name__ == "__main__":
    entry_point()