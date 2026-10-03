from __future__ import annotations

import tempfile
from pathlib import Path


def get_temp_root() -> Path:
    root = Path.cwd() / "tmp"
    root.mkdir(parents=True, exist_ok=True)
    return root


def mkdtemp(*, prefix: str) -> str:
    return tempfile.mkdtemp(prefix=prefix, dir=str(get_temp_root()))


def mkstemp(*, prefix: str, suffix: str = "") -> tuple[int, str]:
    return tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=str(get_temp_root()))


def named_tempfile(*args, **kwargs):
    kwargs.setdefault("dir", str(get_temp_root()))
    return tempfile.NamedTemporaryFile(*args, **kwargs)
