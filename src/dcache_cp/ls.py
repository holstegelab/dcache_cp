#!/usr/bin/env python3
"""dcache_ls — list dCache directory contents like ``ls``.

Usage::

    dcache_ls dcache:/data/
    dcache_ls -l analysis:/archive/run42/
    dcache_ls -lh --pin dcache:/data/
    dcache_ls -R dcache:/data/

The remote prefix (e.g. ``dcache:``) selects the config file
following the same conventions as ``dcache_cp``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .cli import (
    MACAROON_DIR,
    _default_ada,
    format_bytes,
    parse_remote_prefix,
    resolve_config_for_prefix,
    load_rclone_config,
    resolve_remote_name,
    resolve_api_url,
    run_command,
)

# ---------------------------------------------------------------------------
# ANSI colors  (reuse pattern from cli.py, but keyed on stdout for ls)
# ---------------------------------------------------------------------------

class _C:
    RESET = BOLD = DIM = ""
    RED = GREEN = YELLOW = BLUE = CYAN = MAGENTA = WHITE = ""

    @classmethod
    def init(cls, stream=None):
        stream = stream or sys.stdout
        if hasattr(stream, "isatty") and stream.isatty() and os.environ.get("NO_COLOR") is None:
            cls.RESET   = "\033[0m"
            cls.BOLD    = "\033[1m"
            cls.DIM     = "\033[2m"
            cls.RED     = "\033[31m"
            cls.GREEN   = "\033[32m"
            cls.YELLOW  = "\033[33m"
            cls.BLUE    = "\033[34m"
            cls.MAGENTA = "\033[35m"
            cls.CYAN    = "\033[36m"
            cls.WHITE   = "\033[37m"


# ---------------------------------------------------------------------------
# Pin / locality helpers (ported from ada_ls.py and enhanced)
# ---------------------------------------------------------------------------

_PIN_KEY_RE = re.compile(
    r"(?i)(?:\bpin\b|sticky).*(?:expir|expire|until|valid|life)"
    r"|(?:expir|expire|until|valid).*(?:\bpin\b|sticky)"
)


def _parse_epoch(value: object) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v <= 0:
            return None
        if v >= 1e17:
            ts = v / 1e9
        elif v >= 1e14:
            ts = v / 1e6
        elif v >= 1e11:
            ts = v / 1e3
        elif v >= 1e9:
            ts = v
        else:
            return None
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return _parse_epoch(int(s))
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _walk_for_pin_times(obj: object) -> list[datetime]:
    out: list[datetime] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and _PIN_KEY_RE.search(k):
                dt = _parse_epoch(v)
                if dt is not None:
                    out.append(dt)
            out.extend(_walk_for_pin_times(v))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(_walk_for_pin_times(v))
    return out


def _fmt_timedelta(seconds: int) -> str:
    if seconds < 0:
        return "expired"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, sec = divmod(rem, 60)
    if days > 0:
        return f"{days}d{hours:02}h"
    if hours > 0:
        return f"{hours}h{minutes:02}m"
    if minutes > 0:
        return f"{minutes}m{sec:02}s"
    return f"{sec}s"


def _extract_pin_display(entry: dict) -> str:
    for k in ("pinLifetime", "pin_lifetime", "stickyLifetime", "sticky_lifetime"):
        if k in entry and isinstance(entry[k], (str, int, float)):
            return str(entry[k])
    times = _walk_for_pin_times(entry)
    if not times:
        return "-"
    now = datetime.now(tz=timezone.utc)
    future = sorted(t for t in times if t >= now)
    if future:
        return _fmt_timedelta(int((future[0] - now).total_seconds()))
    latest = max(times)
    return _fmt_timedelta(int((latest - now).total_seconds()))


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _format_mtime(value: object) -> str:
    dt = _parse_epoch(value)
    if dt is None:
        return "-"
    now = datetime.now(tz=timezone.utc)
    if (now - dt).days > 180:
        return dt.strftime("%b %d  %Y")
    return dt.strftime("%b %d %H:%M")


def _filemode(file_type: str | None, mode_value: object) -> str:
    if not isinstance(mode_value, int):
        mode_value = 0
    if file_type == "DIR":
        return stat.filemode(stat.S_IFDIR | mode_value)
    if file_type == "LINK":
        return stat.filemode(stat.S_IFLNK | mode_value)
    return stat.filemode(stat.S_IFREG | mode_value)


def _locality_color(loc: str) -> str:
    up = loc.upper()
    if "ONLINE" in up and "NEARLINE" in up:
        return _C.GREEN  # ONLINE_AND_NEARLINE — best of both
    if "ONLINE" in up:
        return _C.GREEN
    if "NEARLINE" in up:
        return _C.YELLOW
    if "UNAVAILABLE" in up:
        return _C.RED
    return ""


def _name_color(file_type: str | None) -> str:
    if file_type == "DIR":
        return _C.BOLD + _C.BLUE
    if file_type == "LINK":
        return _C.CYAN
    return ""


def _format_size(size: int | str, human: bool) -> str:
    if isinstance(size, str):
        try:
            size = int(size)
        except ValueError:
            return str(size)
    if human:
        return format_bytes(size)
    return str(size)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class _Row:
    mode: str = ""
    nlink: str = "1"
    owner: str = "-"
    group: str = "-"
    size: str = "0"
    mtime: str = "-"
    name: str = ""
    name_color: str = ""
    # optional columns
    locality: str = ""
    locality_color: str = ""
    pin: str = ""
    qos: str = ""
    checksum: str = ""
    is_dir: bool = False


def _render_long(rows: list[_Row], *, show_pin: bool, show_locality: bool, show_checksum: bool):
    if not rows:
        return

    # Compute column widths
    w_mode = max(len(r.mode) for r in rows)
    w_nlink = max(len(r.nlink) for r in rows)
    w_owner = max(len(r.owner) for r in rows)
    w_group = max(len(r.group) for r in rows)
    w_size = max(len(r.size) for r in rows)
    w_mtime = max(len(r.mtime) for r in rows)

    extra_hdrs: list[tuple[str, int]] = []
    if show_locality:
        w_loc = max((len(r.locality) for r in rows), default=8)
        w_loc = max(w_loc, 8)
        extra_hdrs.append(("locality", w_loc))
    if show_pin:
        w_pin = max((len(r.pin) for r in rows), default=3)
        w_pin = max(w_pin, 3)
        extra_hdrs.append(("pin", w_pin))
    if show_checksum:
        w_cksum = max((len(r.checksum) for r in rows), default=8)
        w_cksum = max(w_cksum, 8)
        extra_hdrs.append(("checksum", w_cksum))

    for r in rows:
        parts = [
            f"{r.mode:<{w_mode}}",
            f"{r.nlink:>{w_nlink}}",
            f"{r.owner:>{w_owner}}",
            f"{r.group:>{w_group}}",
            f"{r.size:>{w_size}}",
            f"{r.mtime:<{w_mtime}}",
        ]
        if show_locality:
            w = extra_hdrs[0][1] if extra_hdrs and extra_hdrs[0][0] == "locality" else 8
            loc_text = f"{r.locality_color}{r.locality:<{w}}{_C.RESET}" if r.locality_color else f"{r.locality:<{w}}"
            parts.append(loc_text)
        if show_pin:
            idx = next((i for i, (n, _) in enumerate(extra_hdrs) if n == "pin"), -1)
            w = extra_hdrs[idx][1] if idx >= 0 else 3
            parts.append(f"{r.pin:>{w}}")
        if show_checksum:
            idx = next((i for i, (n, _) in enumerate(extra_hdrs) if n == "checksum"), -1)
            w = extra_hdrs[idx][1] if idx >= 0 else 8
            parts.append(f"{r.checksum:<{w}}")

        name_display = f"{r.name_color}{r.name}{_C.RESET}" if r.name_color else r.name
        print(" ".join(parts) + " " + name_display)


def _render_short(rows: list[_Row]):
    if not rows:
        return
    names = []
    for r in rows:
        if r.name_color:
            names.append(f"{r.name_color}{r.name}{_C.RESET}")
        else:
            names.append(r.name)
    # Simple multi-column output like ls; fall back to one per line if not tty
    if sys.stdout.isatty():
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 80
        max_name = max(len(r.name) for r in rows) + 2
        ncols = max(cols // max_name, 1)
        for i in range(0, len(names), ncols):
            chunk = names[i:i + ncols]
            # Pad with raw name lengths (ignoring ANSI)
            padded = []
            for j, n in enumerate(chunk):
                raw_len = len(rows[i + j].name)
                pad = max_name - raw_len
                padded.append(n + " " * pad)
            print("".join(padded).rstrip())
    else:
        for n in names:
            print(n)


# ---------------------------------------------------------------------------
# Listing via ada --stat  (single-item or directory children)
# ---------------------------------------------------------------------------

def _ada_stat(ada_cmd: str, tokenfile: Path, api: str | None, remote_path: str) -> dict:
    cmd = [ada_cmd, "--tokenfile", str(tokenfile)]
    if api:
        cmd += ["--api", api]
    cmd += ["--stat", "/" + remote_path.strip("/")]
    result = run_command(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"ada --stat failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(
            f"ada --stat returned non-JSON output (old ada version?).\n"
            f"Output: {result.stdout[:200]!r}\n"
            f"Upgrade ada or set --ada to point to the bundled version."
        )


def _ada_checksum(ada_cmd: str, tokenfile: Path, api: str | None, remote_path: str) -> str:
    cmd = [ada_cmd, "--tokenfile", str(tokenfile)]
    if api:
        cmd += ["--api", api]
    cmd += ["--checksum", "/" + remote_path.strip("/")]
    result = run_command(cmd, check=False)
    if result.returncode != 0:
        return "-"
    for token in result.stdout.strip().split():
        if "=" in token:
            key, val = token.split("=", 1)
            if key.lower().startswith("adler"):
                return val.strip()
    return result.stdout.strip()[:16] or "-"


def _list_path(
    ada_cmd: str,
    tokenfile: Path,
    api: str | None,
    remote_path: str,
    *,
    human: bool,
    show_pin: bool,
    show_checksum: bool,
) -> list[_Row]:
    """List a single remote path. Returns rows for rendering."""
    data = _ada_stat(ada_cmd, tokenfile, api, remote_path)

    entries: list[dict]
    if isinstance(data, dict) and isinstance(data.get("children"), list):
        entries = [e for e in data["children"] if isinstance(e, dict)]
    elif isinstance(data, dict):
        entries = [data]
    else:
        raise TypeError(f"unexpected JSON from ada --stat: {type(data)}")

    rows: list[_Row] = []
    for e in entries:
        file_type = e.get("fileType")
        name = e.get("fileName")
        if not isinstance(name, str):
            name = remote_path.rstrip("/").rsplit("/", 1)[-1] or remote_path

        if file_type == "DIR" and not name.endswith("/"):
            name += "/"

        cksum = ""
        if show_checksum and file_type != "DIR":
            full_path = remote_path.rstrip("/") + "/" + name.rstrip("/") if len(entries) > 1 else remote_path
            cksum = _ada_checksum(ada_cmd, tokenfile, api, full_path)

        locality = str(e.get("fileLocality", "-")) if file_type != "DIR" else ""

        rows.append(_Row(
            mode=_filemode(file_type if isinstance(file_type, str) else None, e.get("mode")),
            nlink=str(e.get("nlink", 1)),
            owner=str(e.get("owner", "-")),
            group=str(e.get("group", "-")),
            size=_format_size(e.get("size", 0), human),
            mtime=_format_mtime(e.get("mtime")),
            name=name,
            name_color=_name_color(file_type),
            locality=locality,
            locality_color=_locality_color(locality),
            pin=_extract_pin_display(e) if show_pin else "",
            qos=str(e.get("currentQos", "")),
            checksum=cksum,
            is_dir=file_type == "DIR",
        ))

    rows.sort(key=lambda r: (not r.is_dir, r.name.lower()))
    return rows


def _list_recursive(
    ada_cmd: str,
    tokenfile: Path,
    api: str | None,
    remote_path: str,
    *,
    human: bool,
    show_pin: bool,
    show_checksum: bool,
    long_format: bool,
    show_locality: bool,
    _first: bool = True,
) -> None:
    """Recursively list a directory tree."""
    rows = _list_path(
        ada_cmd, tokenfile, api, remote_path,
        human=human, show_pin=show_pin, show_checksum=show_checksum,
    )

    if not _first:
        print()
    path_display = "/" + remote_path.strip("/")
    print(f"{_C.BOLD}{path_display}:{_C.RESET}")

    if long_format:
        _render_long(rows, show_pin=show_pin, show_locality=show_locality, show_checksum=show_checksum)
    else:
        _render_short(rows)

    # Recurse into subdirectories
    for r in rows:
        if r.is_dir:
            subpath = remote_path.rstrip("/") + "/" + r.name.rstrip("/")
            _list_recursive(
                ada_cmd, tokenfile, api, subpath,
                human=human, show_pin=show_pin, show_checksum=show_checksum,
                long_format=long_format, show_locality=show_locality, _first=False,
            )


# ---------------------------------------------------------------------------
# Summary line (total size, file count, online/nearline breakdown)
# ---------------------------------------------------------------------------

def _summary_line(rows: list[_Row]) -> str:
    files = [r for r in rows if not r.is_dir]
    dirs = [r for r in rows if r.is_dir]
    n_online = sum(1 for r in files if "ONLINE" in r.locality.upper())
    n_nearline = sum(1 for r in files if "NEARLINE" in r.locality.upper() and "ONLINE" not in r.locality.upper())
    parts = [f"{len(files)} file(s), {len(dirs)} dir(s)"]
    if n_online or n_nearline:
        parts.append(
            f"{_C.GREEN}{n_online} online{_C.RESET}, "
            f"{_C.YELLOW}{n_nearline} nearline{_C.RESET}"
        )
    return "  ".join(parts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dcache_ls",
        description=(
            "List dCache directory contents.\n\n"
            "Use a remote prefix (e.g. dcache: or analysis:) to identify\n"
            "the dCache path. Config resolution follows dcache_cp conventions."
        ),
        epilog=(
            "examples:\n"
            "  dcache_ls dcache:/data/\n"
            "  dcache_ls -lh dcache:/data/\n"
            "  dcache_ls -l --pin analysis:/archive/run42/\n"
            "  dcache_ls -lR --checksum dcache:/\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("path", help="Remote path to list (prefix with <remote>:)")
    p.add_argument("-l", "--long", action="store_true",
                   help="Long listing format (permissions, owner, size, date)")
    p.add_argument("-H", "--human-readable", action="store_true",
                   help="Print sizes in human-readable format (e.g. 1.5GiB)")
    p.add_argument("-R", "--recursive", action="store_true",
                   help="List directories recursively")
    p.add_argument("--pin", action="store_true",
                   help="Show pin/staging lifetime column")
    p.add_argument("--locality", action="store_true", default=None,
                   help="Show file locality (ONLINE/NEARLINE) column (default with -l)")
    p.add_argument("--no-locality", dest="locality", action="store_false",
                   help="Hide file locality column")
    p.add_argument("--checksum", action="store_true",
                   help="Show Adler-32 checksum column (slow — one API call per file)")
    p.add_argument(
        "--config", "--rclone-config", dest="config", type=Path,
        help="rclone config file override",
    )
    p.add_argument("--remote", default=os.environ.get("RCLONE_REMOTE"),
                   help="rclone remote name (default: only section in config)")
    p.add_argument("--ada", default=_default_ada(),
                   help="ada executable (default: bundled or $ADA)")
    p.add_argument("--api", help="dCache API URL override")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    _C.init(sys.stdout)

    # Parse remote prefix
    prefix, bare_path = parse_remote_prefix(args.path)
    if not prefix:
        print(f"error: path must have a remote prefix (e.g. dcache:{args.path})", file=sys.stderr)
        return 1

    # Resolve config
    rclone_config = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(rclone_config)
    remote = resolve_remote_name(config, args.remote)
    api = resolve_api_url(args.api, config[remote])

    # Defaults: show locality in long mode
    show_locality = args.locality if args.locality is not None else args.long

    remote_path = bare_path.strip("/")
    if not remote_path:
        remote_path = "/"

    try:
        if args.recursive:
            _list_recursive(
                args.ada, rclone_config, api, remote_path,
                human=args.human_readable,
                show_pin=args.pin,
                show_checksum=args.checksum,
                long_format=args.long,
                show_locality=show_locality,
            )
        else:
            rows = _list_path(
                args.ada, rclone_config, api, remote_path,
                human=args.human_readable,
                show_pin=args.pin,
                show_checksum=args.checksum,
            )

            if args.long:
                _render_long(
                    rows,
                    show_pin=args.pin,
                    show_locality=show_locality,
                    show_checksum=args.checksum,
                )
            else:
                _render_short(rows)

            # Print summary with locality breakdown
            if args.long and rows:
                print(f"{_C.DIM}{_summary_line(rows)}{_C.RESET}")
    except subprocess.CalledProcessError as exc:
        print(f"error: ada command failed (exit {exc.returncode})", file=sys.stderr)
        if exc.stderr:
            print(exc.stderr.strip(), file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


def entry_point():
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    entry_point()
