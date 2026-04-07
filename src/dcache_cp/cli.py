#!/usr/bin/env python3
"""dcache_cp — copy files to/from dCache with Adler-32 verification and tape staging.

Usage
-----
Upload (local → dCache)::

    dcache_cp  ./local/dir/  dcache:/data/

Download (dCache → local)::

    dcache_cp  dcache:/data/  ./local/dir/

File list::

    dcache_cp --file-list transfers.tsv

The ``dcache:`` prefix identifies the remote side.  A custom prefix
(e.g. ``analysis:``) selects ``~/macaroons/analysis.conf`` as the
rclone token/config file.
"""

from __future__ import annotations

import argparse
import configparser
import concurrent.futures
import csv
import hashlib
import json
import logging
import os
import posixpath
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zlib
from pathlib import Path

from . import __version__

LOG = logging.getLogger("dcache_cp")

DEFAULT_COPY_TIMEOUT = "300m"
DEFAULT_STAGE_TIMEOUT = 86400  # 24 h
DEFAULT_STAGE_POLL = 60  # seconds
DEFAULT_STAGE_BATCH = 50  # files staged at a time
MACAROON_DIR = Path("~/macaroons").expanduser()

_ADA_URL = (
    "https://raw.githubusercontent.com/sara-nl/SpiderScripts"
    "/refs/heads/master/ada/ada"
)
_ADA_CACHE = Path("~/.local/share/dcache_cp/ada").expanduser()
_ADA_STAMP = Path("~/.local/share/dcache_cp/.ada_checked").expanduser()
_ADA_CHECK_INTERVAL = 86400  # seconds — recheck GitHub once per day


def _update_ada() -> None:
    """Download ada from GitHub, replacing the cache only if the content changed."""
    try:
        with urllib.request.urlopen(_ADA_URL, timeout=10) as resp:
            data = resp.read()
    except Exception as exc:
        LOG.debug("Could not fetch ada from GitHub: %s", exc)
        return

    if _ADA_CACHE.exists():
        if hashlib.sha256(_ADA_CACHE.read_bytes()).digest() == hashlib.sha256(data).digest():
            _ADA_STAMP.touch()
            return
        LOG.info("Updating ada from GitHub")
    else:
        LOG.info("Downloading ada from GitHub to %s", _ADA_CACHE)

    _ADA_CACHE.parent.mkdir(parents=True, exist_ok=True)
    _ADA_CACHE.write_bytes(data)
    _ADA_CACHE.chmod(0o755)
    _ADA_STAMP.touch()


def _default_ada() -> str:
    """Return path to ada: $ADA env var, or a cached copy kept fresh from GitHub."""
    env = os.environ.get("ADA")
    if env:
        return env

    # Re-check GitHub at most once per day; skip silently if unreachable.
    needs_check = not _ADA_CACHE.exists() or (
        not _ADA_STAMP.exists()
        or (time.time() - _ADA_STAMP.stat().st_mtime) > _ADA_CHECK_INTERVAL
    )
    if needs_check:
        _update_ada()

    return str(_ADA_CACHE) if _ADA_CACHE.is_file() else "ada"

# Project-level macaroon directories searched when ~/macaroons/ has no match
_SNELLIUS_WORK_GLOBS = ["/gpfs/work*/0"]
_SPIDER_PROJECT_DIR = Path("/project")

DEFAULT_RCLONE_CONFIG_CANDIDATES = [
    Path("~/config/rclone/rclone.conf"),   # Snellius-specific
    Path("~/.config/rclone/rclone.conf"),
]


# ---------------------------------------------------------------------------
# ANSI colors (disabled when stderr is not a terminal)
# ---------------------------------------------------------------------------

class _C:
    """ANSI escape sequences.  Call ``_C.init(stream)`` once to enable/disable."""
    RESET = BOLD = DIM = ""
    RED = GREEN = YELLOW = BLUE = CYAN = MAGENTA = WHITE = ""
    BG_GREEN = BG_RED = BG_BLUE = BG_YELLOW = ""

    @classmethod
    def init(cls, stream=None):
        stream = stream or sys.stderr
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
            cls.BG_GREEN  = "\033[42m"
            cls.BG_RED    = "\033[41m"
            cls.BG_BLUE   = "\033[44m"
            cls.BG_YELLOW = "\033[43m"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def format_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024


def fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 3600:
        return f"{s // 60:02d}:{s % 60:02d}"
    return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"


# ---------------------------------------------------------------------------
# Argument / prefix parsing
# ---------------------------------------------------------------------------

def parse_remote_prefix(path: str) -> tuple[str | None, str]:
    """Return (prefix_name, bare_path).  prefix_name is None for local paths."""
    if ":" in path:
        prefix, _, rest = path.partition(":")
        if prefix and not os.path.exists(path):
            return prefix.lower(), rest
    return None, path


def _user_groups() -> list[str]:
    """Return names of UNIX groups the current user belongs to."""
    import grp
    try:
        gids = os.getgroups()
        return [grp.getgrgid(g).gr_name for g in gids]
    except (KeyError, OSError):
        return []


def _spider_project_names() -> list[str]:
    """Derive Spider project folder names from UNIX groups.

    Groups look like ``holstegelab-mhulsman``, ``holstegelab-data``, etc.
    The project folder is the part before the first hyphen: ``holstegelab``.
    Returns deduplicated names preserving order.
    """
    seen: set[str] = set()
    result: list[str] = []
    for g in _user_groups():
        base = g.split("-", 1)[0] if "-" in g else g
        if base and base not in seen:
            seen.add(base)
            result.append(base)
    return result


def _find_config_in_project_dirs(prefix: str) -> Path | None:
    """Search Snellius project spaces and Spider project dirs for <prefix>.conf.

    Snellius: reads MYQUOTA_PROJECTSPACES env var (space-separated project names)
    and checks /gpfs/work*/0/<project>/macaroons/<prefix>.conf.
    Falls back to the user's UNIX group names when the env var is unset.

    Spider: uses the user's UNIX group names to check only
    /project/<group>/Data/macaroons/<prefix>.conf — no directory scanning.
    """
    import glob as _glob

    fname = f"{prefix}.conf"

    # Snellius: MYQUOTA_PROJECTSPACES="ades adsprw qtholstg"
    projects = os.environ.get("MYQUOTA_PROJECTSPACES", "").split()
    if not projects:
        projects = _user_groups()
    if projects:
        for work_glob in _SNELLIUS_WORK_GLOBS:
            for work_dir in sorted(_glob.glob(work_glob)):
                for proj in projects:
                    candidate = Path(work_dir) / proj / "macaroons" / fname
                    if candidate.exists():
                        LOG.debug("found config in project dir: %s", candidate)
                        return candidate

    # Spider: /project/<project>/Data/macaroons/<prefix>.conf
    # Derive project names from group memberships (e.g. holstegelab-mhulsman → holstegelab).
    if _SPIDER_PROJECT_DIR.is_dir():
        for project in _spider_project_names():
            candidate = _SPIDER_PROJECT_DIR / project / "Data" / "macaroons" / fname
            if candidate.exists():
                LOG.debug("found config in Spider project dir: %s", candidate)
                return candidate

    return None


def resolve_pool_for_config(config_path: Path) -> str | None:
    """Check for a .pool sidecar file alongside the config.

    If ~/macaroons/dcache.conf is used, this checks for
    ~/macaroons/dcache.pool — a plain-text file containing the
    poolgroup name (e.g. ``agh_rwtapepools``).
    """
    pool_file = config_path.with_suffix(".pool")
    if pool_file.exists():
        poolgroup = pool_file.read_text(encoding="utf-8").strip()
        if poolgroup:
            LOG.debug("auto-detected poolgroup %r from %s", poolgroup, pool_file)
            return poolgroup
    return None


def resolve_config_for_prefix(prefix: str, explicit_config: Path | None) -> Path:
    """Given a remote prefix like 'dcache' or 'analysis', find the rclone config.

    Search order:
      1. Explicit --config
      2. ~/macaroons/<prefix>.conf
      3. Snellius project spaces (from MYQUOTA_PROJECTSPACES env var)
      4. Spider /project/*/Data/macaroons/
      5. Standard rclone config locations
    """
    if explicit_config:
        p = explicit_config.expanduser()
        if not p.exists():
            raise FileNotFoundError(f"config file does not exist: {p}")
        return p
    candidate = MACAROON_DIR / f"{prefix}.conf"
    if candidate.exists():
        return candidate
    project_hit = _find_config_in_project_dirs(prefix)
    if project_hit:
        return project_hit
    return _resolve_default_rclone_config()


def _resolve_default_rclone_config() -> Path:
    env_value = os.environ.get("RCLONE_CONFIG")
    if env_value:
        return Path(env_value).expanduser()
    expanded = [c.expanduser() for c in DEFAULT_RCLONE_CONFIG_CANDIDATES]
    for c in expanded:
        if c.exists():
            return c
    return expanded[0]


# ---------------------------------------------------------------------------
# rclone config handling
# ---------------------------------------------------------------------------

def load_rclone_config(path: Path) -> configparser.ConfigParser:
    resolved = path.expanduser()
    if not resolved.exists():
        searched = ", ".join(str(c.expanduser()) for c in DEFAULT_RCLONE_CONFIG_CANDIDATES)
        raise FileNotFoundError(f"rclone config not found: {resolved}. Also checked: {searched}")
    parser = configparser.ConfigParser()
    with resolved.open("r", encoding="utf-8") as fh:
        parser.read_file(fh)
    if not parser.sections():
        raise ValueError(f"rclone config has no remotes: {resolved}")
    return parser


def resolve_remote_name(parser: configparser.ConfigParser, requested: str | None) -> str:
    if requested:
        if not parser.has_section(requested):
            raise ValueError(
                f"remote {requested!r} not in config; available: {', '.join(parser.sections())}"
            )
        return requested
    sections = parser.sections()
    if len(sections) == 1:
        return sections[0]
    raise ValueError("--remote required when config has multiple remotes: " + ", ".join(sections))


def resolve_api_url(explicit: str | None, remote_cfg: configparser.SectionProxy) -> str | None:
    return (
        explicit
        or os.environ.get("DCACHE_API")
        or os.environ.get("ADA_API")
        or remote_cfg.get("api", fallback=None)
    )


# ---------------------------------------------------------------------------
# Shell commands
# ---------------------------------------------------------------------------

def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    LOG.debug("cmd: %s", " ".join(shlex.quote(str(x)) for x in cmd))
    try:
        result = subprocess.run(cmd, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            LOG.error("stdout: %s", exc.stdout.strip())
        if exc.stderr:
            LOG.error("stderr: %s", exc.stderr.strip())
        raise
    if result.stdout:
        LOG.debug("stdout: %s", result.stdout.strip())
    if result.stderr:
        LOG.debug("stderr: %s", result.stderr.strip())
    return result


# ---------------------------------------------------------------------------
# Adler-32
# ---------------------------------------------------------------------------

def normalize_adler(value: str) -> str:
    s = str(value).strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    s = "".join(ch for ch in s if ch in "0123456789abcdef")
    if not s:
        return s
    return s.zfill(8)[-8:]


def adler32_local(local_path: Path) -> str:
    adler = 1
    with local_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            adler = zlib.adler32(chunk, adler)
    adler &= 0xFFFFFFFF
    return f"{adler:08x}"


# ---------------------------------------------------------------------------
# File list parsing
# ---------------------------------------------------------------------------

def load_file_list(path: Path) -> tuple[str, list[dict]]:
    """Load a two-column TSV.  Returns (direction, entries).

    Each row: ``<source>\\t<destination>``

    Direction is auto-detected from the first row's remote prefix.
    All rows must have the same direction.
    """
    entries: list[dict] = []
    direction: str | None = None

    with path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for lineno, row in enumerate(reader, 1):
            # skip blank lines and comments
            if not row or (row[0].strip().startswith("#")):
                continue
            if len(row) < 2:
                raise ValueError(f"{path}:{lineno}: expected 2 tab-separated columns, got {len(row)}")
            src_raw, dst_raw = row[0].strip(), row[1].strip()
            if not src_raw or not dst_raw:
                raise ValueError(f"{path}:{lineno}: empty source or destination")

            src_prefix, src_path = parse_remote_prefix(src_raw)
            dst_prefix, dst_path = parse_remote_prefix(dst_raw)

            if src_prefix and dst_prefix:
                raise ValueError(f"{path}:{lineno}: both columns have a remote prefix")
            if not src_prefix and not dst_prefix:
                raise ValueError(f"{path}:{lineno}: neither column has a remote prefix")

            row_dir = "download" if src_prefix else "upload"
            row_prefix = src_prefix or dst_prefix

            if direction is None:
                direction = row_dir
                first_prefix = row_prefix
            elif row_dir != direction:
                raise ValueError(
                    f"{path}:{lineno}: mixed directions; first row was {direction}, "
                    f"this row is {row_dir}"
                )

            if row_dir == "upload":
                local = Path(src_path).expanduser().resolve()
                if not local.is_file():
                    raise FileNotFoundError(f"{path}:{lineno}: local file not found: {local}")
                st = local.stat()
                entries.append({
                    "source": local,
                    "resolved_source": local,
                    "rel": local.name,
                    "size": st.st_size,
                    "remote_path": dst_path.strip("/"),
                    "_prefix": row_prefix,
                })
            else:
                remote_p = src_path.strip("/")
                local_p = Path(dst_path).expanduser().resolve()
                entries.append({
                    "remote_path": remote_p,
                    "local_path": local_p,
                    "rel": posixpath.basename(remote_p),
                    "size": 0,  # unknown until we check
                    "_prefix": row_prefix,
                })

    if direction is None:
        raise ValueError(f"file list is empty: {path}")

    # Ensure all rows use the same prefix
    prefixes = {e.pop("_prefix") for e in entries}
    if len(prefixes) > 1:
        raise ValueError(f"file list mixes remote prefixes: {prefixes}; use a single prefix")

    return direction, entries


# ---------------------------------------------------------------------------
# Transfer planning
# ---------------------------------------------------------------------------

def plan_upload(source: Path, destination: str, recursive: bool) -> list[dict]:
    """Enumerate local files and map them to remote paths."""
    source = source.expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(f"source does not exist: {source}")

    dest_is_dir = destination.endswith("/")
    cleaned = destination.strip("/")
    if not cleaned:
        raise ValueError("destination must not be empty")

    if source.is_file():
        resolved = source.resolve(strict=True)
        st = resolved.stat()
        remote = posixpath.join(cleaned, source.name) if dest_is_dir else cleaned
        return [{"source": source, "resolved_source": resolved, "rel": source.name,
                 "size": st.st_size, "remote_path": remote}]

    if not source.is_dir():
        raise ValueError(f"source is not a regular file or directory: {source}")
    if not recursive:
        raise ValueError("source is a directory; use -R/--recursive")

    root = posixpath.join(cleaned, source.name) if dest_is_dir else cleaned
    out: list[dict] = []
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        try:
            resolved = path.resolve(strict=True) if path.is_symlink() else path
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"symlink target missing: {path}") from exc
        rel = str(path.relative_to(source)).replace(os.sep, "/")
        st = resolved.stat()
        out.append({"source": path, "resolved_source": resolved, "rel": rel,
                     "size": st.st_size, "remote_path": posixpath.join(root, rel)})
    return out


def plan_download(
    rclone_config: Path, remote: str, remote_path: str,
    local_dest: Path, recursive: bool,
) -> list[dict]:
    """Enumerate remote files via ``rclone lsjson`` and map them to local paths."""
    remote_path = remote_path.strip("/")
    if not remote_path:
        raise ValueError("remote path must not be empty")

    local_dest = local_dest.expanduser().resolve()

    cmd = [
        "rclone", "--config", str(rclone_config),
        "lsjson", f"{remote}:{remote_path}",
    ]
    if recursive:
        cmd.append("--recursive")

    result = run_command(cmd)
    entries = json.loads(result.stdout)

    if not entries:
        raise FileNotFoundError(f"no files found at remote path: {remote_path}")

    out: list[dict] = []
    for entry in entries:
        if entry.get("IsDir", False):
            continue
        rel = entry["Path"]
        size = entry.get("Size", 0)
        file_remote = posixpath.join(remote_path, rel)
        local_target = local_dest / rel
        out.append({
            "remote_path": file_remote,
            "local_path": local_target,
            "rel": rel,
            "size": size,
        })

    if not out:
        if not recursive:
            raise ValueError("remote path is a directory; use -R/--recursive")
        raise FileNotFoundError(f"no files found at remote path: {remote_path}")

    return out


# ---------------------------------------------------------------------------
# Quota tracking
# ---------------------------------------------------------------------------

class QuotaTracker:
    """Periodically query ``ada --space`` and expose usage for display."""

    def __init__(self, ada_cmd: str, tokenfile: Path, api: str | None, poolgroup: str):
        self.ada_cmd = ada_cmd
        self.tokenfile = tokenfile
        self.api = api
        self.poolgroup = poolgroup
        self.total = 0
        self.free = 0
        self.precious = 0
        self.removable = 0
        self.pinned = 0
        self.available = 0
        self._ok = False
        self.lock = threading.Lock()

    def refresh(self):
        """Fetch current quota from ada --space.  Non-fatal on failure."""
        cmd = [self.ada_cmd, "--tokenfile", str(self.tokenfile)]
        if self.api:
            cmd += ["--api", self.api]
        cmd += ["--space", self.poolgroup]
        result = run_command(cmd, check=False)
        if result.returncode != 0:
            LOG.debug("quota query failed: %s", result.stderr.strip())
            return
        try:
            data = json.loads(result.stdout)
            with self.lock:
                self.total = data["total"]
                self.free = data["free"]
                self.precious = data["precious"]
                self.removable = data["removable"]
                self.pinned = self.total - self.free - self.precious - self.removable
                self.available = self.free + self.removable
                self._ok = True
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            LOG.debug("quota parse error: %s", exc)

    @property
    def ok(self) -> bool:
        with self.lock:
            return self._ok

    def summary_line(self) -> str:
        with self.lock:
            if not self._ok:
                return ""
            return (
                f"quota: {format_bytes(self.available)} avail "
                f"({format_bytes(self.free)} free + {format_bytes(self.removable)} removable) "
                f"/ {format_bytes(self.total)} total  "
                f"pinned: {format_bytes(self.pinned)}"
            )


class _QuotaPoller:
    """Background thread that refreshes quota at an interval."""

    def __init__(self, tracker: QuotaTracker, interval: float = 120):
        self.tracker = tracker
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self.tracker.refresh()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.wait(self.interval):
            self.tracker.refresh()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Progress tracking & display
# ---------------------------------------------------------------------------

class Progress:
    def __init__(self, total_files: int, total_bytes: int):
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.validated_files = 0
        self.validated_bytes = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self.total_retries = 0
        self.failed: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        self.start_time = time.monotonic()

    def success(self, rel: str, size: int, attempts: int = 1, skipped: bool = False):
        with self.lock:
            self.validated_files += 1
            self.validated_bytes += size
            if skipped:
                self.skipped_files += 1
                self.skipped_bytes += size
            if attempts > 1:
                self.total_retries += attempts - 1
            return self.validated_files, self.validated_bytes

    def failure(self, rel: str, exc: Exception):
        with self.lock:
            self.failed.append((rel, str(exc)))
            return len(self.failed)

    @property
    def done(self) -> int:
        return self.validated_files + len(self.failed)


class ProgressBar:
    """Thread-safe single-line progress bar on stderr with ANSI colors."""
    BAR_WIDTH = 30

    def __init__(self, progress: Progress, quota: QuotaTracker | None = None, stream=None):
        self.progress = progress
        self.quota = quota
        self.stream = stream or sys.stderr
        self._is_tty = hasattr(self.stream, "isatty") and self.stream.isatty()
        self._lock = threading.Lock()

    def update(self, last_file: str = ""):
        if not self._is_tty:
            return
        with self._lock:
            p = self.progress
            total = p.total_files
            done = p.done
            pct = done / total if total else 1.0
            filled = int(self.BAR_WIDTH * pct)
            bar_fill = f"{_C.BG_GREEN}{_C.WHITE}" + " " * filled + f"{_C.RESET}"
            bar_empty = f"{_C.DIM}" + "\u2591" * (self.BAR_WIDTH - filled) + f"{_C.RESET}"
            bar = bar_fill + bar_empty
            elapsed = time.monotonic() - p.start_time
            elapsed_str = fmt_duration(elapsed)
            eta_str = fmt_duration(elapsed / pct - elapsed) if 0 < pct < 1.0 else "--:--"
            pct_str = f"{_C.BOLD}{_C.GREEN}{pct * 100:5.1f}%{_C.RESET}"
            files_str = f"{_C.CYAN}{done}{_C.RESET}/{total}"
            bytes_str = (
                f"{_C.CYAN}{format_bytes(p.validated_bytes)}{_C.RESET}"
                f"/{format_bytes(p.total_bytes)}"
            )
            time_str = f"{_C.DIM}{elapsed_str}<{eta_str}{_C.RESET}"
            err = ""
            if p.failed:
                err = f"  {_C.RED}{_C.BOLD}err:{len(p.failed)}{_C.RESET}"
            line = f"  {bar} {pct_str}  {files_str} files  {bytes_str}  {time_str}{err}"
            if self.quota and self.quota.ok:
                line += f"  {_C.DIM}|{_C.RESET} {self._quota_display()}"
            elif last_file:
                mx = 35
                display = last_file if len(last_file) <= mx else "..." + last_file[-(mx - 3):]
                line += f"  {_C.DIM}{display}{_C.RESET}"
            self.stream.write(f"\r\033[K{line}")
            self.stream.flush()

    def _quota_display(self) -> str:
        if not self.quota or not self.quota.ok:
            return ""
        q = self.quota
        with q.lock:
            avail_str = f"{_C.GREEN}{format_bytes(q.available)}{_C.RESET}"
            total_str = format_bytes(q.total)
            pinned_str = f"{_C.YELLOW}{format_bytes(q.pinned)}{_C.RESET}"
        return f"{avail_str} avail / {total_str}  pin:{pinned_str}"

    def finish(self):
        if self._is_tty:
            with self._lock:
                self.stream.write("\r\033[K")
                self.stream.flush()


def print_summary(progress: Progress, direction: str, quota: QuotaTracker | None = None):
    elapsed = time.monotonic() - progress.start_time
    transferred_bytes = progress.validated_bytes - progress.skipped_bytes
    transferred_files = progress.validated_files - progress.skipped_files
    label = "uploaded" if direction == "upload" else "downloaded"

    w = sys.stderr.write
    is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()

    def line(text: str = ""):
        if is_tty:
            w(text + "\n")
        else:
            LOG.info(text.replace(_C.RESET, ""))

    line()
    line(f"{_C.BOLD}{_C.CYAN}{'=' * 34}{_C.RESET}")
    line(f"{_C.BOLD}{_C.CYAN}  transfer summary{_C.RESET}")
    line(f"{_C.BOLD}{_C.CYAN}{'=' * 34}{_C.RESET}")
    arrow = f"{_C.GREEN}\u2191{_C.RESET}" if direction == "upload" else f"{_C.BLUE}\u2193{_C.RESET}"
    line(f"  {arrow} direction : {_C.BOLD}{direction}{_C.RESET}")
    line(f"    planned   : {progress.total_files} files, {format_bytes(progress.total_bytes)}")
    line(
        f"    {label:<10s}: {_C.GREEN}{transferred_files}{_C.RESET} files, "
        f"{_C.GREEN}{format_bytes(transferred_bytes)}{_C.RESET}"
    )
    if progress.skipped_files:
        line(
            f"    skipped   : {_C.CYAN}{progress.skipped_files}{_C.RESET} files, "
            f"{format_bytes(progress.skipped_bytes)} {_C.DIM}(already verified){_C.RESET}"
        )
    if progress.total_retries:
        line(f"    retries   : {_C.YELLOW}{progress.total_retries}{_C.RESET}")
    if progress.failed:
        line(f"    {_C.RED}{_C.BOLD}FAILED    : {len(progress.failed)}{_C.RESET}")
        for rel, msg in progress.failed:
            line(f"      {_C.RED}\u2718{_C.RESET} {rel} {_C.DIM}|{_C.RESET} {msg}")
    line(f"    elapsed   : {fmt_duration(elapsed)}")
    if elapsed > 0 and transferred_bytes > 0:
        line(f"    speed     : {_C.CYAN}{format_bytes(int(transferred_bytes / elapsed))}/s{_C.RESET}")
    if quota and quota.ok:
        quota.refresh()
        line(f"    {quota.summary_line()}")

    if not progress.failed:
        status = f"{_C.GREEN}{_C.BOLD}\u2714 COMPLETED{_C.RESET}"
    else:
        status = f"{_C.RED}{_C.BOLD}\u2718 COMPLETED WITH ERRORS{_C.RESET}"
    line(f"    status    : {status}")
    line(f"{_C.BOLD}{_C.CYAN}{'=' * 34}{_C.RESET}")


# ---------------------------------------------------------------------------
# Staging manager  (uses ada CLI)
# ---------------------------------------------------------------------------

class StageManager:
    """Bulk stage / poll / destage via the ``ada`` CLI."""

    def __init__(self, ada_cmd: str, tokenfile: Path, api: str | None):
        self.ada_cmd = ada_cmd
        self.tokenfile = tokenfile
        self.api = api

    def _base_cmd(self) -> list[str]:
        cmd = [self.ada_cmd, "--tokenfile", str(self.tokenfile)]
        if self.api:
            cmd += ["--api", self.api]
        return cmd

    def stage(self, remote_paths: list[str], lifetime: str = "7D"):
        """Issue a bulk stage request using ``ada --stage --from-file``."""
        if not remote_paths:
            return
        LOG.info("staging %d file(s) (lifetime %s) ...", len(remote_paths), lifetime)
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            for p in remote_paths:
                fh.write("/" + p.strip("/") + "\n")
            list_file = fh.name
        try:
            cmd = self._base_cmd() + ["--stage", "--from-file", list_file, "--lifetime", lifetime]
            run_command(cmd)
        finally:
            os.unlink(list_file)

    def unstage(self, remote_paths: list[str]):
        """Release pins via ``ada --unstage --from-file``."""
        if not remote_paths:
            return
        LOG.info("releasing pins for %d file(s) ...", len(remote_paths))
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            for p in remote_paths:
                fh.write("/" + p.strip("/") + "\n")
            list_file = fh.name
        try:
            cmd = self._base_cmd() + ["--unstage", "--from-file", list_file]
            run_command(cmd)
        finally:
            os.unlink(list_file)

    def is_online(self, remote_path: str) -> bool:
        """Check file locality via ``ada --stat``."""
        cmd = self._base_cmd() + ["--stat", "/" + remote_path.strip("/")]
        result = run_command(cmd, check=False)
        if result.returncode != 0:
            return False
        try:
            data = json.loads(result.stdout)
            locality = data.get("fileLocality", "")
            return "ONLINE" in locality.upper()
        except (json.JSONDecodeError, AttributeError):
            return False

    def wait_online(
        self,
        remote_paths: list[str],
        poll_interval: int = DEFAULT_STAGE_POLL,
        timeout: int = DEFAULT_STAGE_TIMEOUT,
    ) -> list[str]:
        """Poll until all files are ONLINE or timeout.  Returns list of ONLINE paths in order."""
        pending = set(remote_paths)
        online_order: list[str] = []
        start = time.monotonic()
        is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()

        while pending:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise TimeoutError(
                    f"staging timed out after {fmt_duration(elapsed)}; "
                    f"{len(pending)}/{len(remote_paths)} files still not online"
                )

            newly_online = []
            for path in list(pending):
                if self.is_online(path):
                    newly_online.append(path)

            for p in newly_online:
                pending.discard(p)
                online_order.append(p)

            done = len(remote_paths) - len(pending)
            total = len(remote_paths)
            pct = done / total if total else 1.0
            filled = int(ProgressBar.BAR_WIDTH * pct)
            bar_fill = f"{_C.BG_BLUE}{_C.WHITE}" + " " * filled + f"{_C.RESET}"
            bar_empty = f"{_C.DIM}" + "\u2591" * (ProgressBar.BAR_WIDTH - filled) + f"{_C.RESET}"
            bar = bar_fill + bar_empty

            if is_tty:
                sys.stderr.write(
                    f"\r\033[K  {bar} {_C.BLUE}staging{_C.RESET}: "
                    f"{_C.CYAN}{done}{_C.RESET}/{total} ONLINE  "
                    f"{_C.DIM}{fmt_duration(elapsed)} elapsed{_C.RESET}"
                )
                sys.stderr.flush()

            if pending:
                LOG.debug(
                    "staging: %d/%d online, %d pending, elapsed %s",
                    done, total, len(pending), fmt_duration(elapsed),
                )
                time.sleep(poll_interval)

        if is_tty:
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()
        LOG.info("all %d file(s) are ONLINE", len(remote_paths))
        return online_order

    def wait_one_online(
        self,
        remote_paths: list[str],
        already_online: set[str],
        poll_interval: int = DEFAULT_STAGE_POLL,
        timeout: int = DEFAULT_STAGE_TIMEOUT,
    ) -> str:
        """Wait until at least one path from remote_paths (not in already_online) is ONLINE.
        Returns the first path found online."""
        start = time.monotonic()
        candidates = [p for p in remote_paths if p not in already_online]
        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise TimeoutError(f"staging timed out after {fmt_duration(elapsed)}")
            for path in candidates:
                if self.is_online(path):
                    return path
            time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Copier — handles both upload and download with retries + verification
# ---------------------------------------------------------------------------

class Transferer:
    def __init__(
        self,
        rclone_config: Path,
        remote: str,
        ada_cmd: str,
        api: str | None,
        max_retries: int,
        retry_wait: int,
        copy_timeout: str,
        skip_verified: bool = True,
    ):
        self.rclone_config = Path(rclone_config).expanduser()
        self.remote = remote
        self.ada_cmd = ada_cmd
        self.api = api
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self.copy_timeout = copy_timeout
        self.skip_verified = skip_verified
        self._seen_dirs: set[str] = set()
        self._dirs_lock = threading.Lock()

        if not self.rclone_config.exists():
            raise FileNotFoundError(f"rclone config does not exist: {self.rclone_config}")

    # -- upload (local → dCache) -------------------------------------------

    def upload(self, entry: dict) -> dict:
        local_path = Path(entry["resolved_source"])
        rel = entry["rel"].replace(os.sep, "/")
        remote_path = str(entry["remote_path"]).strip("/")
        if not remote_path:
            raise ValueError("remote path could not be derived")
        remote_dir = posixpath.dirname(remote_path)
        local_adler = adler32_local(local_path)

        if self.skip_verified:
            try:
                remote_adler = self._remote_adler(remote_path)
                if normalize_adler(local_adler) == normalize_adler(remote_adler):
                    LOG.debug("skip %s (verified)", rel)
                    return self._result(rel, remote_path, entry, local_adler, remote_adler, 0, True)
            except Exception:
                LOG.debug("remote checksum unavailable for %s; uploading", rel)

        for attempt in range(self.max_retries + 1):
            self._rclone_mkdir(remote_dir)
            self._rclone_copyto(str(local_path), f"{self.remote}:{remote_path}")
            remote_adler = self._remote_adler(remote_path)
            if normalize_adler(local_adler) == normalize_adler(remote_adler):
                return self._result(rel, remote_path, entry, local_adler, remote_adler, attempt + 1, False)
            LOG.warning("checksum mismatch %s: local=%s remote=%s (attempt %d/%d)",
                        rel, local_adler, remote_adler, attempt + 1, self.max_retries + 1)
            self._rclone_deletefile(f"{self.remote}:{remote_path}")
            if attempt < self.max_retries:
                time.sleep(self.retry_wait)

        raise RuntimeError(f"checksum mismatch for {rel}: local={local_adler} remote={remote_adler}")

    # -- download (dCache → local) -----------------------------------------

    def download(self, entry: dict) -> dict:
        remote_path = str(entry["remote_path"]).strip("/")
        local_path = Path(entry["local_path"])
        rel = entry["rel"]
        size = entry.get("size", 0)

        if self.skip_verified and local_path.exists():
            try:
                remote_adler = self._remote_adler(remote_path)
                local_adler = adler32_local(local_path)
                if normalize_adler(local_adler) == normalize_adler(remote_adler):
                    LOG.debug("skip %s (verified)", rel)
                    return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, 0, True)
            except Exception:
                LOG.debug("checksum comparison failed for %s; downloading", rel)

        for attempt in range(self.max_retries + 1):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._rclone_copyto(f"{self.remote}:{remote_path}", str(local_path))
            remote_adler = self._remote_adler(remote_path)
            local_adler = adler32_local(local_path)
            if normalize_adler(local_adler) == normalize_adler(remote_adler):
                return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, attempt + 1, False)
            LOG.warning("checksum mismatch %s: local=%s remote=%s (attempt %d/%d)",
                        rel, local_adler, remote_adler, attempt + 1, self.max_retries + 1)
            local_path.unlink(missing_ok=True)
            if attempt < self.max_retries:
                time.sleep(self.retry_wait)

        raise RuntimeError(f"checksum mismatch for {rel}: local={local_adler} remote={remote_adler}")

    # -- shared helpers ----------------------------------------------------

    @staticmethod
    def _result(rel, remote_path, entry, local_adler, remote_adler, attempt, skipped):
        return {"rel": rel, "remote_path": remote_path, "size": entry.get("size", 0),
                "local_adler": local_adler, "remote_adler": remote_adler,
                "attempt": attempt, "skipped": skipped}

    @staticmethod
    def _dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, attempt, skipped):
        return {"rel": rel, "remote_path": remote_path, "local_path": str(local_path),
                "size": size, "local_adler": local_adler, "remote_adler": remote_adler,
                "attempt": attempt, "skipped": skipped}

    def _remote_adler(self, remote_path: str) -> str:
        cmd = [self.ada_cmd, "--tokenfile", str(self.rclone_config)]
        if self.api:
            cmd += ["--api", self.api]
        cmd += ["--checksum", "/" + remote_path.strip("/")]
        result = run_command(cmd)
        for token in result.stdout.strip().split():
            if "=" in token:
                key, val = token.split("=", 1)
                if key.lower().startswith("adler"):
                    return val.strip()
        for line in result.stdout.splitlines():
            if "adler32" not in line.lower():
                continue
            for part in line.replace(",", " ").split():
                if part.lower().startswith("adler32="):
                    return part.split("=", 1)[1]
        raise RuntimeError(f"cannot parse remote checksum for {remote_path}; ada output: {result.stdout.strip()!r}")

    def _rclone_mkdir(self, remote_dir: str):
        if not remote_dir:
            return
        with self._dirs_lock:
            if remote_dir in self._seen_dirs:
                return
            self._seen_dirs.add(remote_dir)
        run_command(["rclone", "--config", str(self.rclone_config), "mkdir", f"{self.remote}:{remote_dir}"])

    def _rclone_copyto(self, src: str, dst: str):
        run_command([
            "rclone", "--config", str(self.rclone_config),
            "-v", "--timeout", self.copy_timeout,
            "copyto", src, dst,
        ])

    def _rclone_deletefile(self, target: str):
        run_command(["rclone", "--config", str(self.rclone_config), "-v", "deletefile", target])


# ---------------------------------------------------------------------------
# Execution strategies
# ---------------------------------------------------------------------------

def _execute_simple(
    files: list[dict],
    worker_fn,
    workers: int,
    progress: Progress,
    bar: ProgressBar,
):
    """Simple parallel execution — used for uploads and --no-stage downloads."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(worker_fn, e): e for e in files}
        for future in concurrent.futures.as_completed(future_map):
            entry = future_map[future]
            _handle_result(future, entry, progress, bar)


def _handle_result(future: concurrent.futures.Future, entry: dict, progress: Progress, bar: ProgressBar):
    """Process a completed transfer future: update progress and display."""
    try:
        result = future.result()
    except Exception as exc:
        progress.failure(entry["rel"], exc)
        bar.finish()
        LOG.error("%s\u2718%s %s: %s", _C.RED, _C.RESET, entry["rel"], exc)
        bar.update(entry["rel"])
        return None
    skipped = result.get("skipped", False)
    progress.success(
        entry["rel"], entry.get("size", 0),
        attempts=result.get("attempt", 1),
        skipped=skipped,
    )
    if skipped:
        LOG.debug("%s\u2714%s %s %s(verified)%s", _C.CYAN, _C.RESET, result["rel"], _C.DIM, _C.RESET)
    else:
        bar.finish()
        LOG.info(
            "%s\u2714%s %s %s(%s)%s",
            _C.GREEN, _C.RESET, result["rel"],
            _C.DIM, format_bytes(entry.get("size", 0)), _C.RESET,
        )
    bar.update(result["rel"])
    return result


def _execute_pipeline_download(
    files: list[dict],
    transferer: Transferer,
    stage_mgr: StageManager,
    workers: int,
    progress: Progress,
    bar: ProgressBar,
    stage_batch: int,
    stage_lifetime: str,
    stage_poll: int,
    stage_timeout: int,
    destage: bool,
):
    """Pipeline: stage a batch → download as files come online → destage completed files.

    This avoids filling the staging area with more data than can be held at once.
    Files are processed in batches of ``stage_batch``.
    """
    remote_path_to_entry = {"/" + e["remote_path"].strip("/"): e for e in files}
    all_remote_paths = list(remote_path_to_entry.keys())

    # Process in batches
    for batch_start in range(0, len(all_remote_paths), stage_batch):
        batch_paths = all_remote_paths[batch_start: batch_start + stage_batch]
        batch_entries = [remote_path_to_entry[p] for p in batch_paths]
        batch_num = batch_start // stage_batch + 1
        total_batches = (len(all_remote_paths) + stage_batch - 1) // stage_batch

        if total_batches > 1:
            LOG.info("batch %d/%d: staging %d file(s)", batch_num, total_batches, len(batch_paths))

        # Stage this batch
        stage_mgr.stage(batch_paths, lifetime=stage_lifetime)

        # Wait for all files in this batch to come online, then download
        # Use a polling thread that feeds online files to the download pool
        online_set: set[str] = set()
        pending_stage = set(batch_paths)
        completed_paths: list[str] = []
        download_futures: dict[concurrent.futures.Future, dict] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
            stage_start = time.monotonic()

            while pending_stage or download_futures:
                # Check which pending files are now online
                if pending_stage:
                    newly_online = []
                    for path in list(pending_stage):
                        if stage_mgr.is_online(path):
                            newly_online.append(path)
                    for path in newly_online:
                        pending_stage.discard(path)
                        online_set.add(path)
                        entry = remote_path_to_entry[path]
                        fut = executor.submit(transferer.download, entry)
                        download_futures[fut] = entry

                    # Show staging progress when still waiting
                    if pending_stage and is_tty:
                        staged_n = len(batch_paths) - len(pending_stage)
                        elapsed = time.monotonic() - stage_start
                        pct = staged_n / len(batch_paths) if batch_paths else 1.0
                        filled = int(ProgressBar.BAR_WIDTH * pct)
                        bar_fill = f"{_C.BG_BLUE}{_C.WHITE}" + " " * filled + f"{_C.RESET}"
                        bar_empty = f"{_C.DIM}" + "\u2591" * (ProgressBar.BAR_WIDTH - filled) + f"{_C.RESET}"
                        sbar = bar_fill + bar_empty
                        sys.stderr.write(
                            f"\r\033[K  {sbar} {_C.BLUE}staging{_C.RESET}: "
                            f"{_C.CYAN}{staged_n}{_C.RESET}/{len(batch_paths)} ONLINE  "
                            f"{_C.DIM}{fmt_duration(elapsed)} elapsed{_C.RESET}"
                        )
                        sys.stderr.flush()

                # Check completed downloads
                done_futures = []
                for fut in list(download_futures):
                    if fut.done():
                        done_futures.append(fut)

                for fut in done_futures:
                    entry = download_futures.pop(fut)
                    rp = "/" + str(entry["remote_path"]).strip("/")
                    result = _handle_result(fut, entry, progress, bar)
                    completed_paths.append(rp)

                # Check staging timeout
                if pending_stage:
                    if time.monotonic() - stage_start > stage_timeout:
                        if is_tty:
                            sys.stderr.write("\r\033[K")
                            sys.stderr.flush()
                        raise TimeoutError(
                            f"staging timed out; {len(pending_stage)} files still not online"
                        )
                    if not done_futures and not newly_online:
                        time.sleep(stage_poll)
                elif download_futures:
                    # All staged, just wait for downloads to finish
                    if is_tty:
                        sys.stderr.write("\r\033[K")
                        sys.stderr.flush()
                    try:
                        done_iter = concurrent.futures.as_completed(download_futures, timeout=10)
                        for fut in done_iter:
                            entry = download_futures.pop(fut)
                            rp = "/" + str(entry["remote_path"]).strip("/")
                            _handle_result(fut, entry, progress, bar)
                            completed_paths.append(rp)
                            break  # back to outer loop to check for more
                    except concurrent.futures.TimeoutError:
                        pass

            if is_tty:
                sys.stderr.write("\r\033[K")
                sys.stderr.flush()

        # Destage this batch's files
        if destage and completed_paths:
            try:
                stage_mgr.unstage(completed_paths)
                LOG.debug("destaged %d file(s) from batch %d", len(completed_paths), batch_num)
            except Exception as exc:
                LOG.warning("destage failed for batch %d: %s", batch_num, exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dcache_cp",
        description=(
            "Copy files to/from dCache with Adler-32 verification.\n\n"
            "Use a remote prefix (e.g. dcache: or analysis:) on either\n"
            "source or destination to indicate the dCache side.\n"
            "The prefix selects ~/macaroons/<prefix>.conf automatically."
        ),
        epilog=(
            "examples:\n"
            "  dcache_cp ./data/ dcache:/data/   # upload\n"
            "  dcache_cp dcache:/data/ ./data/   # download\n"
            "  dcache_cp analysis:/archive/run1/ ./run1/ -R         # download with custom prefix\n"
            "  dcache_cp --file-list transfers.tsv                  # from file list\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("source", nargs="?", help="Source path (prefix with <remote>: for dCache)")
    p.add_argument("destination", nargs="?", help="Destination path (prefix with <remote>: for dCache)")
    p.add_argument("--file-list", type=Path, metavar="TSV",
                    help="Two-column TSV file with source/destination pairs (one per line)")
    p.add_argument("-R", "--recursive", action="store_true", help="Copy directories recursively")
    p.add_argument(
        "--config", "--rclone-config", dest="config", type=Path,
        help="rclone config file (default: ~/macaroons/<prefix>.conf or standard rclone locations)",
    )
    p.add_argument("--remote", default=os.environ.get("RCLONE_REMOTE"),
                    help="rclone remote name (default: only section in config)")
    p.add_argument("--ada", default=_default_ada(),
                    help="ada executable for checksums and staging (default: bundled)")
    p.add_argument("--api", help="dCache API URL override")
    p.add_argument("--dry-run", action="store_true", help="Show planned transfers without copying")
    p.add_argument("--no-skip-verified", dest="skip_verified", action="store_false", default=True,
                    help="Re-upload/download even if checksum already matches (default: skip verified)")
    p.add_argument("--workers", type=int, default=4, help="Concurrent transfer threads (default: 4)")
    p.add_argument("--max-retries", type=int, default=3, help="Max retries on checksum mismatch (default: 3)")
    p.add_argument("--retry-wait", type=int, default=60, help="Seconds between retries (default: 60)")
    p.add_argument("--copy-timeout", default=DEFAULT_COPY_TIMEOUT,
                    help="rclone --timeout value (default: %(default)s)")
    # Staging options (download only)
    p.add_argument("--no-stage", action="store_true",
                    help="Skip staging; assume files are already online (download only)")
    p.add_argument("--no-destage", action="store_true",
                    help="Keep files staged after download (default: destage after verified copy)")
    p.add_argument("--stage-batch", type=int, default=DEFAULT_STAGE_BATCH,
                    help="Files to stage at a time (default: 50)")
    p.add_argument("--stage-timeout", type=int, default=DEFAULT_STAGE_TIMEOUT,
                    help="Seconds to wait for files to come online (default: 86400 = 24h)")
    p.add_argument("--stage-poll", type=int, default=DEFAULT_STAGE_POLL,
                    help="Seconds between staging status polls (default: 60)")
    p.add_argument("--stage-lifetime", default="7D",
                    help="Pin lifetime for staged files (default: 7D)")
    # Quota
    p.add_argument("--quota-pool", metavar="POOLGROUP",
                    help="Show quota usage from ada --space <poolgroup> in progress bar")
    p.add_argument("--verbose", action="store_true", help="Enable debug logging")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.verbose)
    _C.init()

    # ---- Determine source/dest and direction ----
    if args.file_list:
        if args.source or args.destination:
            LOG.error("do not specify source/destination when using --file-list")
            return 1
        direction, files = load_file_list(args.file_list)
        # detect prefix from first entry to resolve config
        if direction == "upload":
            _, prefix_detect = parse_remote_prefix("upload:" + files[0]["remote_path"])
            # Actually we need the prefix from the file list.  Re-parse.
            # The prefix was validated in load_file_list; infer from original file.
            # We'll re-read line 1 to get the prefix
            with args.file_list.open("r") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    cols = line.split("\t")
                    src_pf, _ = parse_remote_prefix(cols[0].strip())
                    dst_pf, _ = parse_remote_prefix(cols[1].strip())
                    prefix = src_pf or dst_pf
                    break
        else:
            with args.file_list.open("r") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    cols = line.split("\t")
                    src_pf, _ = parse_remote_prefix(cols[0].strip())
                    dst_pf, _ = parse_remote_prefix(cols[1].strip())
                    prefix = src_pf or dst_pf
                    break

    else:
        if not args.source or not args.destination:
            parser.error("source and destination are required (or use --file-list)")
        src_prefix, src_path = parse_remote_prefix(args.source)
        dst_prefix, dst_path = parse_remote_prefix(args.destination)

        if src_prefix and dst_prefix:
            LOG.error("both source and destination have a remote prefix; only one side can be dCache")
            return 1
        if not src_prefix and not dst_prefix:
            LOG.error("neither source nor destination has a remote prefix (e.g. dcache:)")
            return 1

        direction = "download" if src_prefix else "upload"
        prefix = src_prefix or dst_prefix

        if direction == "upload":
            files = plan_upload(Path(src_path), dst_path, args.recursive)
        else:
            # Need config to enumerate remote; resolve early
            pass  # handled below after config resolution

    # ---- Resolve config ----
    rclone_config = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(rclone_config)
    remote = resolve_remote_name(config, args.remote)
    api = resolve_api_url(args.api, config[remote])

    # ---- Plan downloads if not from file-list ----
    if not args.file_list and direction == "download":
        files = plan_download(rclone_config, remote, src_path, Path(dst_path), args.recursive)

    # ---- Validate ----
    if args.workers < 1:
        LOG.error("--workers must be >= 1"); return 1
    if args.max_retries < 0:
        LOG.error("--max-retries must be >= 0"); return 1
    if args.stage_batch < 1:
        LOG.error("--stage-batch must be >= 1"); return 1

    if not files:
        LOG.info("no files to process")
        return 0

    total_bytes = sum(e.get("size", 0) for e in files)

    # ---- Dry run ----
    if args.dry_run:
        LOG.info("dry run (%s): %d file(s), %s", direction, len(files), format_bytes(total_bytes))
        for e in files:
            if direction == "upload":
                LOG.info("  %s -> %s (%s)", e["rel"], e["remote_path"], format_bytes(e.get("size", 0)))
            else:
                LOG.info("  %s -> %s (%s)", e["remote_path"], e.get("local_path", "?"), format_bytes(e.get("size", 0)))
        return 0

    # ---- Quota tracker ----
    quota: QuotaTracker | None = None
    quota_poller: _QuotaPoller | None = None
    quota_pool = args.quota_pool or resolve_pool_for_config(rclone_config)
    if quota_pool:
        quota = QuotaTracker(args.ada, rclone_config, api, quota_pool)
        quota_poller = _QuotaPoller(quota)
        quota_poller.start()

    # ---- Header ----
    arrow = f"{_C.GREEN}\u2191{_C.RESET}" if direction == "upload" else f"{_C.BLUE}\u2193{_C.RESET}"
    LOG.info("%sconfig%s  : %s", _C.DIM, _C.RESET, rclone_config)
    LOG.info("%sremote%s  : %s", _C.DIM, _C.RESET, remote)
    if api:
        LOG.info("%sapi%s     : %s", _C.DIM, _C.RESET, api)
    LOG.info("%smode%s    : %s %s%s%s", _C.DIM, _C.RESET, arrow, _C.BOLD, direction, _C.RESET)
    if args.file_list:
        LOG.info("%slist%s    : %s", _C.DIM, _C.RESET, args.file_list)
    LOG.info("%sfiles%s   : %s%d%s (%s)", _C.DIM, _C.RESET, _C.CYAN, len(files), _C.RESET, format_bytes(total_bytes))
    LOG.info("%sworkers%s : %d  retries: %d  skip-verified: %s",
             _C.DIM, _C.RESET, args.workers, args.max_retries, "yes" if args.skip_verified else "no")
    if direction == "download" and not args.no_stage:
        LOG.info("%sstaging%s : batch=%d  lifetime=%s  poll=%ds  timeout=%s",
                 _C.DIM, _C.RESET, args.stage_batch, args.stage_lifetime, args.stage_poll,
                 fmt_duration(args.stage_timeout))
    if quota and quota.ok:
        LOG.info("%squota%s   : %s", _C.DIM, _C.RESET, quota.summary_line())
    LOG.info("")

    # ---- Transferer ----
    transferer = Transferer(
        rclone_config=rclone_config, remote=remote, ada_cmd=args.ada,
        api=api, max_retries=args.max_retries, retry_wait=args.retry_wait,
        copy_timeout=args.copy_timeout, skip_verified=args.skip_verified,
    )
    progress = Progress(total_files=len(files), total_bytes=total_bytes)
    bar = ProgressBar(progress, quota=quota)

    try:
        if direction == "upload":
            _execute_simple(files, transferer.upload, args.workers, progress, bar)
        elif args.no_stage:
            _execute_simple(files, transferer.download, args.workers, progress, bar)
        else:
            stage_mgr = StageManager(args.ada, rclone_config, api)
            _execute_pipeline_download(
                files=files,
                transferer=transferer,
                stage_mgr=stage_mgr,
                workers=args.workers,
                progress=progress,
                bar=bar,
                stage_batch=args.stage_batch,
                stage_lifetime=args.stage_lifetime,
                stage_poll=args.stage_poll,
                stage_timeout=args.stage_timeout,
                destage=not args.no_destage,
            )
    finally:
        bar.finish()
        if quota_poller:
            quota_poller.stop()

    # ---- Summary ----
    print_summary(progress, direction, quota=quota)

    return 1 if progress.failed else 0


def entry_point():
    """Console-script entry point."""
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOG.warning("interrupted")
        raise SystemExit(130)
    except (FileNotFoundError, ValueError) as exc:
        LOG.error("%s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    entry_point()
