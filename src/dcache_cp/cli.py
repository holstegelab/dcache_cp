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
import atexit
import configparser
import csv
import hashlib
import json
import logging
import os
import posixpath
import queue
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
DEFAULT_CHECKSUM_TIMEOUT = 4 * 3600  # 4 h — TB-class files can take hours
DEFAULT_STAGE_TIMEOUT = 86400  # 24 h
DEFAULT_STAGE_POLL = 60  # seconds
DEFAULT_STAGE_BATCH = 50  # files staged at a time
DEFAULT_PROGRESS_FILE_OVERHEAD = 5 * 1024 * 1024  # weighted progress penalty per file
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

    needs_check = not _ADA_CACHE.exists() or (
        not _ADA_STAMP.exists()
        or (time.time() - _ADA_STAMP.stat().st_mtime) > _ADA_CHECK_INTERVAL
    )
    if needs_check:
        _update_ada()

    return str(_ADA_CACHE) if _ADA_CACHE.is_file() else "ada"


_SNELLIUS_WORK_GLOBS = ["/gpfs/work*/0"]
_SPIDER_PROJECT_DIR = Path("/project")

DEFAULT_RCLONE_CONFIG_CANDIDATES = [
    Path("~/config/rclone/rclone.conf"),
    Path("~/.config/rclone/rclone.conf"),
]


class _C:
    """ANSI escape sequences.  Call ``_C.init(stream)`` once to enable/disable."""
    RESET = BOLD = DIM = ""
    RED = GREEN = YELLOW = BLUE = CYAN = MAGENTA = WHITE = ""
    BG_GREEN = BG_RED = BG_BLUE = BG_YELLOW = ""

    @classmethod
    def init(cls, stream=None):
        stream = stream or sys.stderr
        if hasattr(stream, "isatty") and stream.isatty() and os.environ.get("NO_COLOR") is None:
            cls.RESET = "\033[0m"
            cls.BOLD = "\033[1m"
            cls.DIM = "\033[2m"
            cls.RED = "\033[31m"
            cls.GREEN = "\033[32m"
            cls.YELLOW = "\033[33m"
            cls.BLUE = "\033[34m"
            cls.MAGENTA = "\033[35m"
            cls.CYAN = "\033[36m"
            cls.WHITE = "\033[37m"
            cls.BG_GREEN = "\033[42m"
            cls.BG_RED = "\033[41m"
            cls.BG_BLUE = "\033[44m"
            cls.BG_YELLOW = "\033[43m"


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

def _parse_adler32(output: str) -> str | None:
    """Extract an Adler-32 value from ada --checksum output, or None if absent."""
    for token in output.strip().split():
        if "=" in token:
            key, val = token.split("=", 1)
            if key.lower().startswith("adler"):
                return val.strip()
    for line in output.splitlines():
        if "adler32" not in line.lower():
            continue
        for part in line.replace(",", " ").split():
            if part.lower().startswith("adler32="):
                return part.split("=", 1)[1]
    return None


def _ada_reports_missing_path(output: str) -> bool:
    """Return True when ada output indicates the target path does not exist."""
    text = output.lower()
    markers = (
        '"status": "404"',
        '"title": "not found"',
        "no such file or directory",
        "error while getting information about",
        "could not determine type of object",
    )
    return all(marker in text for marker in markers[:2]) or any(marker in text for marker in markers[2:])


def normalize_adler(value: str) -> str:
    s = str(value).strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    s = "".join(ch for ch in s if ch in "0123456789abcdef")
    if not s:
        return s
    return s.zfill(8)[-8:]


_CHECKSUM_CACHE_ROOT = Path("~/.cache/dcache_cp/checksums").expanduser()
_CHECKSUM_FLUSH_INTERVAL = 120  # seconds between background disk flushes


def _dir_cache_path(directory: Path) -> Path:
    """Map an absolute directory to its cache JSON file, mirroring the path.

    /gpfs/work1/0/proj/bams/ → ~/.cache/dcache_cp/checksums/gpfs/work1/0/proj/bams.json
    """
    rel = directory.resolve().relative_to("/")
    if not rel.parts:
        return _CHECKSUM_CACHE_ROOT / "root.json"
    return _CHECKSUM_CACHE_ROOT / rel.parent / (rel.name + ".json")


class _ChecksumCache:
    """In-process Adler-32 cache with periodic background flush to disk.

    All threads share one in-memory dict.  The lock only guards the dict
    (fast); checksum computation and disk I/O happen outside it.  Dirty
    entries are written to disk every *flush_interval* seconds by a daemon
    thread, and once more on exit via atexit.
    """

    def __init__(self, flush_interval: int = _CHECKSUM_FLUSH_INTERVAL) -> None:
        self._lock = threading.Lock()
        self._data: dict[Path, dict] = {}   # cache_path → {filename → entry}
        self._dirty: set[Path] = set()
        self._flush_interval = flush_interval
        self._thread = threading.Thread(target=self._flush_loop, daemon=True,
                                        name="checksum-cache-flusher")
        self._thread.start()
        atexit.register(self._flush_all)

    def get(self, local_path: Path, st: os.stat_result) -> str | None:
        """Return cached Adler-32 if the entry is fresh, else None."""
        cache_path = _dir_cache_path(local_path.parent)
        with self._lock:
            if cache_path not in self._data:
                self._data[cache_path] = self._load(cache_path)
            entry = self._data[cache_path].get(local_path.name)
        if (entry
                and entry.get("size") == st.st_size
                and entry.get("mtime_ns") == st.st_mtime_ns):
            return entry["adler32"]
        return None

    def put(self, local_path: Path, adler_str: str, st: os.stat_result) -> None:
        """Store a computed Adler-32; the background thread will flush to disk."""
        cache_path = _dir_cache_path(local_path.parent)
        with self._lock:
            if cache_path not in self._data:
                self._data[cache_path] = self._load(cache_path)
            self._data[cache_path][local_path.name] = {
                "adler32": adler_str,
                "size": st.st_size,
                "mtime_ns": st.st_mtime_ns,
            }
            self._dirty.add(cache_path)

    def _flush_all(self) -> None:
        """Snapshot dirty entries and write them to disk outside the lock."""
        with self._lock:
            if not self._dirty:
                return
            snapshot = {p: dict(self._data[p]) for p in self._dirty}
        for cache_path, data in snapshot.items():
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = cache_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
                tmp.replace(cache_path)
                with self._lock:
                    self._dirty.discard(cache_path)
            except OSError as exc:
                LOG.debug("could not write checksum cache %s: %s", cache_path, exc)

    def _flush_loop(self) -> None:
        while True:
            time.sleep(self._flush_interval)
            self._flush_all()

    @staticmethod
    def _load(cache_path: Path) -> dict:
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}


_checksum_cache = _ChecksumCache()


def adler32_local(local_path: Path) -> str:
    """Return the Adler-32 of a local file, using the shared in-process cache.

    Cache hits are served from memory (no I/O).  Misses compute the checksum
    outside any lock and queue the result for the next background flush.
    """
    st = local_path.stat()
    cached = _checksum_cache.get(local_path, st)
    if cached is not None:
        return cached

    adler = 1
    with local_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            adler = zlib.adler32(chunk, adler)
    adler_str = f"{adler & 0xFFFFFFFF:08x}"

    _checksum_cache.put(local_path, adler_str, st)
    return adler_str


def _run_in_daemon_thread(func, *args, name: str | None = None, **kwargs):
    """Run a callable in a daemon thread and return a zero-arg waiter."""
    result_queue: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)

    def _runner() -> None:
        try:
            result_queue.put((True, func(*args, **kwargs)))
        except BaseException as exc:
            result_queue.put((False, exc))

    thread = threading.Thread(target=_runner, daemon=True, name=name)
    thread.start()

    def _wait():
        ok, payload = result_queue.get()
        if ok:
            return payload
        raise payload

    return _wait


class _DaemonWorkerPool:
    """Run blocking transfer work in daemon threads and collect results via a queue."""

    def __init__(self, worker_fn, workers: int, *, name_prefix: str):
        self._worker_fn = worker_fn
        self._work_queue: queue.Queue[dict | object] = queue.Queue()
        self._result_queue: queue.Queue[tuple[dict, BaseException | None, dict | None]] = queue.Queue()
        self._stop = threading.Event()
        self._sentinel = object()
        self._closed = False
        self._threads = [
            threading.Thread(target=self._worker, daemon=True, name=f"{name_prefix}-{index + 1}")
            for index in range(workers)
        ]
        for thread in self._threads:
            thread.start()

    def _worker(self) -> None:
        while True:
            try:
                entry = self._work_queue.get(timeout=0.2)
            except queue.Empty:
                if self._stop.is_set():
                    return
                continue
            try:
                if entry is self._sentinel:
                    return
                try:
                    result = self._worker_fn(entry)
                except BaseException as exc:
                    self._result_queue.put((entry, exc, None))
                else:
                    self._result_queue.put((entry, None, result))
            finally:
                self._work_queue.task_done()

    def submit(self, entry: dict) -> None:
        if self._closed:
            raise RuntimeError("cannot submit work after closing daemon worker pool")
        self._work_queue.put(entry)

    def get_result(self, timeout: float | None = None) -> tuple[dict, BaseException | None, dict | None]:
        return self._result_queue.get(timeout=timeout)

    def get_result_nowait(self) -> tuple[dict, BaseException | None, dict | None]:
        return self._result_queue.get_nowait()

    def finish_submissions(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _ in self._threads:
            self._work_queue.put(self._sentinel)

    def stop(self) -> None:
        self._stop.set()
        self.finish_submissions()

    def join(self, timeout: float = 1.0) -> None:
        for thread in self._threads:
            thread.join(timeout=timeout)


# ---------------------------------------------------------------------------
# File list parsing
# ---------------------------------------------------------------------------

def load_file_list(path: Path, *, allow_missing_local: bool = False) -> tuple[str, list[dict]]:
    """Load a two-column TSV.  Returns (direction, entries).

    Each row: ``<source>\t<destination>``

    Direction is auto-detected from the first row's remote prefix.
    All rows must have the same direction.
    """
    entries: list[dict] = []
    direction: str | None = None

    with path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for lineno, row in enumerate(reader, 1):
            if not row or row[0].strip().startswith("#"):
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
            elif row_dir != direction:
                raise ValueError(
                    f"{path}:{lineno}: mixed directions; first row was {direction}, "
                    f"this row is {row_dir}"
                )

            if row_dir == "upload":
                local = Path(src_path).expanduser().resolve()
                if not local.is_file():
                    if not allow_missing_local:
                        raise FileNotFoundError(f"{path}:{lineno}: local file not found: {local}")
                    entries.append({
                        "source": local,
                        "resolved_source": local,
                        "rel": local.name,
                        "size": 0,
                        "remote_path": dst_path.strip("/"),
                        "_prefix": row_prefix,
                        "_missing_source": True,
                    })
                    continue
                st = local.stat()
                entries.append({
                    "source": local,
                    "resolved_source": local,
                    "rel": local.name,
                    "size": st.st_size,
                    "remote_path": dst_path.strip("/"),
                    "_prefix": row_prefix,
                    "_missing_source": False,
                })
            else:
                remote_p = src_path.strip("/")
                local_p = Path(dst_path).expanduser().resolve()
                entries.append({
                    "remote_path": remote_p,
                    "local_path": local_p,
                    "rel": posixpath.basename(remote_p),
                    "size": 0,
                    "_prefix": row_prefix,
                })

    if direction is None:
        raise ValueError(f"file list is empty: {path}")

    prefixes = {e.pop("_prefix") for e in entries}
    if len(prefixes) > 1:
        raise ValueError(f"file list mixes remote prefixes: {prefixes}; use a single prefix")

    return direction, entries


def _rclone_lsjson(
    rclone_config: Path,
    remote: str,
    remote_path: str,
    *,
    recursive: bool = False,
    missing_ok: bool = False,
) -> list[dict]:
    remote_path = remote_path.strip("/")
    target = f"{remote}:{remote_path}" if remote_path else f"{remote}:"

    cmd = [
        "rclone", "--config", str(rclone_config),
        "lsjson", target,
    ]
    if recursive:
        cmd.append("--recursive")

    result = run_command(cmd, check=not missing_ok)
    if result.returncode != 0:
        if missing_ok:
            return []
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    entries = json.loads(result.stdout)
    if not isinstance(entries, list):
        raise ValueError(f"unexpected lsjson output for {target}")
    return entries


def _fill_file_list_download_sizes(
    rclone_config: Path,
    remote: str,
    files: list[dict],
    *,
    allow_resumed_move: bool = False,
) -> list[dict]:
    """Populate sizes for file-list download entries by listing each parent directory once."""
    grouped: dict[str, set[str]] = {}
    for entry in files:
        remote_path = str(entry["remote_path"]).strip("/")
        parent = posixpath.dirname(remote_path)
        name = posixpath.basename(remote_path)
        grouped.setdefault(parent, set()).add(name)

    dir_sizes: dict[str, dict[str, int]] = {}
    active_files: list[dict] = []
    missing: list[str] = []
    for parent, expected_names in grouped.items():
        dir_entries = _rclone_lsjson(rclone_config, remote, parent, recursive=False, missing_ok=allow_resumed_move)
        sizes: dict[str, int] = {}
        for dir_entry in dir_entries:
            if dir_entry.get("IsDir", False):
                continue
            path_name = dir_entry.get("Path")
            if isinstance(path_name, str):
                sizes[path_name] = int(dir_entry.get("Size", 0))
        dir_sizes[parent] = sizes

    for entry in files:
        remote_path = str(entry["remote_path"]).strip("/")
        parent = posixpath.dirname(remote_path)
        name = posixpath.basename(remote_path)
        if name in dir_sizes[parent]:
            entry["size"] = dir_sizes[parent][name]
            active_files.append(entry)
            continue
        if allow_resumed_move and Path(entry["local_path"]).exists():
            LOG.info("skip %s (already moved)", entry["rel"])
            continue
        missing.append(remote_path)

    if missing:
        preview = ", ".join(sorted(missing)[:5])
        suffix = "" if len(missing) <= 5 else f" (+{len(missing) - 5} more)"
        raise FileNotFoundError(f"remote file(s) not found in file list: {preview}{suffix}")

    return active_files


def _filter_resumed_move_upload_file_list_entries(
    rclone_config: Path,
    remote: str,
    files: list[dict],
) -> list[dict]:
    """Skip upload move rows whose local source is gone but remote destination already exists."""
    grouped: dict[str, set[str]] = {}
    active_files: list[dict] = []

    for entry in files:
        if not entry.get("_missing_source"):
            active_files.append(entry)
            continue
        remote_path = str(entry["remote_path"]).strip("/")
        parent = posixpath.dirname(remote_path)
        name = posixpath.basename(remote_path)
        grouped.setdefault(parent, set()).add(name)

    if not grouped:
        return active_files

    dir_entries_by_parent: dict[str, set[str]] = {}
    for parent in grouped:
        dir_entries = _rclone_lsjson(rclone_config, remote, parent, recursive=False, missing_ok=True)
        dir_entries_by_parent[parent] = {
            str(dir_entry.get("Path"))
            for dir_entry in dir_entries
            if not dir_entry.get("IsDir", False) and isinstance(dir_entry.get("Path"), str)
        }

    missing: list[str] = []
    for entry in files:
        if not entry.get("_missing_source"):
            continue
        remote_path = str(entry["remote_path"]).strip("/")
        parent = posixpath.dirname(remote_path)
        name = posixpath.basename(remote_path)
        if name in dir_entries_by_parent[parent]:
            LOG.info("skip %s (already moved)", entry["rel"])
            continue
        missing.append(remote_path)

    if missing:
        preview = ", ".join(sorted(missing)[:5])
        suffix = "" if len(missing) <= 5 else f" (+{len(missing) - 5} more)"
        raise FileNotFoundError(f"source file(s) missing and destination not found: {preview}{suffix}")

    return active_files


# ---------------------------------------------------------------------------
# Transfer planning
# ---------------------------------------------------------------------------

def plan_upload(source: Path, destination: str, recursive: bool,
                spinner: "_EnumSpinner | None" = None) -> list[dict]:
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
        if spinner:
            spinner.tick()
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
        if spinner:
            spinner.tick()
    return out


def plan_download(
    rclone_config: Path, remote: str, remote_path: str,
    local_dest: Path, recursive: bool,
    spinner: "_EnumSpinner | None" = None,
) -> list[dict]:
    """Enumerate remote files via ``rclone lsjson`` and map them to local paths."""
    entries = _rclone_lsjson(rclone_config, remote, remote_path, recursive=recursive)
    remote_path = remote_path.strip("/")
    if not remote_path:
        raise ValueError("remote path must not be empty")

    local_dest = local_dest.expanduser().resolve()

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
        if spinner:
            spinner.tick()

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
            with self.lock:
                self._ok = False
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
            with self.lock:
                self._ok = False
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
# Enumeration spinner
# ---------------------------------------------------------------------------

class _EnumSpinner:
    """Shows a spinning cursor + file count on stderr while enumerating files.

    Usage::

        with _EnumSpinner("scanning") as sp:
            for path in source.rglob("*"):
                sp.tick()
                ...
    """
    _FRAMES = r"⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def __init__(self, label: str = "scanning"):
        self._label = label
        self._count = 0
        self._frame = 0
        self._is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join(timeout=1)
        if self._is_tty:
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()

    def tick(self):
        with self._lock:
            self._count += 1

    def _run(self):
        while not self._stop.wait(0.1):
            if not self._is_tty:
                continue
            with self._lock:
                frame = self._frames[self._frame % len(self._frames)]
                count = self._count
            self._frame += 1
            sys.stderr.write(f"\r\033[K  {frame} {self._label}… {count} files found")
            sys.stderr.flush()

    @property
    def _frames(self):
        return self._FRAMES


# ---------------------------------------------------------------------------
# Progress tracking & display
# ---------------------------------------------------------------------------

class Progress:
    _SPEED_WINDOW = 30.0  # seconds for rolling speed average

    def __init__(self, total_files: int, total_bytes: int, file_overhead: int = DEFAULT_PROGRESS_FILE_OVERHEAD):
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.file_overhead = file_overhead
        self.validated_files = 0
        self.validated_bytes = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self.failed_bytes = 0
        self.total_retries = 0
        self.failed: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        self.start_time = time.monotonic()
        self.status = ""  # current per-worker activity shown in the bar
        # Rolling window: list of (timestamp, bytes) for speed calculation
        self._speed_samples: list[tuple[float, int]] = []

    def success(self, rel: str, size: int, attempts: int = 1, skipped: bool = False):
        with self.lock:
            self.validated_files += 1
            self.validated_bytes += size
            if skipped:
                self.skipped_files += 1
                self.skipped_bytes += size
            else:
                now = time.monotonic()
                self._speed_samples.append((now, size))
                # Trim samples outside the window
                cutoff = now - self._SPEED_WINDOW
                while self._speed_samples and self._speed_samples[0][0] < cutoff:
                    self._speed_samples.pop(0)
            if attempts > 1:
                self.total_retries += attempts - 1
            return self.validated_files, self.validated_bytes

    def failure(self, rel: str, size: int, exc: Exception):
        with self.lock:
            self.failed_bytes += size
            self.failed.append((rel, str(exc)))
            return len(self.failed)

    @property
    def total_progress_units(self) -> int:
        return self.total_bytes + self.total_files * self.file_overhead

    @property
    def completed_progress_units(self) -> int:
        completed_files = self.validated_files + len(self.failed)
        return self.validated_bytes + self.failed_bytes + completed_files * self.file_overhead

    @property
    def progress_fraction(self) -> float:
        total_units = self.total_progress_units
        if total_units <= 0:
            return 1.0
        return min(self.completed_progress_units / total_units, 1.0)

    def speed_bps(self) -> float | None:
        """Rolling average bytes/s over the last SPEED_WINDOW seconds."""
        with self.lock:
            if len(self._speed_samples) < 2:
                return None
            now = time.monotonic()
            cutoff = now - self._SPEED_WINDOW
            samples = [(t, b) for t, b in self._speed_samples if t >= cutoff]
            if len(samples) < 2:
                return None
            window = samples[-1][0] - samples[0][0]
            if window <= 0:
                return None
            return sum(b for _, b in samples) / window

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
        # Ticker: refreshes the bar every second so hashing status and speed
        # stay current without waiting for a file to complete.
        self._stop = threading.Event()
        self._ticker = threading.Thread(target=self._tick_loop, daemon=True,
                                        name="progress-ticker")
        self._ticker.start()

    def _tick_loop(self):
        while not self._stop.wait(1.0):
            self.update()

    def stop(self):
        self._stop.set()

    def update(self, last_file: str = ""):
        if not self._is_tty:
            return
        with self._lock:
            p = self.progress
            total = p.total_files
            done = p.done
            pct = p.progress_fraction
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
            speed = p.speed_bps()
            speed_str = f"  {_C.CYAN}{format_bytes(int(speed))}/s{_C.RESET}" if speed else ""
            err = ""
            if p.failed:
                err = f"  {_C.RED}{_C.BOLD}err:{len(p.failed)}{_C.RESET}"
            line = f"  {bar} {pct_str}  {files_str} files  {bytes_str}  {time_str}{speed_str}{err}"
            # Always show hashing status when active; fall back to quota or last filename.
            status = p.status or last_file
            if status:
                mx = 35
                display = status if len(status) <= mx else "..." + status[-(mx - 3):]
                line += f"  {_C.DIM}{display}{_C.RESET}"
            elif self.quota and self.quota.ok:
                line += f"  {_C.DIM}|{_C.RESET} {self._quota_display()}"
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


def print_summary(
    progress: Progress,
    direction: str,
    quota: QuotaTracker | None = None,
    interrupted: bool = False,
):
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
    if quota:
        quota.refresh()
        summary = quota.summary_line()
        if summary:
            line(f"    {summary}")

    if interrupted:
        status = f"{_C.YELLOW}{_C.BOLD}! INTERRUPTED{_C.RESET}"
    elif not progress.failed:
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
        checksum_timeout: int = DEFAULT_CHECKSUM_TIMEOUT,
        skip_verified: bool = True,
        delete_source: bool = False,
    ):
        self.rclone_config = Path(rclone_config).expanduser()
        self.remote = remote
        self.ada_cmd = ada_cmd
        self.api = api
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self.copy_timeout = copy_timeout
        self.checksum_timeout = checksum_timeout
        self.skip_verified = skip_verified
        self.delete_source = delete_source
        self.progress: Progress | None = None  # set by caller to enable status updates
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

        # Skip-verification: check remote first (cheap API call).
        # Only hash locally if the remote already has a checksum — avoids
        # reading the entire file before uploading it on a cold run.
        local_adler: str | None = None
        if self.skip_verified:
            try:
                remote_adler = self._remote_adler(remote_path)
                # Remote has a checksum — now compute local to compare.
                if self.progress:
                    self.progress.status = f"hashing {local_path.name}"
                local_adler = adler32_local(local_path)
                if self.progress:
                    self.progress.status = ""
                if normalize_adler(local_adler) == normalize_adler(remote_adler):
                    self._delete_uploaded_source(entry)
                    LOG.debug("skip %s (verified)", rel)
                    return self._result(rel, remote_path, entry, local_adler, remote_adler, 0, True)
            except FileNotFoundError:
                LOG.debug("remote file missing for %s; uploading", rel)
            except Exception:
                LOG.debug("remote checksum unavailable for %s; uploading", rel)
            finally:
                if self.progress:
                    self.progress.status = ""

        for attempt in range(self.max_retries + 1):
            self._rclone_mkdir(remote_dir)
            self._rclone_copyto(str(local_path), f"{self.remote}:{remote_path}")
            # Fetch remote checksum (may wait for dCache to compute it).
            # Compute local hash concurrently in a thread so we don't add
            # extra wall-clock time on top of the checksum wait.
            try:
                if local_adler is None:
                    if self.progress:
                        self.progress.status = f"hashing {local_path.name}"
                    wait_for_local_adler = _run_in_daemon_thread(
                        adler32_local,
                        local_path,
                        name=f"hash-{local_path.name}",
                    )
                    if self.progress:
                        self.progress.status = f"waiting checksum {local_path.name}"
                    try:
                        remote_adler = self._remote_adler(remote_path)
                        local_adler = wait_for_local_adler()
                    finally:
                        if self.progress:
                            self.progress.status = ""
                else:
                    if self.progress:
                        self.progress.status = f"waiting checksum {local_path.name}"
                    try:
                        remote_adler = self._remote_adler(remote_path)
                    finally:
                        if self.progress:
                            self.progress.status = ""
            except Exception as exc:
                LOG.warning("verification failed %s: %s (attempt %d/%d)",
                            rel, exc, attempt + 1, self.max_retries + 1)
                try:
                    self._rclone_deletefile(f"{self.remote}:{remote_path}")
                except Exception as delete_exc:
                    LOG.debug("cleanup after verification failure for %s failed: %s", rel, delete_exc)
                local_adler = None
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                    continue
                raise RuntimeError(f"verification failed for {rel}: {exc}") from exc

            if normalize_adler(local_adler) == normalize_adler(remote_adler):
                self._delete_uploaded_source(entry)
                return self._result(rel, remote_path, entry, local_adler, remote_adler, attempt + 1, False)
            LOG.warning("checksum mismatch %s: local=%s remote=%s (attempt %d/%d)",
                        rel, local_adler, remote_adler, attempt + 1, self.max_retries + 1)
            self._rclone_deletefile(f"{self.remote}:{remote_path}")
            local_adler = None  # re-hash on retry in case file changed
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
                    self._delete_downloaded_source(remote_path)
                    LOG.debug("skip %s (verified)", rel)
                    return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, 0, True)
            except Exception:
                LOG.debug("checksum comparison failed for %s; downloading", rel)

        for attempt in range(self.max_retries + 1):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._rclone_copyto(f"{self.remote}:{remote_path}", str(local_path))
            try:
                remote_adler = self._remote_adler(remote_path)
                local_adler = adler32_local(local_path)
            except Exception as exc:
                LOG.warning("verification failed %s: %s (attempt %d/%d)",
                            rel, exc, attempt + 1, self.max_retries + 1)
                local_path.unlink(missing_ok=True)
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                    continue
                raise RuntimeError(f"verification failed for {rel}: {exc}") from exc
            if normalize_adler(local_adler) == normalize_adler(remote_adler):
                self._delete_downloaded_source(remote_path)
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
        """Fetch the remote Adler-32 via ada --checksum.

        Retries within self.checksum_timeout seconds on:
                - Missing remote path: raises FileNotFoundError immediately
        - HTTP 429 (rate-limit): ada exits non-zero, backoff capped at 60 s
        - Checksum not yet computed: ada exits 0 but no ADLER32 token
          (dCache computes checksums asynchronously; TB-class files can take hours)
          backoff grows to 5 min then stays there
        Any other non-zero exit raises immediately.
        """
        cmd = [self.ada_cmd, "--tokenfile", str(self.rclone_config)]
        if self.api:
            cmd += ["--api", self.api]
        cmd += ["--checksum", "/" + remote_path.strip("/")]

        deadline = time.monotonic() + self.checksum_timeout
        attempt = 0
        while True:
            result = run_command(cmd, check=False)
            output = result.stdout + result.stderr

            if _ada_reports_missing_path(output):
                raise FileNotFoundError(f"remote path not found for checksum lookup: {remote_path}")

            if result.returncode != 0:
                if "429" in output:
                    wait = min(2 ** attempt * 2, 60)  # 2 s … 60 s
                    if time.monotonic() + wait < deadline:
                        LOG.debug("ada rate-limited (429) for %s, retrying in %ds", remote_path, wait)
                        time.sleep(wait)
                        attempt += 1
                        continue
                # Non-429, or deadline would be exceeded waiting for 429 retry.
                detail = (result.stdout.strip() or result.stderr.strip()).splitlines()[0]
                LOG.debug("ada --checksum failed for %s (exit %d): %s",
                          remote_path, result.returncode, detail)
                raise RuntimeError(f"ada checksum unavailable for {remote_path}")

            adler = _parse_adler32(output)
            if adler is not None:
                return adler

            # dCache hasn't computed the checksum yet — poll with backoff.
            wait = min(5 * 2 ** attempt, 300)  # 5 s, 10 s, 20 s … 5 min
            elapsed = self.checksum_timeout - (deadline - time.monotonic())
            if time.monotonic() + wait >= deadline:
                raise RuntimeError(
                    f"checksum not available for {remote_path} after {elapsed:.0f}s; "
                    f"ada output: {result.stdout.strip()!r}"
                )
            LOG.debug("checksum not yet available for %s, retrying in %ds (elapsed %.0fs/%.0fs)",
                      remote_path, wait, elapsed, self.checksum_timeout)
            time.sleep(wait)
            attempt += 1

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

    def _delete_uploaded_source(self, entry: dict) -> None:
        if not self.delete_source:
            return
        source_path = Path(entry.get("source", entry["resolved_source"]))
        source_path.unlink()

    def _delete_downloaded_source(self, remote_path: str) -> None:
        if not self.delete_source:
            return
        self._rclone_deletefile(f"{self.remote}:{remote_path}")


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
    pool = _DaemonWorkerPool(worker_fn, workers, name_prefix="transfer-worker")

    try:
        for i, e in enumerate(files):
            pool.submit(e)
            # Stagger the first wave of workers so they don't all hit the
            # dCache checksum API simultaneously and trigger HTTP 429.
            if i < workers - 1:
                time.sleep(0.5)
        pool.finish_submissions()

        remaining = len(files)
        while remaining:
            try:
                entry, exc, result = pool.get_result(timeout=0.2)
            except queue.Empty:
                continue
            remaining -= 1
            _handle_worker_result(entry, exc, result, progress, bar)
    except KeyboardInterrupt:
        pool.stop()
        raise
    finally:
        pool.join(timeout=1)


def _handle_failed_result(entry: dict, exc: BaseException, progress: Progress, bar: ProgressBar):
    progress.failure(entry["rel"], entry.get("size", 0), exc)
    bar.finish()
    LOG.error("%s\u2718%s %s: %s", _C.RED, _C.RESET, entry["rel"], exc)
    bar.update(entry["rel"])
    return None


def _handle_completed_result(result: dict, entry: dict, progress: Progress, bar: ProgressBar):
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


def _handle_worker_result(
    entry: dict,
    exc: BaseException | None,
    result: dict | None,
    progress: Progress,
    bar: ProgressBar,
):
    if exc is not None:
        return _handle_failed_result(entry, exc, progress, bar)
    if result is None:
        return _handle_failed_result(entry, RuntimeError("worker finished without a result"), progress, bar)
    return _handle_completed_result(result, entry, progress, bar)


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
        batch_num = batch_start // stage_batch + 1
        total_batches = (len(all_remote_paths) + stage_batch - 1) // stage_batch

        if total_batches > 1:
            LOG.info("batch %d/%d: staging %d file(s)", batch_num, total_batches, len(batch_paths))

        # Stage this batch
        stage_mgr.stage(batch_paths, lifetime=stage_lifetime)

        # Wait for files in this batch to come online, then download them in daemon workers.
        pending_stage = set(batch_paths)
        completed_paths: list[str] = []
        pending_downloads = 0
        pool = _DaemonWorkerPool(transferer.download, workers, name_prefix="stage-download-worker")

        try:
            is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
            stage_start = time.monotonic()

            while pending_stage or pending_downloads:
                # Check which pending files are now online
                newly_online: list[str] = []
                if pending_stage:
                    for path in list(pending_stage):
                        if stage_mgr.is_online(path):
                            newly_online.append(path)
                    for path in newly_online:
                        pending_stage.discard(path)
                        entry = remote_path_to_entry[path]
                        pool.submit(entry)
                        pending_downloads += 1

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
                done_results = 0
                while pending_downloads:
                    try:
                        entry, exc, result = pool.get_result_nowait()
                    except queue.Empty:
                        break
                    pending_downloads -= 1
                    done_results += 1
                    handled = _handle_worker_result(entry, exc, result, progress, bar)
                    if handled is not None:
                        completed_paths.append("/" + str(entry["remote_path"]).strip("/"))

                # Check staging timeout
                if pending_stage:
                    if time.monotonic() - stage_start > stage_timeout:
                        if is_tty:
                            sys.stderr.write("\r\033[K")
                            sys.stderr.flush()
                        raise TimeoutError(
                            f"staging timed out; {len(pending_stage)} files still not online"
                        )
                    if not done_results and not newly_online:
                        time.sleep(stage_poll)
                elif pending_downloads:
                    # All staged, just wait for downloads to finish
                    if is_tty:
                        sys.stderr.write("\r\033[K")
                        sys.stderr.flush()
                    try:
                        entry, exc, result = pool.get_result(timeout=10)
                    except queue.Empty:
                        pass
                    else:
                        pending_downloads -= 1
                        handled = _handle_worker_result(entry, exc, result, progress, bar)
                        if handled is not None:
                            completed_paths.append("/" + str(entry["remote_path"]).strip("/"))

            if is_tty:
                sys.stderr.write("\r\033[K")
                sys.stderr.flush()
        except KeyboardInterrupt:
            pool.stop()
            raise
        finally:
            pool.finish_submissions()
            pool.join(timeout=1)

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

def build_parser(*, prog: str = "dcache_cp", delete_source: bool = False) -> argparse.ArgumentParser:
    verb = "Move" if delete_source else "Copy"
    action = "move" if delete_source else "copy"
    p = argparse.ArgumentParser(
        prog=prog,
        description=(
            f"{verb} files to/from dCache with Adler-32 verification.\n\n"
            "Use a remote prefix (e.g. dcache: or analysis:) on either\n"
            "source or destination to indicate the dCache side.\n"
            "The prefix selects ~/macaroons/<prefix>.conf automatically."
        ),
        epilog=(
            f"examples:\n"
            f"  {prog} ./data/ dcache:/data/              # upload directory\n"
            f"  {prog} file1.bam file2.bam dcache:/data/  # upload multiple files\n"
            f"  {prog} dcache:/data/ ./data/ -R           # download\n"
            f"  {prog} analysis:/archive/run1/ ./run1/ -R # download with custom prefix\n"
            f"  {prog} --file-list transfers.tsv          # from file list\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("paths", nargs="*", metavar="path",
                    help="Source(s) and destination. Last argument is the destination "
                         "(prefix with <remote>: for dCache). Multiple sources are supported.")
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
                    help=f"Re-{action} even if checksum already matches (default: skip verified)")
    p.add_argument("--workers", type=int, default=4, help="Concurrent transfer threads (default: 4)")
    p.add_argument("--max-retries", type=int, default=3, help="Max retries on checksum mismatch (default: 3)")
    p.add_argument("--retry-wait", type=int, default=60, help="Seconds between retries (default: 60)")
    p.add_argument("--copy-timeout", default=DEFAULT_COPY_TIMEOUT,
                    help="rclone idle --timeout value (default: %(default)s)")
    p.add_argument("--checksum-timeout", type=int, default=DEFAULT_CHECKSUM_TIMEOUT,
                    metavar="SEC",
                    help="Max seconds to wait for dCache to compute a checksum "
                         "(default: 14400 = 4h; TB-class files can take hours)")
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


def main(argv: list[str] | None = None, *, prog: str = "dcache_cp", delete_source: bool = False) -> int:
    parser = build_parser(prog=prog, delete_source=delete_source)
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    _C.init()

    # ---- Determine source/dest and direction ----
    if args.file_list:
        if args.paths:
            LOG.error("do not specify paths when using --file-list")
            return 1
        direction, files = load_file_list(args.file_list, allow_missing_local=delete_source)
        # Detect remote prefix from first data row to resolve config.
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
        if len(args.paths) < 2:
            parser.error("provide at least one source and a destination (or use --file-list)")
        *sources_raw, destination_raw = args.paths

        dst_prefix, dst_path = parse_remote_prefix(destination_raw)

        # Validate that all sources are on the same side (local or remote).
        src_prefixes = []
        for s in sources_raw:
            sp, _ = parse_remote_prefix(s)
            src_prefixes.append(sp)

        if any(sp for sp in src_prefixes) and not all(sp for sp in src_prefixes):
            LOG.error("mix of local and remote sources is not supported")
            return 1
        src_prefix = src_prefixes[0] if src_prefixes else None

        if src_prefix and dst_prefix:
            LOG.error("both source and destination have a remote prefix; only one side can be dCache")
            return 1
        if not src_prefix and not dst_prefix:
            LOG.error("neither source nor destination has a remote prefix (e.g. dcache:)")
            return 1

        direction = "download" if src_prefix else "upload"
        prefix = src_prefix or dst_prefix

        if direction == "upload":
            if len(sources_raw) > 1:
                # Multiple sources must go into a directory destination.
                dst_path_dir = dst_path if dst_path.endswith("/") else dst_path + "/"
            else:
                dst_path_dir = dst_path
            files = []
            with _EnumSpinner("scanning") as spinner:
                for src_raw in sources_raw:
                    _, src_path = parse_remote_prefix(src_raw)
                    try:
                        files.extend(plan_upload(Path(src_path), dst_path_dir, args.recursive,
                                                 spinner=spinner))
                    except ValueError as exc:
                        LOG.error("%s", exc)
                        return 1
                    except FileNotFoundError as exc:
                        LOG.error("%s", exc)
                        return 1
        else:
            # Download: multiple remote sources → local destination directory.
            if len(sources_raw) > 1:
                dst_path_dir = dst_path if dst_path.endswith("/") else dst_path + "/"
            else:
                dst_path_dir = dst_path
            # Actual enumeration happens below after config is resolved.
            pass

    # ---- Resolve config ----
    rclone_config = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(rclone_config)
    remote = resolve_remote_name(config, args.remote)
    api = resolve_api_url(args.api, config[remote])

    if args.file_list and delete_source and direction == "upload":
        files = _filter_resumed_move_upload_file_list_entries(rclone_config, remote, files)

    if args.file_list and direction == "download":
        files = _fill_file_list_download_sizes(
            rclone_config,
            remote,
            files,
            allow_resumed_move=delete_source,
        )

    # ---- Plan downloads if not from file-list ----
    if not args.file_list and direction == "download":
        files = []
        with _EnumSpinner("listing remote") as spinner:
            for src_raw in sources_raw:
                _, src_path = parse_remote_prefix(src_raw)
                files.extend(plan_download(rclone_config, remote, src_path,
                                           Path(dst_path_dir), args.recursive,
                                           spinner=spinner))

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
    if delete_source:
        LOG.info("%ssource%s  : delete after verified transfer", _C.DIM, _C.RESET)
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
        copy_timeout=args.copy_timeout, checksum_timeout=args.checksum_timeout,
        skip_verified=args.skip_verified, delete_source=delete_source,
    )
    progress = Progress(total_files=len(files), total_bytes=total_bytes)
    transferer.progress = progress
    bar = ProgressBar(progress, quota=quota)

    interrupted = False
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
                destage=(not args.no_destage) and (not delete_source),
            )
    except KeyboardInterrupt:
        interrupted = True
    finally:
        bar.stop()
        bar.finish()
        if quota_poller:
            quota_poller.stop()

    if interrupted:
        LOG.warning("interrupted")

    # ---- Summary ----
    print_summary(progress, direction, quota=quota, interrupted=interrupted)

    if interrupted:
        return 130
    return 1 if progress.failed else 0


def _run_entry_point(*, prog: str, delete_source: bool) -> None:
    try:
        raise SystemExit(main(prog=prog, delete_source=delete_source))
    except KeyboardInterrupt:
        LOG.warning("interrupted")
        raise SystemExit(130)
    except (FileNotFoundError, ValueError) as exc:
        LOG.error("%s", exc)
        raise SystemExit(1)


def entry_point():
    """Console-script entry point."""
    _run_entry_point(prog="dcache_cp", delete_source=False)


if __name__ == "__main__":
    entry_point()
