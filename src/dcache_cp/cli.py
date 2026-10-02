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
from contextlib import contextmanager
import csv
import hashlib
import json
import logging
import os
import posixpath
import queue
import random
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from pathlib import Path

from . import __version__

LOG = logging.getLogger("dcache_cp")

DEFAULT_COPY_TIMEOUT = "300m"
DEFAULT_COMMAND_TIMEOUT = 120  # seconds for metadata and management commands
DEFAULT_CHECKSUM_TIMEOUT = 4 * 3600  # 4 h — TB-class files can take hours
DEFAULT_STAGE_TIMEOUT = 86400  # 24 h
DEFAULT_STAGE_POLL = 60  # seconds
DEFAULT_STAGE_BATCH = 10000  # files staged at a time
DEFAULT_STAGE_BATCH_BYTES = 5 * 1024 ** 4  # 5 TiB staged at a time
DEFAULT_STAGE_FALLBACK_WORKERS = 4  # concurrent WebDAV range reads when ada staging fails
DEFAULT_STAGE_FALLBACK_MAX_TIME = 1  # seconds to wait for the 1-byte WebDAV priming read
DEFAULT_PROGRESS_FILE_OVERHEAD = 5 * 1024 * 1024  # weighted progress penalty per file
MACAROON_DIR = Path("~/macaroons").expanduser()

_ADA_URL = (
    "https://raw.githubusercontent.com/sara-nl/SpiderScripts"
    "/refs/heads/master/ada/ada"
)
_ADA_CHECK_INTERVAL = 86400  # seconds — recheck GitHub once per day

# Candidate directories for caching ada, tried in order.
_ADA_CACHE_DIRS = [
    Path("~/.local/share/dcache_cp").expanduser(),
    Path(os.environ.get("TMPDIR", "/tmp")) / f"dcache_cp_{os.getenv('USER', 'user')}",
    Path.cwd() / ".dcache_cp",
]


def _update_ada() -> None:
    """Download ada from GitHub into the first candidate dir where it fits."""
    try:
        with urllib.request.urlopen(_ADA_URL, timeout=10) as resp:
            data = resp.read()
    except Exception as exc:
        LOG.debug("Could not fetch ada from GitHub: %s", exc)
        return

    for d in _ADA_CACHE_DIRS:
        cache = d / "ada"
        stamp = d / ".ada_checked"

        # If already cached and up-to-date, just refresh the stamp.
        if cache.is_file() and cache.stat().st_size > 0:
            if hashlib.sha256(cache.read_bytes()).digest() == hashlib.sha256(data).digest():
                try:
                    stamp.touch()
                except OSError:
                    pass
                return
            LOG.info("Updating ada from GitHub (%s)", cache)
        else:
            LOG.info("Downloading ada from GitHub to %s", cache)

        # Write via a temp file so a failed write never leaves a 0-byte ada.
        try:
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / ".ada_tmp"
            tmp.write_bytes(data)
            tmp.chmod(0o755)
            tmp.replace(cache)
            try:
                stamp.touch()
            except OSError:
                pass
            return
        except OSError as exc:
            LOG.debug("Could not write ada to %s: %s", cache, exc)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

    LOG.debug("No writable location found to cache ada; will use system ada")


def _find_ada_cache() -> Path | None:
    """Return a valid cached ada binary: non-empty and executable."""
    for d in _ADA_CACHE_DIRS:
        p = d / "ada"
        try:
            if p.is_file() and p.stat().st_size > 0 and os.access(p, os.X_OK):
                return p
        except OSError:
            continue
    return None


def _needs_update() -> bool:
    for d in _ADA_CACHE_DIRS:
        stamp = d / ".ada_checked"
        if stamp.exists():
            return (time.time() - stamp.stat().st_mtime) > _ADA_CHECK_INTERVAL
    return True


def _default_ada() -> str:
    """Return path to ada: $ADA env var, or a cached copy kept fresh from GitHub."""
    env = os.environ.get("ADA")
    if env:
        return env

    cached = _find_ada_cache()
    if cached is None or _needs_update():
        _update_ada()
        cached = _find_ada_cache()

    return str(cached) if cached else "ada"


_SNELLIUS_WORK_GLOBS = ["/gpfs/work*/0"]
_SPIDER_PROJECT_DIR = Path("/project")

DEFAULT_RCLONE_CONFIG_CANDIDATES = [
    Path("~/config/rclone/rclone.conf"),
    Path("~/.config/rclone/rclone.conf"),
]


class _C:
    """ANSI escape sequences.  Call ``_C.init(stream)`` once to enable/disable."""
    RESET = BOLD = DIM = ""
    RED = GREEN = YELLOW = BLUE = CYAN = MAGENTA = WHITE = GRAY = ""
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
            cls.GRAY = "\033[90m"
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
    parser = configparser.ConfigParser(interpolation=None)
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

class SourceChangedError(RuntimeError):
    pass


class TransferCancelled(RuntimeError):
    pass


def run_command(cmd: list[str], check: bool = True, *, timeout: float | None = DEFAULT_COMMAND_TIMEOUT,
                cancel_event: threading.Event | None = None, secrets: tuple[str, ...] = (), quiet: bool = False) -> subprocess.CompletedProcess:
    """Run a bounded command; stop its entire process group on cancellation."""
    def redacted(value: str | None) -> str:
        value = _redact_http_secrets(value or "")
        for secret in secrets:
            value = value.replace(secret, "<redacted>")
        return value

    if cancel_event is not None and cancel_event.is_set():
        raise TransferCancelled("transfer cancelled")
    if not quiet:
        LOG.debug("cmd: %s", redacted(shlex.join([str(x) for x in cmd])))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
    deadline = time.monotonic() + timeout if timeout is not None else None
    try:
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("transfer cancelled")
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(cmd, timeout)
            try:
                stdout, stderr = process.communicate(timeout=min(0.2, max(deadline - time.monotonic(), 0.001)) if deadline else 0.2)
                break
            except subprocess.TimeoutExpired:
                continue
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.communicate(timeout=1)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
        raise
    result = subprocess.CompletedProcess(cmd, process.returncode, stdout, stderr)
    if result.stdout and not quiet:
        LOG.debug("stdout: %s", redacted(result.stdout).strip())
    if result.stderr and not quiet:
        LOG.debug("stderr: %s", redacted(result.stderr).strip())
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, cmd, output=redacted(stdout), stderr=redacted(stderr))
    return result


def _resolve_bearer_token(remote_cfg: configparser.SectionProxy, *, timeout: float = DEFAULT_COMMAND_TIMEOUT, cancel_event=None) -> str:
    token = remote_cfg.get("bearer_token", "").strip()
    command = remote_cfg.get("bearer_token_command", "").strip()
    if not token and command:
        # Token command output is a credential, so never send it through logging.
        try:
            result = run_command(shlex.split(command), timeout=timeout, cancel_event=cancel_event, check=False, quiet=True)
        except subprocess.SubprocessError:
            raise RuntimeError("configured bearer token command failed or timed out") from None
        if result.returncode:
            raise RuntimeError("configured bearer token command failed")
        token = result.stdout.strip()
    if not token or "\n" in token or "\r" in token:
        raise ValueError("selected remote must provide one bearer token or bearer_token_command")
    return token


@contextmanager
def _ada_tokenfile(config_path: Path, remote: str | None = None, *, timeout: float = DEFAULT_COMMAND_TIMEOUT, cancel_event=None):
    """Give ADA only the selected remote's token, in an owner-only file."""
    config = load_rclone_config(config_path)
    section = config[resolve_remote_name(config, remote)]
    token = _resolve_bearer_token(section, timeout=timeout, cancel_event=cancel_event)
    selected = configparser.ConfigParser(interpolation=None)
    selected["dcache"] = {"url": section.get("url", ""), "bearer_token": token}
    with tempfile.NamedTemporaryFile("w", suffix=".conf") as fh:
        selected.write(fh)
        fh.flush()
        yield Path(fh.name), token


def run_ada(ada_cmd: str, config_path: Path, api: str | None, arguments: list[str], *,
            remote: str | None = None, check: bool = True, timeout: float = DEFAULT_COMMAND_TIMEOUT,
            cancel_event: threading.Event | None = None):
    deadline = time.monotonic() + timeout
    with _ada_tokenfile(config_path, remote, timeout=timeout, cancel_event=cancel_event) as (tokenfile, token):
        cmd = [ada_cmd, "--tokenfile", str(tokenfile)]
        if api:
            cmd += ["--api", api]
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("ADA command deadline exceeded while resolving credentials")
        return run_command(cmd + arguments, check=check, timeout=remaining, cancel_event=cancel_event, secrets=(token,))


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


_ADA_REQUEST_ID_RE = re.compile(r"\b([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\b")


def _extract_ada_request_ids(output: str) -> list[str]:
    seen: set[str] = set()
    request_ids: list[str] = []
    for match in _ADA_REQUEST_ID_RE.finditer(output or ""):
        request_id = match.group(1)
        if request_id not in seen:
            seen.add(request_id)
            request_ids.append(request_id)
    return request_ids


def _redact_http_secrets(text: str) -> str:
    redacted = re.sub(r"(?i)(authz=)([^&\s]+)", r"\1<redacted>", text)
    redacted = re.sub(r"(?i)(authorization:\s*bearer\s+)(\S+)", r"\1<redacted>", redacted)
    return redacted


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
        # Filesystem clocks can coalesce several writes into one timestamp tick.
        # Only reuse a checksum computed after the file had settled for a second.
        if (isinstance(entry, dict)
                and entry.get("fingerprint") == list(_file_fingerprint(st))
                and entry.get("cached_at_ns", 0) - st.st_ctime_ns >= 1_000_000_000):
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
                "fingerprint": list(_file_fingerprint(st)),
                "cached_at_ns": time.time_ns(),
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
                with tempfile.NamedTemporaryFile("w", dir=cache_path.parent, delete=False) as fh:
                    tmp = Path(fh.name)
                    json.dump(data, fh)
                tmp.replace(cache_path)
                with self._lock:
                    if self._data[cache_path] == data:
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
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


_checksum_cache = _ChecksumCache()


def _file_fingerprint(st: os.stat_result) -> tuple[int, ...]:
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def adler32_local(local_path: Path, *, use_cache: bool = True, cancel_event: threading.Event | None = None) -> str:
    """Return the Adler-32 of a local file, using the shared in-process cache.

    Cache hits are served from memory (no I/O).  Misses compute the checksum
    outside any lock and queue the result for the next background flush.
    """
    st = local_path.stat()
    cached = _checksum_cache.get(local_path, st) if use_cache else None
    if cached is not None:
        return cached

    adler = 1
    with local_path.open("rb") as fh:
        if _file_fingerprint(os.fstat(fh.fileno())) != _file_fingerprint(st):
            raise SourceChangedError(f"file changed before hashing: {local_path}")
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelled("hashing cancelled")
            adler = zlib.adler32(chunk, adler)
        if _file_fingerprint(os.fstat(fh.fileno())) != _file_fingerprint(st):
            raise SourceChangedError(f"file changed while hashing: {local_path}")
    if _file_fingerprint(local_path.stat()) != _file_fingerprint(st):
        raise SourceChangedError(f"file changed while hashing: {local_path}")
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
                if self._stop.is_set():
                    continue
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
        owner = getattr(self._worker_fn, "__self__", None)
        if isinstance(owner, (Transferer, StageManager)):
            owner.cancel()
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
            if len(row) != 2:
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
                raw_local = Path(src_path).expanduser()
                local = raw_local.parent.resolve() / raw_local.name
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
                    "resolved_source": local.resolve(),
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

    prefixes = {e["_prefix"] for e in entries}
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


def _rclone_stat(rclone_config: Path, remote: str, remote_path: str) -> dict:
    """Return metadata for the requested object, rather than its contents."""
    remote_path = remote_path.strip("/")
    target = f"{remote}:{remote_path}" if remote_path else f"{remote}:"
    result = run_command([
        "rclone", "--config", str(rclone_config), "lsjson", target, "--stat",
    ])
    entry = json.loads(result.stdout)
    if not isinstance(entry, dict) or not isinstance(entry.get("IsDir"), bool):
        raise ValueError(f"unexpected lsjson --stat output for {target}")
    return entry


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
                spinner: "_EnumSpinner | None" = None, *, move: bool = False) -> list[dict]:
    """Enumerate local files and map them to remote paths."""
    source = source.expanduser()
    source = source.parent.resolve() / source.name
    if move and source.is_symlink() and source.is_dir():
        raise ValueError("cannot move through a directory symlink; use its explicit target path")
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
    local_dest: Path | str, recursive: bool,
    spinner: "_EnumSpinner | None" = None,
) -> list[dict]:
    """Enumerate remote files via ``rclone lsjson`` and map them to local paths."""
    remote_path = remote_path.strip("/")
    if not remote_path:
        raise ValueError("remote path must not be empty")

    dest_is_dir = str(local_dest).endswith("/")
    local_dest = Path(local_dest).expanduser().resolve()
    source = _rclone_stat(rclone_config, remote, remote_path)

    if not source["IsDir"]:
        name = posixpath.basename(remote_path)
        local_target = local_dest / name if dest_is_dir or local_dest.is_dir() else local_dest
        if spinner:
            spinner.tick()
        return [{
            "remote_path": remote_path,
            "local_path": local_target,
            "rel": name,
            "size": source.get("Size", 0),
        }]

    entries = _rclone_lsjson(rclone_config, remote, remote_path, recursive=recursive)

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


def _validate_transfer_plan(files: list[dict], direction: str, move: bool = False) -> None:
    destinations: set[str] = set()
    sources: set[str] = set()
    for entry in files:
        if direction == "upload":
            destination = posixpath.normpath("/" + entry["remote_path"].strip("/"))
            source = str(Path(entry["source"]).parent.resolve() / Path(entry["source"]).name)
        else:
            destination = str(Path(entry["local_path"]).resolve())
            source = posixpath.normpath("/" + entry["remote_path"].strip("/"))
        if destination in destinations:
            raise ValueError(f"multiple inputs have the same destination: {destination}")
        if move and source in sources:
            raise ValueError(f"a move source appears more than once: {source}; copy to all destinations before moving")
        if not str(entry["remote_path"]).strip("/"):
            raise ValueError("remote file path must not be empty")
        destinations.add(destination)
        sources.add(source)
    for destination in destinations:
        parent = posixpath.dirname(destination)
        while parent and parent != "/":
            if parent in destinations:
                raise ValueError(f"overlapping destination paths: {parent} and {destination}")
            parent = posixpath.dirname(parent)


# ---------------------------------------------------------------------------
# Quota tracking
# ---------------------------------------------------------------------------

class QuotaTracker:
    """Periodically query ``ada --space`` and expose usage for display."""

    def __init__(self, ada_cmd: str, tokenfile: Path, api: str | None, poolgroup: str, remote: str | None = None):
        self.ada_cmd = ada_cmd
        self.tokenfile = tokenfile
        self.api = api
        self.poolgroup = poolgroup
        self.remote = remote
        self.cancel_event = threading.Event()
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
        if self.cancel_event.is_set():
            return
        try:
            result = run_ada(self.ada_cmd, self.tokenfile, self.api, ["--space", self.poolgroup], remote=self.remote,
                             check=False, cancel_event=self.cancel_event)
        except TransferCancelled:
            return
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError):
            with self.lock:
                self._ok = False
            return
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
        self.tracker.cancel_event.set()
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
    _SPEED_WINDOW = 300.0  # seconds for rolling speed average

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
        self.requested_files = 0
        self.requested_bytes = 0
        self.online_files = 0
        self.online_bytes = 0
        self._stage_states: dict[str, tuple[str, int]] = {}
        self._has_stage_activity = False
        # Rolling window of cumulative transferred bytes sampled on each bar refresh.
        self._speed_observations: list[tuple[float, int]] = []

    def _add_stage_bucket(self, state: str, size: int):
        if state == "requested":
            self.requested_files += 1
            self.requested_bytes += size
        elif state == "online":
            self.online_files += 1
            self.online_bytes += size

    def _remove_stage_bucket(self, state: str, size: int):
        if state == "requested":
            self.requested_files = max(0, self.requested_files - 1)
            self.requested_bytes = max(0, self.requested_bytes - size)
        elif state == "online":
            self.online_files = max(0, self.online_files - 1)
            self.online_bytes = max(0, self.online_bytes - size)

    def _set_stage_state(self, key: str, size: int, new_state: str | None) -> bool:
        normalized_size = max(int(size), 0)
        previous = self._stage_states.get(key)
        if previous is not None:
            old_state, old_size = previous
            if old_state == new_state and old_size == normalized_size:
                return False
            self._remove_stage_bucket(old_state, old_size)
            del self._stage_states[key]
        if new_state is None:
            return previous is not None
        self._has_stage_activity = True
        self._stage_states[key] = (new_state, normalized_size)
        self._add_stage_bucket(new_state, normalized_size)
        return True

    def mark_stage_requested(self, key: str, size: int):
        with self.lock:
            return self._set_stage_state(key, size, "requested")

    def mark_stage_online(self, key: str, size: int):
        with self.lock:
            return self._set_stage_state(key, size, "online")

    def clear_stage_state(self, key: str, size: int = 0):
        with self.lock:
            return self._set_stage_state(key, size, None)

    def clear_stage_states(self, entries: list[tuple[str, int]]):
        with self.lock:
            for key, size in entries:
                self._set_stage_state(key, size, None)

    def success(self, rel: str, size: int, attempts: int = 1, skipped: bool = False, stage_key: str | None = None):
        with self.lock:
            if stage_key:
                self._set_stage_state(stage_key, size, None)
            self.validated_files += 1
            self.validated_bytes += size
            if skipped:
                self.skipped_files += 1
                self.skipped_bytes += size
            if attempts > 1:
                self.total_retries += attempts - 1
            return self.validated_files, self.validated_bytes

    def failure(self, rel: str, size: int, exc: Exception, stage_key: str | None = None):
        with self.lock:
            if stage_key:
                self._set_stage_state(stage_key, size, None)
            self.failed_bytes += size
            self.failed.append((rel, str(exc)))
            return len(self.failed)

    @staticmethod
    def _units_for(files: int, size: int, file_overhead: int) -> int:
        return max(int(size), 0) + max(int(files), 0) * file_overhead

    def snapshot(self) -> dict[str, int | float | bool | str]:
        with self.lock:
            failed_files = len(self.failed)
            total_units = self.total_bytes + self.total_files * self.file_overhead
            validated_units = self._units_for(self.validated_files, self.validated_bytes, self.file_overhead)
            failed_units = self._units_for(failed_files, self.failed_bytes, self.file_overhead)
            online_units = self._units_for(self.online_files, self.online_bytes, self.file_overhead)
            requested_units = self._units_for(self.requested_files, self.requested_bytes, self.file_overhead)
            completed_units = validated_units + failed_units
            return {
                "total_files": self.total_files,
                "total_bytes": self.total_bytes,
                "total_units": total_units,
                "done_files": self.validated_files + failed_files,
                "validated_files": self.validated_files,
                "validated_bytes": self.validated_bytes,
                "failed_files": failed_files,
                "failed_units": failed_units,
                "online_files": self.online_files,
                "online_units": online_units,
                "requested_files": self.requested_files,
                "requested_units": requested_units,
                "completed_units": completed_units,
                "progress_fraction": min(completed_units / total_units, 1.0) if total_units > 0 else 1.0,
                "status": self.status,
                "start_time": self.start_time,
                "has_stage_activity": self._has_stage_activity,
                "error_count": failed_files,
            }

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

    def observe_speed(self, now: float | None = None) -> None:
        """Record the current total transferred bytes for rolling speed estimation."""
        timestamp = now if now is not None else time.monotonic()
        with self.lock:
            transferred_bytes = max(self.validated_bytes - self.skipped_bytes, 0)
            if self._speed_observations and self._speed_observations[-1][1] == transferred_bytes:
                self._speed_observations[-1] = (timestamp, transferred_bytes)
            else:
                self._speed_observations.append((timestamp, transferred_bytes))

            cutoff = timestamp - self._SPEED_WINDOW
            while len(self._speed_observations) > 1 and self._speed_observations[0][0] < cutoff:
                self._speed_observations.pop(0)

    def speed_bps(self) -> float | None:
        """Rolling average bytes/s over the last SPEED_WINDOW seconds."""
        with self.lock:
            if len(self._speed_observations) < 2:
                return None
            first_time, first_bytes = self._speed_observations[0]
            last_time, last_bytes = self._speed_observations[-1]
            window = last_time - first_time
            if window <= 0:
                return None
            return max(last_bytes - first_bytes, 0) / window

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

    @staticmethod
    def _allocate_widths(units: list[int], width: int) -> list[int]:
        total_units = sum(units)
        if width <= 0:
            return [0] * len(units)
        if total_units <= 0:
            widths = [0] * len(units)
            widths[-1] = width
            return widths
        raw_widths = [(unit * width) / total_units for unit in units]
        widths = [int(raw) for raw in raw_widths]
        remaining = width - sum(widths)
        order = sorted(
            range(len(units)),
            key=lambda idx: raw_widths[idx] - widths[idx],
            reverse=True,
        )
        for idx in order[:remaining]:
            widths[idx] += 1
        return widths

    def _render_bar(self, snap: dict[str, int | float | bool | str]) -> str:
        total_units = int(snap["total_units"])
        validated_units = int(snap["completed_units"]) - int(snap["failed_units"])
        failed_units = int(snap["failed_units"])
        online_units = int(snap["online_units"])
        requested_units = int(snap["requested_units"])
        used_units = min(total_units, validated_units + failed_units + online_units + requested_units)
        empty_units = max(total_units - used_units, 0)
        widths = self._allocate_widths(
            [validated_units, failed_units, online_units, requested_units, empty_units],
            self.BAR_WIDTH,
        )
        segments = [
            (_C.GREEN, "█", widths[0]),
            (_C.RED, "█", widths[1]),
            (_C.BLUE, "█", widths[2]),
            (_C.GRAY, "█", widths[3]),
            (_C.DIM, "░", widths[4]),
        ]
        parts: list[str] = []
        for color, char, seg_width in segments:
            if seg_width <= 0:
                continue
            parts.append(f"{color}{char * seg_width}{_C.RESET}")
        return f"{_C.DIM}[{_C.RESET}{''.join(parts)}{_C.DIM}]{_C.RESET}"

    def update(self, last_file: str = ""):
        if not self._is_tty:
            return
        with self._lock:
            self.progress.observe_speed()
            snap = self.progress.snapshot()
            total = int(snap["total_files"])
            done = int(snap["done_files"])
            pct = float(snap["progress_fraction"])
            bar = self._render_bar(snap)
            elapsed = time.monotonic() - float(snap["start_time"])
            elapsed_str = fmt_duration(elapsed)
            eta_str = fmt_duration(elapsed / pct - elapsed) if 0 < pct < 1.0 else "--:--"
            pct_str = f"{_C.BOLD}{_C.GREEN}{pct * 100:5.1f}%{_C.RESET}"
            files_str = f"{_C.CYAN}{done}{_C.RESET}/{total}"
            bytes_str = (
                f"{_C.CYAN}{format_bytes(int(snap['validated_bytes']))}{_C.RESET}"
                f"/{format_bytes(int(snap['total_bytes']))}"
            )
            time_str = f"{_C.DIM}{elapsed_str}<{eta_str}{_C.RESET}"
            speed = self.progress.speed_bps()
            speed_str = f"  {_C.CYAN}{format_bytes(int(speed))}/s{_C.RESET}" if speed else ""
            err = ""
            if int(snap["error_count"]):
                err = f"  {_C.RED}{_C.BOLD}err:{int(snap['error_count'])}{_C.RESET}"
            line = f"  {bar} {pct_str}  {files_str} files  {bytes_str}  {time_str}{speed_str}{err}"
            if bool(snap["has_stage_activity"]):
                line += (
                    f"  {_C.BLUE}on:{int(snap['online_files'])}{_C.RESET}"
                    f" {_C.GRAY}stg:{int(snap['requested_files'])}{_C.RESET}"
                )
            # Always show hashing status when active; fall back to quota or last filename.
            status = str(snap["status"]) or last_file
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
    elif not progress.failed and progress.validated_files == progress.total_files:
        status = f"{_C.GREEN}{_C.BOLD}\u2714 COMPLETED{_C.RESET}"
    elif not progress.failed:
        status = f"{_C.YELLOW}{_C.BOLD}! INCOMPLETE{_C.RESET}"
    else:
        status = f"{_C.RED}{_C.BOLD}\u2718 COMPLETED WITH ERRORS{_C.RESET}"
    line(f"    status    : {status}")
    line(f"{_C.BOLD}{_C.CYAN}{'=' * 34}{_C.RESET}")


# ---------------------------------------------------------------------------
# Staging manager  (uses ada CLI)
# ---------------------------------------------------------------------------

class StageManager:
    """Bulk stage / poll / destage via the ``ada`` CLI."""

    def __init__(
        self,
        ada_cmd: str,
        tokenfile: Path,
        api: str | None,
        remote_cfg: configparser.SectionProxy | None = None,
    ):
        self.ada_cmd = ada_cmd
        self.tokenfile = tokenfile
        self.api = api
        self.remote = remote_cfg.name if remote_cfg is not None else None
        self.cancel_event = threading.Event()
        self.deadline: float | None = None
        self.remote_cfg = remote_cfg
        self.webdav_url = remote_cfg.get("url", fallback=None) if remote_cfg else None
        self.webdav_bearer_token = remote_cfg.get("bearer_token", fallback=None) if remote_cfg else None
        self.webdav_bearer_token_command = remote_cfg.get("bearer_token_command", fallback=None) if remote_cfg else None
        self._resolved_webdav_bearer_token = (self.webdav_bearer_token or "").strip() or None

    def cancel(self) -> None:
        self.cancel_event.set()

    def _command_timeout(self) -> float:
        if self.deadline is None:
            return DEFAULT_COMMAND_TIMEOUT
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("staging deadline exceeded")
        return min(DEFAULT_COMMAND_TIMEOUT, remaining)

    def _run_ada(self, arguments: list[str], *, check: bool = True, cleanup: bool = False):
        return run_ada(self.ada_cmd, self.tokenfile, self.api, arguments, remote=self.remote,
                       check=check, timeout=DEFAULT_COMMAND_TIMEOUT if cleanup else self._command_timeout(),
                       cancel_event=None if cleanup else self.cancel_event)

    def can_prime_via_webdav_range(self) -> bool:
        return bool(self.webdav_url and (self._resolved_webdav_bearer_token or self.webdav_bearer_token_command))

    def _resolve_webdav_bearer_token(self) -> str | None:
        if self._resolved_webdav_bearer_token:
            return self._resolved_webdav_bearer_token
        if not self.webdav_bearer_token_command:
            return None
        if self.remote_cfg is None:
            return None
        # Refresh command-backed tokens on each fallback request.
        return _resolve_bearer_token(self.remote_cfg, timeout=self._command_timeout(), cancel_event=self.cancel_event)

    def _webdav_file_url(self, remote_path: str) -> str:
        if not self.webdav_url:
            raise RuntimeError("WebDAV URL not configured for stage fallback")
        parts = [urllib.parse.quote(part, safe="") for part in remote_path.strip("/").split("/") if part]
        return self.webdav_url.rstrip("/") + "/" + "/".join(parts)

    def prime_via_webdav_range(self, entry: dict) -> dict:
        """Trigger dCache auto-staging through a 1-byte authenticated WebDAV read."""
        remote_path = str(entry["remote_path"]).strip("/")
        token = self._resolve_webdav_bearer_token()
        if not token:
            raise RuntimeError("no WebDAV bearer token available for staging fallback")

        with tempfile.NamedTemporaryFile("w", suffix=".headers") as fh:
            fh.write(f"Authorization: Bearer {token}\n")
            fh.flush()
            return self._prime_via_webdav_range(remote_path, fh.name)

    def _prime_via_webdav_range(self, remote_path: str, header_file: str) -> dict:
        url = self._webdav_file_url(remote_path)
        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--max-time",
            str(DEFAULT_STAGE_FALLBACK_MAX_TIME),
            "--range",
            "0-0",
            "--dump-header",
            "-",
            "--output",
            "/dev/null",
            "--write-out",
            "\n%{http_code}",
            "--header",
            "@" + header_file,
            url,
        ]
        redacted_cmd = cmd
        redacted_cmd_text = shlex.join(redacted_cmd)
        LOG.debug("webdav stage fallback command: %s", redacted_cmd_text)
        attempt = 0
        while True:
            result = run_command(cmd, check=False, timeout=min(self._command_timeout(), DEFAULT_STAGE_FALLBACK_MAX_TIME + 5),
                                 cancel_event=self.cancel_event)
            stdout_text = result.stdout.rstrip()
            stdout_lines = stdout_text.splitlines() if stdout_text else []
            http_code_text = stdout_lines[-1].strip() if stdout_lines else "000"
            response_text = "\n".join(stdout_lines[:-1]).strip()
            try:
                http_code = int(http_code_text)
            except ValueError:
                http_code = 0

            if http_code in {200, 206, 416}:
                return {"remote_path": "/" + remote_path}

            if result.returncode == 28:
                return {"remote_path": "/" + remote_path, "timed_out": True}

            detail_parts = []
            if result.stderr.strip():
                detail_parts.append(result.stderr.strip())
            if response_text:
                detail_parts.append(response_text)
            detail_text = _redact_http_secrets("\n".join(part for part in detail_parts if part).strip())
            detail_lines = detail_text.splitlines()
            summary = detail_lines[0] if detail_lines else f"HTTP {http_code or 'unknown'}"

            if http_code == 429 and attempt < 6:
                wait = min(2 ** attempt, 8) + random.uniform(0, 0.5)
                LOG.debug(
                    "WebDAV range-read fallback rate-limited for %s; retrying in %.1fs\ncommand: %s\n%s",
                    remote_path,
                    wait,
                    redacted_cmd_text,
                    detail_text or "(no curl details)",
                )
                if self.cancel_event.wait(min(wait, self._command_timeout())):
                    raise TransferCancelled("staging cancelled")
                attempt += 1
                continue

            if http_code in {500, 502, 503, 504} and attempt < 4:
                wait = min(2 ** attempt * 15, 120) + random.uniform(0, 5)
                LOG.warning(
                    "WebDAV range-read fallback got HTTP %d for %s; retrying in %.1fs\ncommand: %s\n%s",
                    http_code,
                    remote_path,
                    wait,
                    redacted_cmd_text,
                    detail_text or "(no curl details)",
                )
                if self.cancel_event.wait(min(wait, self._command_timeout())):
                    raise TransferCancelled("staging cancelled")
                attempt += 1
                continue

            if result.returncode != 0 and http_code == 0 and attempt < 4:
                wait = min(2 ** attempt * 10, 60) + random.uniform(0, 3)
                LOG.warning(
                    "WebDAV range-read fallback transport failure for %s; retrying in %.1fs\ncommand: %s\n%s",
                    remote_path,
                    wait,
                    redacted_cmd_text,
                    detail_text or "(no curl details)",
                )
                if self.cancel_event.wait(min(wait, self._command_timeout())):
                    raise TransferCancelled("staging cancelled")
                attempt += 1
                continue

            if not detail_text:
                if http_code:
                    detail_text = f"HTTP {http_code}"
                else:
                    detail_text = f"curl exit {result.returncode}"

            if http_code:
                raise RuntimeError(
                    f"WebDAV range-read fallback failed for {remote_path}: HTTP {http_code}; "
                    f"curl exit {result.returncode}; command: {redacted_cmd_text}; details: {detail_text}"
                )
            raise RuntimeError(
                f"WebDAV range-read fallback failed for {remote_path}: curl exit {result.returncode}; "
                f"command: {redacted_cmd_text}; details: {detail_text}"
            )

    def stage(self, remote_paths: list[str], lifetime: str = "7D") -> list[str]:
        """Issue a bulk stage request using ``ada --stage --from-file``."""
        if not remote_paths:
            return []
        LOG.info("staging %d file(s) (lifetime %s) ...", len(remote_paths), lifetime)
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            for p in remote_paths:
                fh.write("/" + p.strip("/") + "\n")
            list_file = fh.name
        try:
            result = self._run_ada(["--stage", "--from-file", list_file, "--lifetime", lifetime])
            request_ids = _extract_ada_request_ids((result.stdout or "") + "\n" + (result.stderr or ""))
            if request_ids:
                LOG.info("stage request id(s): %s", ", ".join(request_ids))
            else:
                LOG.debug("no stage request id found in ada output")
            return request_ids
        finally:
            os.unlink(list_file)

    def unstage(self, remote_paths: list[str]):
        """Release pins via ``ada --unstage --from-file``."""
        if not remote_paths:
            return
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            for p in remote_paths:
                fh.write("/" + p.strip("/") + "\n")
            list_file = fh.name
        try:
            self._run_ada(["--unstage", "--from-file", list_file], cleanup=True)
        finally:
            os.unlink(list_file)

    def _stat_json(self, remote_path: str) -> tuple[dict | None, str | None]:
        """Return parsed ``ada --stat`` JSON for a path, or an error string."""
        result = self._run_ada(["--stat", "/" + remote_path.strip("/")], check=False)
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip()).splitlines()
            summary = detail[0] if detail else f"exit {result.returncode}"
            return None, f"ada --stat failed for {remote_path}: {summary}"
        try:
            return json.loads(result.stdout), None
        except (json.JSONDecodeError, AttributeError):
            preview = result.stdout.strip()[:200]
            return None, f"ada --stat returned invalid JSON for {remote_path}: {preview!r}"

    def _stat_request_json(self, request_id: str) -> tuple[dict | None, str | None]:
        result = self._run_ada(["--stat-request", request_id], check=False)
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip()).splitlines()
            summary = detail[0] if detail else f"exit {result.returncode}"
            return None, f"ada --stat-request failed for {request_id}: {summary}"
        try:
            return json.loads(result.stdout), None
        except (json.JSONDecodeError, AttributeError):
            preview = result.stdout.strip()[:200]
            return None, f"ada --stat-request returned invalid JSON for {request_id}: {preview!r}"

    def is_online(self, remote_path: str) -> bool:
        """Check file locality via ``ada --stat``."""
        data, error = self._stat_json(remote_path)
        if error or not isinstance(data, dict):
            return False
        locality = data.get("fileLocality", "")
        return "ONLINE" in str(locality).upper()

    @staticmethod
    def _payload_is_online(data: dict | None) -> bool:
        if not isinstance(data, dict):
            return False
        locality = data.get("fileLocality", "")
        return "ONLINE" in str(locality).upper()

    def poll_online_statuses(self, remote_paths: list[str]) -> tuple[list[str], dict[str, str]]:
        """Poll many paths efficiently by grouping ``ada --stat`` calls per parent directory."""
        grouped: dict[str, list[tuple[str, str]]] = {}
        for remote_path in remote_paths:
            original = "/" + remote_path.strip("/") if remote_path.startswith("/") else remote_path.strip("/")
            cleaned = remote_path.strip("/")
            parent = posixpath.dirname(cleaned)
            grouped.setdefault(parent, []).append((original, cleaned))

        online: list[str] = []
        errors: dict[str, str] = {}

        for parent, paths in grouped.items():
            data, error = self._stat_json(parent)
            used_directory_children = False
            if not error and isinstance(data, dict):
                children = data.get("children")
                if isinstance(children, list):
                    child_map: dict[str, dict] = {}
                    for child in children:
                        if isinstance(child, dict):
                            name = child.get("fileName")
                            if isinstance(name, str):
                                child_map[name.rstrip("/")] = child

                    used_directory_children = True
                    for original_path, cleaned_path in paths:
                        name = posixpath.basename(cleaned_path)
                        child = child_map.get(name)
                        if child is None:
                            fallback_data, fallback_error = self._stat_json(cleaned_path)
                            if fallback_error:
                                errors[original_path] = fallback_error
                            elif self._payload_is_online(fallback_data):
                                online.append(original_path)
                            continue
                        if self._payload_is_online(child):
                            online.append(original_path)

            if used_directory_children:
                continue

            for original_path, cleaned_path in paths:
                fallback_data, fallback_error = self._stat_json(cleaned_path)
                if fallback_error:
                    if error:
                        errors[original_path] = f"{error}; fallback failed: {fallback_error}"
                    else:
                        errors[original_path] = fallback_error
                    continue
                if self._payload_is_online(fallback_data):
                    online.append(original_path)

        return online, errors

    def poll_stage_request_errors(
        self,
        request_ids: list[str],
        relevant_paths: set[str],
    ) -> dict[str, str]:
        """Return per-path terminal bulk-request failures for the given stage requests."""
        failures: dict[str, str] = {}
        if not request_ids or not relevant_paths:
            return failures

        normalized_relevant = {"/" + path.strip("/") for path in relevant_paths}
        for request_id in request_ids:
            data, error = self._stat_request_json(request_id)
            if error:
                LOG.debug("could not inspect stage request %s: %s", request_id, error)
                continue
            if not isinstance(data, dict):
                continue

            request_status = str(data.get("status", ""))
            targets = data.get("targets")
            if not isinstance(targets, list):
                continue

            for target in targets:
                if not isinstance(target, dict):
                    continue
                target_path = target.get("target")
                if not isinstance(target_path, str):
                    continue
                normalized_target = "/" + target_path.strip("/")
                if normalized_target not in normalized_relevant:
                    continue
                state = str(target.get("state", ""))
                if state.upper() in {"FAILED", "CANCELLED"}:
                    error_message = str(target.get("errorMessage") or target.get("errorType") or request_status or "stage request failed")
                    failures[normalized_target] = f"stage request {request_id} {state.lower()}: {error_message}"

        return failures

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
        self.deadline = start + timeout
        is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()

        while pending:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise TimeoutError(
                    f"staging timed out after {fmt_duration(elapsed)}; "
                    f"{len(pending)}/{len(remote_paths)} files still not online"
                )

            newly_online, _ = self.poll_online_statuses(list(pending))

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
                if self.cancel_event.wait(min(poll_interval, self._command_timeout())):
                    raise TransferCancelled("staging cancelled")

        if is_tty:
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()
        self.deadline = None
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
        self.deadline = start + timeout
        candidates = [p for p in remote_paths if p not in already_online]
        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise TimeoutError(f"staging timed out after {fmt_duration(elapsed)}")
            online, _ = self.poll_online_statuses(candidates)
            if online:
                self.deadline = None
                return online[0]
            if self.cancel_event.wait(min(poll_interval, self._command_timeout())):
                raise TransferCancelled("staging cancelled")


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
        self.cancel_event = threading.Event()
        self.progress: Progress | None = None  # set by caller to enable status updates
        self._seen_dirs: set[str] = set()
        self._dirs_lock = threading.Lock()

        if not self.rclone_config.exists():
            raise FileNotFoundError(f"rclone config does not exist: {self.rclone_config}")

    # -- upload (local → dCache) -------------------------------------------

    def cancel(self) -> None:
        self.cancel_event.set()

    def _check_cancelled(self) -> None:
        if self.cancel_event.is_set():
            raise TransferCancelled("transfer cancelled")

    @staticmethod
    def _upload_snapshot(entry: dict) -> tuple:
        return (_file_fingerprint(Path(entry["source"]).lstat()),
                _file_fingerprint(Path(entry["resolved_source"]).stat()))

    def _assert_upload_unchanged(self, entry: dict, snapshot: tuple) -> None:
        self._check_cancelled()
        if self._upload_snapshot(entry) != snapshot:
            raise SourceChangedError(f"source changed during transfer; retained: {entry['source']}")

    def upload(self, entry: dict) -> dict:
        local_path = Path(entry["resolved_source"])
        rel = entry["rel"].replace(os.sep, "/")
        remote_path = str(entry["remote_path"]).strip("/")
        if not remote_path:
            raise ValueError("remote path could not be derived")
        entry = dict(entry, source=entry.get("source", local_path))
        snapshot = self._upload_snapshot(entry)
        local_adler = adler32_local(local_path, use_cache=not self.delete_source, cancel_event=self.cancel_event)
        self._assert_upload_unchanged(entry, snapshot)
        if self.skip_verified:
            try:
                remote_adler = self._remote_adler(remote_path)
            except (RuntimeError, FileNotFoundError, subprocess.SubprocessError):
                self._check_cancelled()
                remote_adler = None
            if remote_adler and normalize_adler(local_adler) == normalize_adler(remote_adler):
                self._delete_uploaded_source(entry, snapshot, local_adler)
                return self._result(rel, remote_path, entry, local_adler, remote_adler, 0, True)

        last_error = None
        for attempt in range(self.max_retries + 1):
            self._assert_upload_unchanged(entry, snapshot)
            remote_dir = posixpath.dirname(remote_path)
            temporary = posixpath.join(remote_dir, f".dcache-cp-{uuid.uuid4().hex}.part")
            preserve_temporary = False
            promoted = False
            try:
                self._rclone_mkdir(remote_dir)
                self._rclone_copyto(str(local_path), f"{self.remote}:{temporary}")
                remote_adler = self._remote_adler(temporary)
                self._assert_upload_unchanged(entry, snapshot)
                if normalize_adler(adler32_local(local_path, use_cache=False, cancel_event=self.cancel_event)) != normalize_adler(local_adler):
                    raise SourceChangedError(f"source content changed during transfer; retained: {local_path}")
                if normalize_adler(local_adler) != normalize_adler(remote_adler):
                    raise RuntimeError(f"checksum mismatch for {rel}: local={local_adler} remote={remote_adler}")
                # Once promotion starts, retain any remaining verified temporary
                # copy on failure so it can be recovered even if MOVE was partial.
                preserve_temporary = True
                self._rclone_moveto(f"{self.remote}:{temporary}", f"{self.remote}:{remote_path}")
                promoted = True
                remote_adler = self._remote_adler(remote_path)
                if normalize_adler(local_adler) != normalize_adler(remote_adler):
                    raise RuntimeError(f"checksum mismatch after promotion for {rel}; source retained")
                self._delete_uploaded_source(entry, snapshot, local_adler)
                return self._result(rel, remote_path, entry, local_adler, remote_adler, attempt + 1, False)
            except (SourceChangedError, TransferCancelled):
                raise
            except Exception as exc:
                last_error = exc
                if preserve_temporary:
                    raise RuntimeError(f"promotion/verification failed for {rel}; source retained; "
                                       f"check {self.remote}:{temporary} and {self.remote}:{remote_path}: {exc}") from exc
                LOG.warning("transfer failed %s: %s (attempt %d/%d)", rel, exc, attempt + 1, self.max_retries + 1)
            finally:
                if not preserve_temporary and not promoted:
                    try:
                        self._rclone_deletefile(f"{self.remote}:{temporary}")
                    except Exception as exc:
                        LOG.debug("temporary upload cleanup failed for %s: %s", temporary, exc)
            if attempt < self.max_retries and self.cancel_event.wait(self.retry_wait):
                raise TransferCancelled("transfer cancelled")
        raise RuntimeError(f"upload failed for {rel}: {last_error}") from last_error

    # -- download (dCache → local) -----------------------------------------

    def download(self, entry: dict) -> dict:
        self._check_cancelled()
        remote_path = str(entry["remote_path"]).strip("/")
        local_path = Path(entry["local_path"])
        rel, size = entry["rel"], entry.get("size", 0)
        if self.skip_verified and local_path.is_file():
            snapshot = _file_fingerprint(local_path.stat())
            try:
                remote_adler = self._remote_adler(remote_path)
                local_adler = adler32_local(local_path, use_cache=not self.delete_source, cancel_event=self.cancel_event)
            except (RuntimeError, FileNotFoundError, subprocess.SubprocessError):
                self._check_cancelled()
            else:
                if normalize_adler(local_adler) == normalize_adler(remote_adler):
                    self._delete_downloaded_source(remote_path, local_path, local_adler, snapshot)
                    return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, 0, True)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        destination_snapshot = _file_fingerprint(local_path.lstat()) if local_path.exists() else None
        last_error = None
        for attempt in range(self.max_retries + 1):
            temporary = local_path.with_name(f".dcache-cp-{uuid.uuid4().hex}.part")
            promoted = False
            try:
                self._rclone_copyto(f"{self.remote}:{remote_path}", str(temporary))
                remote_adler = self._remote_adler(remote_path)
                local_adler = adler32_local(temporary, use_cache=False, cancel_event=self.cancel_event)
                if normalize_adler(local_adler) != normalize_adler(remote_adler):
                    raise RuntimeError(f"checksum mismatch for {rel}: local={local_adler} remote={remote_adler}")
                self._check_cancelled()
                current = _file_fingerprint(local_path.lstat()) if local_path.exists() else None
                if current != destination_snapshot:
                    raise SourceChangedError(f"destination changed during transfer; retained: {local_path}")
                if current is not None:
                    temporary.chmod(local_path.stat().st_mode & 0o777)
                temporary.replace(local_path)
                promoted = True
                snapshot = _file_fingerprint(local_path.stat())
                self._delete_downloaded_source(remote_path, local_path, local_adler, snapshot)
                return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, attempt + 1, False)
            except (SourceChangedError, TransferCancelled):
                raise
            except Exception as exc:
                last_error = exc
                # A verified copy already promoted to the destination must survive
                # a failure to delete/recheck the remote source.
                if promoted:
                    raise
                LOG.warning("transfer failed %s: %s (attempt %d/%d)", rel, exc, attempt + 1, self.max_retries + 1)
            finally:
                temporary.unlink(missing_ok=True)
            if attempt < self.max_retries and self.cancel_event.wait(self.retry_wait):
                raise TransferCancelled("transfer cancelled")
        raise RuntimeError(f"download failed for {rel}: {last_error}") from last_error

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
        deadline = time.monotonic() + self.checksum_timeout
        attempt = 0
        while True:
            self._check_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"checksum deadline exceeded for {remote_path}")
            result = run_ada(self.ada_cmd, self.rclone_config, self.api,
                             ["--checksum", "/" + remote_path.strip("/")], remote=self.remote,
                             check=False, timeout=min(DEFAULT_COMMAND_TIMEOUT, remaining), cancel_event=self.cancel_event)
            if time.monotonic() > deadline:
                raise TimeoutError(f"checksum deadline exceeded for {remote_path}")
            output = result.stdout + result.stderr

            if _ada_reports_missing_path(output):
                raise FileNotFoundError(f"remote path not found for checksum lookup: {remote_path}")

            if result.returncode != 0:
                if "429" in output:
                    wait = min(2 ** attempt * 2, 60)  # 2 s … 60 s
                    if time.monotonic() + wait < deadline:
                        LOG.debug("ada rate-limited (429) for %s, retrying in %ds", remote_path, wait)
                        if self.cancel_event.wait(wait):
                            raise TransferCancelled("transfer cancelled")
                        attempt += 1
                        continue
                # Non-429, or deadline would be exceeded waiting for 429 retry.
                details = (result.stdout.strip() or result.stderr.strip()).splitlines()
                detail = details[0] if details else f"exit {result.returncode}"
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
            if self.cancel_event.wait(wait):
                raise TransferCancelled("transfer cancelled")
            attempt += 1

    def _rclone_mkdir(self, remote_dir: str):
        if not remote_dir:
            return
        with self._dirs_lock:
            if remote_dir in self._seen_dirs:
                return
            run_command(["rclone", "--config", str(self.rclone_config), "mkdir", f"{self.remote}:{remote_dir}"],
                        cancel_event=self.cancel_event)
            self._seen_dirs.add(remote_dir)

    def _rclone_copyto(self, src: str, dst: str):
        run_command(["rclone", "--config", str(self.rclone_config), "-v", "--timeout", self.copy_timeout,
                     "--ignore-times", "copyto", src, dst], timeout=None, cancel_event=self.cancel_event)

    def _rclone_moveto(self, src: str, dst: str):
        # Let WebDAV's MOVE Overwrite:T replace the target. With a checked
        # destination, rclone deletes it before attempting the server-side MOVE.
        run_command(["rclone", "--config", str(self.rclone_config), "-v", "--timeout", self.copy_timeout,
                     "--ignore-times", "--no-check-dest", "moveto", src, dst], timeout=None, cancel_event=self.cancel_event)

    def _rclone_deletefile(self, target: str):
        run_command(["rclone", "--config", str(self.rclone_config), "-v", "deletefile", target],
                    cancel_event=self.cancel_event)

    def _delete_uploaded_source(self, entry: dict, snapshot: tuple, checksum: str) -> None:
        self._assert_upload_unchanged(entry, snapshot)
        if normalize_adler(adler32_local(Path(entry["resolved_source"]), use_cache=False, cancel_event=self.cancel_event)) != normalize_adler(checksum):
            raise SourceChangedError(f"source changed; retained: {entry['source']}")
        self._assert_upload_unchanged(entry, snapshot)
        if self.delete_source:
            Path(entry["source"]).unlink()

    def _delete_downloaded_source(self, remote_path: str, local_path: Path, checksum: str, snapshot: tuple) -> None:
        self._check_cancelled()
        if _file_fingerprint(local_path.stat()) != snapshot:
            raise SourceChangedError(f"local copy changed; remote source retained: {remote_path}")
        if not self.delete_source:
            return
        self._check_cancelled()
        if normalize_adler(self._remote_adler(remote_path)) != normalize_adler(checksum):
            raise SourceChangedError(f"remote source changed; retained: {remote_path}")
        if normalize_adler(adler32_local(local_path, use_cache=False, cancel_event=self.cancel_event)) != normalize_adler(checksum):
            raise SourceChangedError(f"local copy changed; remote source retained: {remote_path}")
        if _file_fingerprint(local_path.stat()) != snapshot:
            raise SourceChangedError(f"local copy changed; remote source retained: {remote_path}")
        self._check_cancelled()
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
    except BaseException:
        pool.stop()
        raise
    finally:
        pool.join(timeout=1)


def _handle_failed_result(entry: dict, exc: BaseException, progress: Progress, bar: ProgressBar):
    stage_key = entry.get("remote_path")
    if isinstance(stage_key, str):
        stage_key = "/" + stage_key.strip("/")
    progress.failure(entry["rel"], entry.get("size", 0), exc, stage_key=stage_key if isinstance(stage_key, str) else None)
    bar.finish()
    LOG.error("%s\u2718%s %s: %s", _C.RED, _C.RESET, entry["rel"], exc)
    bar.update(entry["rel"])
    return None


def _handle_completed_result(result: dict, entry: dict, progress: Progress, bar: ProgressBar):
    skipped = result.get("skipped", False)
    stage_key = entry.get("remote_path")
    if isinstance(stage_key, str):
        stage_key = "/" + stage_key.strip("/")
    progress.success(
        entry["rel"], entry.get("size", 0),
        attempts=result.get("attempt", 1),
        skipped=skipped,
        stage_key=stage_key if isinstance(stage_key, str) else None,
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


def _filter_verified_download_entries(
    files: list[dict],
    transferer: Transferer,
) -> tuple[list[dict], list[dict]]:
    """Remove download entries whose local target already matches the remote checksum."""
    remaining: list[dict] = []
    skipped: list[dict] = []

    for entry in files:
        local_path = Path(entry["local_path"])
        if not transferer.skip_verified or not local_path.exists():
            remaining.append(entry)
            continue

        remote_path = str(entry["remote_path"]).strip("/")
        try:
            remote_adler = transferer._remote_adler(remote_path)
            snapshot = _file_fingerprint(local_path.stat())
            local_adler = adler32_local(local_path, use_cache=not transferer.delete_source)
            if normalize_adler(local_adler) == normalize_adler(remote_adler):
                transferer._delete_downloaded_source(remote_path, local_path, local_adler, snapshot)
                LOG.debug("skip %s (verified before staging)", entry["rel"])
                skipped.append(entry)
                continue
        except Exception:
            LOG.debug("pre-stage checksum comparison failed for %s; downloading", entry["rel"])

        remaining.append(entry)

    return remaining, skipped


def _build_stage_batches(files: list[dict], max_files: int, max_bytes: int) -> list[list[dict]]:
    """Split download entries into batches bounded by file count and staged bytes."""
    batches: list[list[dict]] = []
    current: list[dict] = []
    current_bytes = 0

    for entry in files:
        entry_size = max(int(entry.get("size", 0)), 0)
        if entry_size > max_bytes:
            raise ValueError(f"file {entry['remote_path']} exceeds --stage-batch-bytes; increase the limit")
        if current and (len(current) >= max_files or current_bytes + entry_size > max_bytes):
            batches.append(current)
            current = []
            current_bytes = 0
        current.append(entry)
        current_bytes += entry_size

    if current:
        batches.append(current)
    return batches


def _format_stage_timeout_error(pending_paths: set[str], stage_errors: dict[str, str]) -> str:
    missing_preview = ", ".join(sorted(pending_paths)[:5])
    missing_suffix = "" if len(pending_paths) <= 5 else f" (+{len(pending_paths) - 5} more)"
    error_lines = [
        f"{path}: {stage_errors[path]}"
        for path in sorted(pending_paths)
        if path in stage_errors
    ]
    if error_lines:
        preview = "; ".join(error_lines[:3])
        suffix = "" if len(error_lines) <= 3 else f" (+{len(error_lines) - 3} more)"
        return (
            f"staging timed out; {len(pending_paths)} files still not online: {missing_preview}{missing_suffix}. "
            f"Errors seen: {preview}{suffix}"
        )
    return f"staging timed out; {len(pending_paths)} files still not online: {missing_preview}{missing_suffix}"


def _destage_paths(
    stage_mgr: StageManager,
    remote_paths: list[str],
    batch_num: int,
    bar: ProgressBar | None = None,
) -> list[str]:
    if not remote_paths:
        return []
    failures: list[str] = []
    if bar is not None:
        bar.finish()
    try:
        stage_mgr.unstage(remote_paths)
        LOG.debug("destaged %d path(s) from batch %d", len(remote_paths), batch_num)
    except Exception as exc:
        LOG.warning("destage failed for batch %d: %s", batch_num, exc)
        failures = list(remote_paths)
    if bar is not None:
        bar.update()
    return failures


def _execute_pipeline_download(
    files: list[dict], transferer: Transferer, stage_mgr: StageManager,
    workers: int, progress: Progress, bar: ProgressBar, stage_batch: int,
    stage_batch_bytes: int, stage_lifetime: str, stage_poll: int,
    stage_timeout: int, destage: bool,
):
    """Stage unique sources in bounded batches, retaining every output request."""
    moving = getattr(transferer, "delete_source", False) is True
    _validate_transfer_plan(files, "download", moving)
    by_path: dict[str, list[dict]] = {}
    for entry in files:
        path = "/" + entry["remote_path"].strip("/")
        by_path.setdefault(path, []).append(entry)
    representatives = [dict(entries[0], size=max(int(e.get("size", 0)) for e in entries))
                       for entries in by_path.values()]
    if not destage and not moving:
        if len(representatives) > stage_batch or sum(e["size"] for e in representatives) > stage_batch_bytes:
            raise ValueError("--no-destage would retain more data than the staging limits; increase the limits or allow destaging")
    batches = _build_stage_batches(representatives, stage_batch, stage_batch_bytes)
    for batch_index, batch in enumerate(batches):
        paths = ["/" + e["remote_path"].strip("/") for e in batch]
        stage_mgr.cancel_event.clear()
        stage_mgr.deadline = time.monotonic() + stage_timeout
        deadline = stage_mgr.deadline
        pending_stage = set(paths)
        pending_downloads = 0
        errors: dict[str, str] = {}
        requested_fallback: set[str] = set()
        initial_errors: dict[str, str] = {}
        requests: list[str] = []
        pool = _DaemonWorkerPool(transferer.download, workers, name_prefix="stage-download-worker")
        fallback_pool = None
        if stage_mgr.can_prime_via_webdav_range():
            fallback_pool = _DaemonWorkerPool(stage_mgr.prime_via_webdav_range,
                                             min(DEFAULT_STAGE_FALLBACK_WORKERS, len(paths)),
                                             name_prefix="stage-fallback-worker")
        def fail_path(path, exc):
            pending_stage.discard(path)
            for entry in by_path[path]:
                _handle_failed_result(entry, exc, progress, bar)

        try:
            LOG.info("batch %d/%d: staging %d unique file(s)", batch_index + 1, len(batches), len(paths))
            try:
                requests = stage_mgr.stage(paths, lifetime=stage_lifetime)
            except (RuntimeError, subprocess.SubprocessError, TimeoutError) as exc:
                if fallback_pool is None:
                    raise
                initial_errors = {path: str(exc) for path in paths}
                LOG.info("initial ADA staging failed; trying WebDAV range-read fallback")
            for entry in batch:
                progress.mark_stage_requested("/" + entry["remote_path"].strip("/"), int(entry.get("size", 0)))
            next_poll = 0.0
            while pending_stage or pending_downloads:
                if pending_stage and time.monotonic() >= deadline:
                    exc = TimeoutError(_format_stage_timeout_error(pending_stage, errors))
                    for path in list(pending_stage):
                        fail_path(path, exc)
                if fallback_pool is not None:
                    while True:
                        try:
                            entry, exc, result = fallback_pool.get_result_nowait()
                        except queue.Empty:
                            break
                        path = "/" + entry["remote_path"].strip("/")
                        if path not in pending_stage:
                            continue
                        if exc is not None:
                            fail_path(path, exc)
                        else:
                            timed_out = bool((result or {}).get("timed_out"))
                            errors[path] = "WebDAV read timed out; waiting for ONLINE" if timed_out else "WebDAV priming completed; waiting for ONLINE"
                if pending_stage and time.monotonic() >= next_poll:
                    try:
                        failures = stage_mgr.poll_stage_request_errors(requests, pending_stage)
                        failures.update(initial_errors)
                        initial_errors.clear()
                        for path, message in failures.items():
                            if path not in pending_stage:
                                continue
                            errors[path] = message
                            if fallback_pool is None:
                                fail_path(path, RuntimeError(message))
                            elif path not in requested_fallback:
                                fallback_pool.submit(dict(by_path[path][0], remote_path=path))
                                requested_fallback.add(path)
                        online, poll_errors = stage_mgr.poll_online_statuses(list(pending_stage)) if pending_stage else ([], {})
                        errors.update(poll_errors)
                        for path in online:
                            if path not in pending_stage:
                                continue
                            pending_stage.remove(path)
                            progress.mark_stage_online(path, int(by_path[path][0].get("size", 0)))
                            for entry in by_path[path]:
                                pool.submit(entry)
                                pending_downloads += 1
                    except (TimeoutError, subprocess.TimeoutExpired) as exc:
                        for path in list(pending_stage):
                            fail_path(path, exc)
                    next_poll = time.monotonic() + stage_poll
                if pending_downloads:
                    try:
                        entry, exc, result = pool.get_result(timeout=0.2)
                    except queue.Empty:
                        pass
                    else:
                        pending_downloads -= 1
                        _handle_worker_result(entry, exc, result, progress, bar)
                elif pending_stage:
                    time.sleep(max(0, min(0.2, next_poll - time.monotonic(), deadline - time.monotonic())))
                bar.update()
        except BaseException:
            pool.stop()
            if fallback_pool is not None:
                fallback_pool.stop()
            raise
        finally:
            pool.finish_submissions()
            pool.join(timeout=1)
            if fallback_pool is not None:
                fallback_pool.stop()
                fallback_pool.join(timeout=1)
            stage_mgr.deadline = None
            progress.clear_stage_states([(p, int(by_path[p][0].get("size", 0))) for p in paths])
            if destage:
                failures = _destage_paths(stage_mgr, paths, batch_index + 1, bar)
                if failures:
                    raise RuntimeError("could not release batch pins; refusing to stage more data")
        # Failed moves can leave pins behind, so do not start another full batch.
        if moving and progress.failed:
            return


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
    p.add_argument("--ada",
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
                    help="Max files to stage at a time (default: 10000)")
    p.add_argument("--stage-batch-bytes", type=int, default=DEFAULT_STAGE_BATCH_BYTES,
                    help="Max bytes to stage at a time (default: 5497558138880 = 5 TiB)")
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
        prefix = files[0]["_prefix"]

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
        if len(set(src_prefixes)) > 1:
            LOG.error("mixed remote prefixes are not supported; use one remote per command")
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
                                                 spinner=spinner, move=delete_source))
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

    if args.retry_wait < 0 or args.checksum_timeout <= 0 or args.stage_timeout <= 0 or args.stage_poll < 0:
        LOG.error("retry/poll intervals must be >= 0 and checksum/stage timeouts must be > 0")
        return 1

    # ---- Resolve config ----
    rclone_config = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(rclone_config)
    remote = resolve_remote_name(config, args.remote)
    api = resolve_api_url(args.api, config[remote])

    if args.file_list or direction == "upload":
        _validate_transfer_plan(files, direction, delete_source)

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
                                           dst_path_dir, args.recursive,
                                           spinner=spinner))

    _validate_transfer_plan(files, direction, delete_source)

    # ---- Validate ----
    if args.workers < 1:
        LOG.error("--workers must be >= 1"); return 1
    if args.max_retries < 0:
        LOG.error("--max-retries must be >= 0"); return 1
    if args.stage_batch < 1:
        LOG.error("--stage-batch must be >= 1"); return 1
    if args.stage_batch_bytes < 1:
        LOG.error("--stage-batch-bytes must be >= 1"); return 1

    if not files:
        LOG.info("no files to process")
        return 0

    planned_files = files
    planned_total_files = len(planned_files)
    planned_total_bytes = sum(e.get("size", 0) for e in planned_files)

    # ---- Dry run ----
    # Resume verification can delete matching sources for moves. Return before
    # constructing the transfer engine or doing any checksum/staging work.
    if args.dry_run:
        LOG.info("dry run (%s): %d file(s), %s", direction, len(files), format_bytes(planned_total_bytes))
        for e in files:
            if direction == "upload":
                LOG.info("  %s -> %s (%s)", e["rel"], e["remote_path"], format_bytes(e.get("size", 0)))
            else:
                LOG.info("  %s -> %s (%s)", e["remote_path"], e.get("local_path", "?"), format_bytes(e.get("size", 0)))
        return 0

    args.ada = args.ada or _default_ada()

    # ---- Transferer ----
    transferer = Transferer(
        rclone_config=rclone_config, remote=remote, ada_cmd=args.ada,
        api=api, max_retries=args.max_retries, retry_wait=args.retry_wait,
        copy_timeout=args.copy_timeout, checksum_timeout=args.checksum_timeout,
        skip_verified=args.skip_verified, delete_source=delete_source,
    )

    pre_skipped_entries: list[dict] = []
    if direction == "download" and args.skip_verified:
        LOG.info("resume  : checking for already verified local files before download")
        files, pre_skipped_entries = _filter_verified_download_entries(files, transferer)

    total_bytes = planned_total_bytes

    # ---- Quota tracker ----
    quota: QuotaTracker | None = None
    quota_poller: _QuotaPoller | None = None
    quota_pool = args.quota_pool or resolve_pool_for_config(rclone_config)
    if quota_pool:
        quota = QuotaTracker(args.ada, rclone_config, api, quota_pool, remote=remote)
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
    LOG.info("%sfiles%s   : %s%d%s (%s)", _C.DIM, _C.RESET, _C.CYAN, planned_total_files, _C.RESET, format_bytes(total_bytes))
    LOG.info("%sworkers%s : %d  retries: %d  skip-verified: %s",
             _C.DIM, _C.RESET, args.workers, args.max_retries, "yes" if args.skip_verified else "no")
    if delete_source:
        LOG.info("%ssource%s  : delete after verified transfer", _C.DIM, _C.RESET)
    if direction == "download" and not args.no_stage:
        LOG.info("%sstaging%s : max-files=%d  max-bytes=%s  lifetime=%s  poll=%ds  timeout=%s",
                 _C.DIM, _C.RESET, args.stage_batch, format_bytes(args.stage_batch_bytes), args.stage_lifetime, args.stage_poll,
                 fmt_duration(args.stage_timeout))
    if pre_skipped_entries:
        LOG.info("%sresume%s  : %s%d%s already verified, skipping stage/copy for %s",
                 _C.DIM, _C.RESET, _C.CYAN, len(pre_skipped_entries), _C.RESET,
                 format_bytes(sum(e.get("size", 0) for e in pre_skipped_entries)))
    if quota and quota.ok:
        LOG.info("%squota%s   : %s", _C.DIM, _C.RESET, quota.summary_line())
    LOG.info("")

    if not files:
        LOG.info("all planned download files are already verified locally")
        return 0

    progress = Progress(total_files=planned_total_files, total_bytes=total_bytes)
    for entry in pre_skipped_entries:
        progress.success(entry["rel"], entry.get("size", 0), attempts=0, skipped=True)
    transferer.progress = progress
    bar = ProgressBar(progress, quota=quota)

    interrupted = False
    try:
        if direction == "upload":
            _execute_simple(files, transferer.upload, args.workers, progress, bar)
        elif args.no_stage:
            _execute_simple(files, transferer.download, args.workers, progress, bar)
        else:
            stage_mgr = StageManager(args.ada, rclone_config, api, config[remote])
            _execute_pipeline_download(
                files=files,
                transferer=transferer,
                stage_mgr=stage_mgr,
                workers=args.workers,
                progress=progress,
                bar=bar,
                stage_batch=args.stage_batch,
                stage_batch_bytes=args.stage_batch_bytes,
                stage_lifetime=args.stage_lifetime,
                stage_poll=args.stage_poll,
                stage_timeout=args.stage_timeout,
                destage=(not args.no_destage) and (not delete_source),
            )
    except KeyboardInterrupt:
        transferer.cancel()
        interrupted = True
    except (RuntimeError, subprocess.SubprocessError, TimeoutError) as exc:
        progress.failure("pipeline", 0, exc)
        LOG.error("pipeline failed: %s", exc)
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
    return 1 if progress.failed or progress.validated_files != planned_total_files else 0


def _run_entry_point(*, prog: str, delete_source: bool) -> None:
    try:
        raise SystemExit(main(prog=prog, delete_source=delete_source))
    except KeyboardInterrupt:
        LOG.warning("interrupted")
        raise SystemExit(130)
    except (FileNotFoundError, ValueError, RuntimeError, subprocess.SubprocessError, TimeoutError) as exc:
        LOG.error("%s", exc)
        raise SystemExit(1)


def entry_point():
    """Console-script entry point."""
    _run_entry_point(prog="dcache_cp", delete_source=False)


if __name__ == "__main__":
    entry_point()
