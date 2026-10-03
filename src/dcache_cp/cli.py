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
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from pathlib import Path

from . import __version__
from .tempfiles import get_temp_root, mkdtemp as dcache_mkdtemp, named_tempfile
from .bundles import (
    DEFAULT_BUNDLE_FORMAT,
    DEFAULT_BUNDLE_MAX_FILE_SIZE,
    DEFAULT_BUNDLE_MAX_MEMBERS,
    DEFAULT_BUNDLE_MIN_DIR_TOTAL,
    DEFAULT_BUNDLE_TARGET_SIZE,
    BundleOptions,
    BundleDeprecationPlan,
    BundleDeprecationRecord,
    BundleDeletedRecord,
    BundleMember,
    BundleMemberMetadata,
    AnchorBundlePlan,
    BundleUploadPlan,
    build_anchor_xattrs_for_routes,
    build_bundle_deleted_xattrs,
    build_bundle_deprecated_xattrs,
    build_bundle_member,
    bundle_object_remote_path,
    build_anchor_xattrs,
    decode_bundle_deleted,
    decode_bundle_deprecated,
    decode_anchor_routes,
    decode_bundle_members,
    make_bundle_generation,
    materialize_bundle_job,
    plan_anchor_bundle_jobs,
    plan_bundle_anchor_groups,
)
from .xattrs import NamespaceXattrClient, NamespaceXattrError, extract_bearer_token

LOG = logging.getLogger("dcache_cp")

DEFAULT_COPY_TIMEOUT = "300m"
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
    get_temp_root() / f"dcache_cp_{os.getenv('USER', 'user')}",
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


_SIZE_LITERAL_RE = re.compile(r"^\s*(\d+)\s*([kmgtp]?i?b?)?\s*$", re.IGNORECASE)


def parse_size_literal(value: str) -> int:
    match = _SIZE_LITERAL_RE.match(str(value))
    if not match:
        raise argparse.ArgumentTypeError(f"invalid size literal: {value!r}")
    amount = int(match.group(1))
    suffix = (match.group(2) or "").lower()
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "ki": 1024,
        "kib": 1024,
        "m": 1024 ** 2,
        "mb": 1024 ** 2,
        "mi": 1024 ** 2,
        "mib": 1024 ** 2,
        "g": 1024 ** 3,
        "gb": 1024 ** 3,
        "gi": 1024 ** 3,
        "gib": 1024 ** 3,
        "t": 1024 ** 4,
        "tb": 1024 ** 4,
        "ti": 1024 ** 4,
        "tib": 1024 ** 4,
        "p": 1024 ** 5,
        "pb": 1024 ** 5,
        "pi": 1024 ** 5,
        "pib": 1024 ** 5,
    }
    if suffix not in multipliers:
        raise argparse.ArgumentTypeError(f"invalid size suffix in {value!r}")
    return amount * multipliers[suffix]


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


def _read_shell_config_value(path: Path, key: str) -> str | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() != key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        return value.strip() or None
    return None


def _read_ada_default_api() -> str | None:
    vendor_default = Path(__file__).resolve().parent / "vendor" / "etc" / "ada.conf"
    candidates = [vendor_default, Path("/etc/ada.conf"), Path.home() / ".ada" / "ada.conf"]
    api: str | None = None
    for candidate in candidates:
        value = _read_shell_config_value(candidate, "api")
        if value:
            api = value
    return api


def _infer_api_url_from_remote(remote_cfg: configparser.SectionProxy) -> str | None:
    webdav_url = (remote_cfg.get("url", fallback=None) or "").strip()
    if not webdav_url:
        return None
    parsed = urllib.parse.urlsplit(webdav_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/api/v1", "", ""))


def resolve_api_url(explicit: str | None, remote_cfg: configparser.SectionProxy) -> str | None:
    return (
        explicit
        or os.environ.get("DCACHE_API")
        or os.environ.get("ADA_API")
        or os.environ.get("ada_api")
        or remote_cfg.get("api", fallback=None)
        or _read_ada_default_api()
        or _infer_api_url_from_remote(remote_cfg)
    )


# ---------------------------------------------------------------------------
# Shell commands
# ---------------------------------------------------------------------------

def run_command(cmd: list[str], check: bool = True,
                log_errors: bool = True) -> subprocess.CompletedProcess:
    """Run a subprocess and return its result.

    *log_errors*: when True (default), log stdout/stderr at ERROR on failure.
    Set to False when the caller expects and handles certain non-zero exits
    (e.g. rclone lsjson on missing bundled dirs, ada 429 rate-limits) to
    avoid spurious ERROR lines.
    """
    LOG.debug("cmd: %s", " ".join(shlex.quote(str(x)) for x in cmd))
    try:
        result = subprocess.run(cmd, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as exc:
        level = logging.ERROR if log_errors else logging.DEBUG
        if exc.stdout:
            LOG.log(level, "stdout: %s", exc.stdout.strip())
        if exc.stderr:
            LOG.log(level, "stderr: %s", exc.stderr.strip())
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


def _ada_tokenfile_cmd(ada_cmd: str, tokenfile: Path, api: str | None) -> list[str]:
    cmd = [ada_cmd, "--tokenfile", str(tokenfile)]
    # When ada already has a tokenfile, let it resolve the matching API itself.
    return cmd


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


def _ada_reports_transient_transport_error(output: str) -> bool:
    """Return True when ada/curl output looks like a retryable transport issue."""
    text = output.lower()
    markers = (
        "curl: (6)",
        "could not resolve host",
        "temporary failure in name resolution",
        "curl: (7)",
        "failed to connect",
        "connection refused",
        "network is unreachable",
        "curl: (28)",
        "operation timed out",
        "connection timed out",
        "timeout was reached",
        "curl: (52)",
        "empty reply from server",
        "curl: (55)",
        "send failure",
        "curl: (56)",
        "recv failure",
        "failure when receiving data from the peer",
    )
    return any(marker in text for marker in markers)


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

    result = run_command(cmd, check=not missing_ok, log_errors=False)
    if result.returncode != 0:
        if missing_ok:
            return []
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    entries = json.loads(result.stdout)
    if not isinstance(entries, list):
        raise ValueError(f"unexpected lsjson output for {target}")
    return entries


def _rclone_path_is_file(rclone_config: Path, remote: str, remote_path: str) -> bool:
    """Return True if ``remote_path`` is a regular file (not a directory).

    Uses ``rclone lsjson --stat`` which describes the path *itself*: a file
    yields ``IsDir=false`` with ``Path`` set to its basename, a directory yields
    ``IsDir=true`` with an empty ``Path``.  Used to disambiguate a single-file
    source from a directory whose one child happens to share its name.
    """
    remote_path = remote_path.strip("/")
    target = f"{remote}:{remote_path}" if remote_path else f"{remote}:"
    cmd = [
        "rclone", "--config", str(rclone_config),
        "lsjson", "--stat", target,
    ]
    result = run_command(cmd, check=False, log_errors=False)
    if result.returncode != 0:
        return False
    try:
        info = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return False
    return isinstance(info, dict) and not info.get("IsDir", True)


def _rclone_lsjson_reports_missing_path(exc: subprocess.CalledProcessError) -> bool:
    text = "\n".join(
        part.strip()
        for part in (exc.stdout, exc.stderr)
        if isinstance(part, str) and part.strip()
    ).lower()
    return any(
        needle in text
        for needle in (
            "directory not found",
            "object not found",
            "file not found",
        )
    )


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
                     "size": st.st_size, "remote_path": posixpath.join(root, rel), "bundle_root": root})
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

    # ``rclone lsjson`` on a *file* returns a single non-dir entry whose ``Path``
    # is the file's own basename.  In that case ``remote_path`` already is the
    # full file path, so joining it with the basename would yield ``<file>/<file>``
    # and a 404.  Confirm with an explicit stat to avoid mis-detecting a directory
    # that contains exactly one equally-named child file.
    source_is_file = (
        len(entries) == 1
        and not entries[0].get("IsDir", False)
        and entries[0].get("Path") == posixpath.basename(remote_path)
        and _rclone_path_is_file(rclone_config, remote, remote_path)
    )

    out: list[dict] = []
    for entry in entries:
        if entry.get("IsDir", False):
            continue
        rel = entry["Path"]
        size = entry.get("Size", 0)
        file_remote = remote_path if source_is_file else posixpath.join(remote_path, rel)
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


def _bundle_members_for_entry(entry: dict) -> list[dict]:
    members = entry.get("bundle_members")
    if isinstance(members, list):
        return members
    if isinstance(members, tuple):
        return list(members)
    return []


def _entry_logical_file_count(entry: dict) -> int:
    bundle_members = _bundle_members_for_entry(entry)
    return len(bundle_members) if bundle_members else 1


def _entry_logical_bytes(entry: dict) -> int:
    bundle_members = _bundle_members_for_entry(entry)
    if bundle_members:
        return sum(int(member.get("size", 0)) for member in bundle_members)
    return int(entry.get("size", 0))


def _entry_stage_bytes(entry: dict) -> int:
    return max(int(entry.get("stage_size", entry.get("size", 0))), 0)


def _bundle_member_matches_remote(local_member: BundleMember, remote_member: BundleMemberMetadata) -> bool:
    return (
        local_member.anchor_rel == remote_member.anchor_rel
        and normalize_adler(local_member.adler32) == normalize_adler(remote_member.adler32)
        and local_member.size == remote_member.size
        and local_member.mtime_ns == remote_member.mtime_ns
        and local_member.mode == remote_member.mode
    )


class _BundleUploadResolver:
    def __init__(
        self,
        rclone_config: Path,
        remote: str,
        api: str | None,
        remote_cfg: configparser.SectionProxy | None,
    ):
        self.rclone_config = rclone_config
        self.remote = remote
        self.api = api
        self.remote_cfg = remote_cfg
        self.xattr_client: NamespaceXattrClient | None = None
        self._anchor_state_cache: dict[str, tuple[str | None, dict[str, str]]] = {}
        self._bundle_state_cache: dict[str, dict[str, object] | None] = {}

    def _require_client(self) -> NamespaceXattrClient:
        if self.xattr_client is None:
            if not self.api:
                raise RuntimeError(
                    "bundled uploads require a resolved dCache API URL"
                )
            token = extract_bearer_token(self.remote_cfg, self.rclone_config)
            if not token:
                raise RuntimeError(
                    "bundled uploads require a bearer token in the selected config"
                )
            self.xattr_client = NamespaceXattrClient(self.api, token)
        return self.xattr_client

    def anchor_state(self, anchor_dir: str) -> tuple[str | None, dict[str, str]]:
        anchor_dir = str(anchor_dir).strip("/")
        if anchor_dir in self._anchor_state_cache:
            generation, routes = self._anchor_state_cache[anchor_dir]
            return generation, dict(routes)

        try:
            xattrs = self._require_client().list_xattrs(anchor_dir)
        except NamespaceXattrError as exc:
            if exc.status == 404:
                self._anchor_state_cache[anchor_dir] = (None, {})
                return None, {}
            raise

        generation = xattrs.get("dcache_cp.bundle_anchor.active_generation")
        routes = decode_anchor_routes(xattrs)
        self._anchor_state_cache[anchor_dir] = (generation, routes)
        return generation, dict(routes)

    def bundle_state_by_remote_path(self, remote_path: str) -> dict[str, object] | None:
        remote_path = str(remote_path).strip("/")
        if remote_path in self._bundle_state_cache:
            cached = self._bundle_state_cache[remote_path]
            return dict(cached) if isinstance(cached, dict) else None

        try:
            xattrs = self._require_client().list_xattrs(remote_path)
        except NamespaceXattrError as exc:
            if exc.status == 404:
                self._bundle_state_cache[remote_path] = None
                return None
            raise

        state: dict[str, object] = {
            "remote_path": remote_path,
            "bundle_id": xattrs.get("dcache_cp.bundle.id") or Path(remote_path).stem,
            "anchor_dir": xattrs.get("dcache_cp.bundle.anchor_dir") or _bundle_anchor_dir_from_object(remote_path) or "",
            "format": xattrs.get("dcache_cp.bundle.format") or DEFAULT_BUNDLE_FORMAT,
            "members": decode_bundle_members(xattrs),
            "deleted": decode_bundle_deleted(xattrs),
            "deprecated": decode_bundle_deprecated(xattrs),
            "xattrs": xattrs,
        }
        self._bundle_state_cache[remote_path] = state
        return dict(state)

    def bundle_details(self, anchor_dir: str, bundle_id: str) -> dict[str, object] | None:
        return self.bundle_state_by_remote_path(bundle_object_remote_path(anchor_dir, bundle_id))

    def resolve_existing_route(self, remote_path: str) -> dict[str, str] | None:
        remote_path = str(remote_path).strip("/")
        if _is_bundle_object_path(remote_path):
            return None
        for anchor_dir in _candidate_anchor_dirs_for_remote_path(remote_path):
            _, routes = self.anchor_state(anchor_dir)
            anchor_rel = posixpath.relpath(remote_path, anchor_dir) if anchor_dir else remote_path
            bundle_id = routes.get(anchor_rel)
            if bundle_id:
                return {
                    "anchor_dir": anchor_dir,
                    "anchor_rel": anchor_rel,
                    "bundle_id": bundle_id,
                }
        return None

    def preferred_anchor_for_directory(self, remote_dir: str) -> str | None:
        remote_dir = str(remote_dir).strip("/")
        probe_path = posixpath.join(remote_dir, ".dcache-cp-anchor-probe") if remote_dir else ".dcache-cp-anchor-probe"
        for anchor_dir in _candidate_anchor_dirs_for_remote_path(probe_path):
            _, routes = self.anchor_state(anchor_dir)
            if not routes:
                continue
            dir_rel = posixpath.relpath(remote_dir, anchor_dir) if anchor_dir else remote_dir
            if dir_rel in {"", "."}:
                return anchor_dir
            prefix = dir_rel.strip("/") + "/"
            if any(route.startswith(prefix) for route in routes):
                return anchor_dir
        return None


def _entry_transfer_root(entry: dict) -> str:
    bundle_root = entry.get("bundle_root")
    if bundle_root is not None:
        return str(bundle_root).strip("/")
    return posixpath.dirname(str(entry["remote_path"]).strip("/"))


def _bundle_transfer_roots(files: list[dict]) -> set[str]:
    return {_entry_transfer_root(entry) for entry in files}


def _build_incremental_anchor_bundle_plan(
    anchor_dir: str,
    anchor_entries: list[dict],
    options: BundleOptions,
    resolver: _BundleUploadResolver,
) -> AnchorBundlePlan | None:
    existing_generation, existing_routes = resolver.anchor_state(anchor_dir)
    existing_route_map = dict(existing_routes)

    reused_members: list[BundleMember] = []
    changed_entries: list[dict] = []
    overridden_bundle_ids: dict[str, str] = {}
    for entry in sorted(anchor_entries, key=lambda item: str(item["remote_path"])):
        local_member = build_bundle_member(entry, anchor_dir=anchor_dir)
        current_bundle_id = existing_route_map.get(local_member.anchor_rel)
        if current_bundle_id:
            details = resolver.bundle_details(anchor_dir, current_bundle_id)
            remote_member = details["members"].get(local_member.anchor_rel) if details else None
            if isinstance(remote_member, BundleMemberMetadata) and _bundle_member_matches_remote(local_member, remote_member):
                reused_members.append(local_member)
                continue
            overridden_bundle_ids[local_member.anchor_rel] = current_bundle_id
        changed_entries.append(entry)

    generation = existing_generation or make_bundle_generation()
    route_map = dict(existing_route_map)
    new_bundles: tuple[object, ...] = ()
    deprecations: list[BundleDeprecationPlan] = []
    commit_required = False
    if changed_entries:
        generation = make_bundle_generation()
        new_bundles = plan_anchor_bundle_jobs(anchor_dir, changed_entries, options, generation=generation)
        for bundle in new_bundles:
            for member in bundle.members:
                route_map[member.anchor_rel] = bundle.bundle_id

        deprecations_by_bundle_id: dict[str, list[BundleDeprecationRecord]] = {}
        for anchor_rel, old_bundle_id in overridden_bundle_ids.items():
            replacement_bundle_id = route_map.get(anchor_rel)
            if replacement_bundle_id and replacement_bundle_id != old_bundle_id:
                deprecations_by_bundle_id.setdefault(old_bundle_id, []).append(
                    BundleDeprecationRecord(
                        anchor_rel=anchor_rel,
                        replacement_bundle_id=replacement_bundle_id,
                        replacement_generation=generation,
                    )
                )
        deprecations = [
            BundleDeprecationPlan(
                bundle_id=bundle_id,
                remote_path=bundle_object_remote_path(anchor_dir, bundle_id),
                members=tuple(sorted(records, key=lambda item: item.anchor_rel)),
            )
            for bundle_id, records in sorted(deprecations_by_bundle_id.items())
        ]
        commit_required = True

    if not new_bundles and not reused_members:
        return None

    return AnchorBundlePlan(
        anchor_dir=anchor_dir,
        generation=generation,
        bundles=tuple(new_bundles),
        route_map=tuple(sorted(route_map.items())),
        reused_members=tuple(reused_members),
        deprecations=tuple(deprecations),
        commit_required=commit_required,
    )


def _build_incremental_bundle_upload_plan(
    files: list[dict],
    options: BundleOptions,
    resolver: _BundleUploadResolver,
) -> BundleUploadPlan:
    options.validate()
    transfer_roots = _bundle_transfer_roots(files)
    multiple_transfer_roots = len(transfer_roots) > 1
    plain_entries: list[dict] = []
    anchor_entries: dict[str, list[dict]] = {}
    deferred_entries: list[dict] = []
    conflicting_plain_paths: list[str] = []

    for entry in sorted(files, key=lambda item: str(item["remote_path"])):
        remote_path = str(entry["remote_path"]).strip("/")
        source = Path(entry["resolved_source"])
        transfer_root = _entry_transfer_root(entry)
        existing_route = resolver.resolve_existing_route(remote_path)
        if not source.is_file() or int(entry.get("size", 0)) > options.max_file_size:
            if existing_route is not None:
                conflicting_plain_paths.append(remote_path)
                continue
            plain_entries.append(entry)
            continue

        if existing_route is not None:
            anchor_entries.setdefault(existing_route["anchor_dir"], []).append(entry)
            continue

        preferred_anchor = resolver.preferred_anchor_for_directory(posixpath.dirname(remote_path))
        if preferred_anchor is not None:
            if multiple_transfer_roots:
                allow_preferred_anchor = (
                    preferred_anchor == transfer_root
                    or _remote_path_within(transfer_root, preferred_anchor)
                )
            else:
                allow_preferred_anchor = (
                    preferred_anchor == transfer_root
                    or _remote_path_within(preferred_anchor, transfer_root)
                    or _remote_path_within(transfer_root, preferred_anchor)
                )
            if allow_preferred_anchor:
                anchor_entries.setdefault(preferred_anchor, []).append(entry)
                continue
        deferred_entries.append(entry)

    deferred_plain_entries, deferred_anchor_groups = plan_bundle_anchor_groups(deferred_entries, options)
    for entry in deferred_plain_entries:
        existing_route = resolver.resolve_existing_route(entry["remote_path"])
        if existing_route is not None:
            conflicting_plain_paths.append(str(entry["remote_path"]).strip("/"))
            continue
        plain_entries.append(entry)

    if conflicting_plain_paths:
        preview = ", ".join(conflicting_plain_paths[:5])
        suffix = "" if len(conflicting_plain_paths) <= 5 else f" (+{len(conflicting_plain_paths) - 5} more)"
        raise ValueError(
            f"bundled upload sync cannot replace existing bundled files with plain files: {preview}{suffix}"
        )

    for anchor_group in deferred_anchor_groups:
        anchor_entries.setdefault(anchor_group.anchor_dir, []).extend(anchor_group.entries)

    anchors: list[AnchorBundlePlan] = []
    for anchor_dir, grouped_entries in sorted(anchor_entries.items(), key=lambda item: item[0]):
        anchor_plan = _build_incremental_anchor_bundle_plan(anchor_dir, grouped_entries, options, resolver)
        if anchor_plan is not None:
            anchors.append(anchor_plan)

    return BundleUploadPlan(
        plain_entries=tuple(plain_entries),
        anchors=tuple(anchors),
    )


def _is_bundle_object_path(remote_path: str) -> bool:
    parts = [part for part in str(remote_path).strip("/").split("/") if part]
    return len(parts) >= 3 and parts[-3] == ".dcpacks" and parts[-2] == "bundles" and parts[-1].endswith(".dcpbundle")


def _bundle_anchor_dir_from_object(remote_path: str) -> str | None:
    if not _is_bundle_object_path(remote_path):
        return None
    parts = [part for part in str(remote_path).strip("/").split("/") if part]
    return "/".join(parts[:-3])


def _remote_path_within(root: str, remote_path: str) -> bool:
    clean_root = str(root).strip("/")
    clean_path = str(remote_path).strip("/")
    if not clean_root:
        return True
    return clean_path == clean_root or clean_path.startswith(clean_root + "/")


def _candidate_anchor_dirs_for_remote_path(remote_path: str) -> list[str]:
    clean = str(remote_path).strip("/")
    current = posixpath.dirname(clean)
    out: list[str] = []
    while True:
        out.append(current)
        if not current:
            return out
        next_current = posixpath.dirname(current)
        if next_current == current:
            out.append("")
            return out
        current = next_current


def _lookup_remote_file_size(rclone_config: Path, remote: str, remote_path: str) -> int:
    remote_path = str(remote_path).strip("/")
    parent = posixpath.dirname(remote_path)
    name = posixpath.basename(remote_path)
    entries = _rclone_lsjson(rclone_config, remote, parent, recursive=False)
    for entry in entries:
        if entry.get("IsDir", False):
            continue
        if entry.get("Path") == name:
            return int(entry.get("Size", 0))
    raise FileNotFoundError(f"remote file not found: {remote_path}")


class _BundleDownloadResolver:
    def __init__(
        self,
        rclone_config: Path,
        remote: str,
        api: str | None,
        remote_cfg: configparser.SectionProxy | None,
    ):
        self.rclone_config = rclone_config
        self.remote = remote
        self.api = api
        self.remote_cfg = remote_cfg
        self.xattr_client: NamespaceXattrClient | None = None
        self._anchor_routes_cache: dict[str, dict[str, str]] = {}
        self._bundle_cache: dict[str, dict[str, object]] = {}
        self._bundle_sizes: dict[str, int] = {}

    def _require_client(self) -> NamespaceXattrClient:
        if self.xattr_client is None:
            if not self.api:
                raise RuntimeError(
                    "transparent bundle downloads require a resolved dCache API URL; "
                    "use --no-unpack-bundles to download raw bundle objects"
                )
            token = extract_bearer_token(self.remote_cfg, self.rclone_config)
            if not token:
                raise RuntimeError(
                    "transparent bundle downloads require a bearer token in the selected config; "
                    "use --no-unpack-bundles to download raw bundle objects"
                )
            self.xattr_client = NamespaceXattrClient(self.api, token)
        return self.xattr_client

    def anchor_routes(self, anchor_dir: str) -> dict[str, str]:
        anchor_dir = str(anchor_dir).strip("/")
        if anchor_dir not in self._anchor_routes_cache:
            try:
                xattrs = self._require_client().list_xattrs(anchor_dir)
            except NamespaceXattrError as exc:
                if exc.status == 404:
                    self._anchor_routes_cache[anchor_dir] = {}
                else:
                    raise
            else:
                self._anchor_routes_cache[anchor_dir] = decode_anchor_routes(xattrs)
        return self._anchor_routes_cache[anchor_dir]

    def bundle_details(
        self,
        anchor_dir: str,
        bundle_id: str,
        *,
        bundle_size: int | None = None,
    ) -> dict[str, object]:
        anchor_dir = str(anchor_dir).strip("/")
        bundle_remote_path = bundle_object_remote_path(anchor_dir, bundle_id)
        if bundle_remote_path not in self._bundle_cache:
            xattrs = self._require_client().list_xattrs(bundle_remote_path)
            members = decode_bundle_members(xattrs)
            self._bundle_cache[bundle_remote_path] = {
                "anchor_dir": anchor_dir,
                "bundle_id": bundle_id,
                "remote_path": bundle_remote_path,
                "format": xattrs.get("dcache_cp.bundle.format") or DEFAULT_BUNDLE_FORMAT,
                "members": members,
                "deleted": decode_bundle_deleted(xattrs),
                "deprecated": decode_bundle_deprecated(xattrs),
                "member_count": len(members),
            }
        if bundle_size is not None and bundle_remote_path not in self._bundle_sizes:
            self._bundle_sizes[bundle_remote_path] = int(bundle_size)
        if bundle_remote_path not in self._bundle_sizes:
            self._bundle_sizes[bundle_remote_path] = _lookup_remote_file_size(
                self.rclone_config,
                self.remote,
                bundle_remote_path,
            )
        details = dict(self._bundle_cache[bundle_remote_path])
        details["size"] = self._bundle_sizes[bundle_remote_path]
        return details

    def resolve_logical_entry(self, entry: dict) -> dict[str, object] | None:
        remote_path = str(entry["remote_path"]).strip("/")
        if _is_bundle_object_path(remote_path):
            return None
        for anchor_dir in _candidate_anchor_dirs_for_remote_path(remote_path):
            anchor_rel = posixpath.relpath(remote_path, anchor_dir) if anchor_dir else remote_path
            routes = self.anchor_routes(anchor_dir)
            bundle_id = routes.get(anchor_rel)
            if not bundle_id:
                continue
            details = self.bundle_details(anchor_dir, bundle_id, bundle_size=entry.get("bundle_size"))
            deleted = details.get("deleted")
            if isinstance(deleted, dict) and anchor_rel in deleted:
                continue
            member = details["members"].get(anchor_rel)
            if member is None:
                raise RuntimeError(
                    f"bundle xattrs for {details['remote_path']} do not contain requested member {anchor_rel!r}"
                )
            return {
                "anchor_dir": anchor_dir,
                "anchor_rel": anchor_rel,
                "bundle_id": bundle_id,
                "bundle_remote_path": details["remote_path"],
                "bundle_format": details["format"],
                "bundle_size": details["size"],
                "bundle_member_total": details["member_count"],
                "member": member,
            }
        return None


def _add_bundle_download_request(
    bundle_entries_by_remote: dict[str, dict],
    request_entry: dict,
    resolved: dict[str, object],
) -> None:
    bundle_remote_path = str(resolved["bundle_remote_path"])
    bundle_entry = bundle_entries_by_remote.get(bundle_remote_path)
    if bundle_entry is None:
        bundle_entry = {
            "rel": f"bundle:{bundle_remote_path}",
            "remote_path": bundle_remote_path,
            "size": 0,
            "stage_size": int(resolved["bundle_size"]),
            "bundle_id": str(resolved["bundle_id"]),
            "bundle_anchor_dir": str(resolved["anchor_dir"]),
            "bundle_format": str(resolved["bundle_format"]),
            "bundle_member_total": int(resolved["bundle_member_total"]),
            "bundle_members": [],
            "_member_keys": set(),
        }
        bundle_entries_by_remote[bundle_remote_path] = bundle_entry

    local_path = Path(request_entry["local_path"])
    request_key = (str(request_entry["remote_path"]).strip("/"), str(local_path))
    if request_key in bundle_entry["_member_keys"]:
        return

    member = resolved["member"]
    bundle_entry["_member_keys"].add(request_key)
    bundle_entry["bundle_members"].append({
        "rel": str(request_entry["rel"]),
        "remote_path": str(request_entry["remote_path"]).strip("/"),
        "local_path": local_path,
        "anchor_rel": str(resolved["anchor_rel"]),
        "size": int(member.size),
        "adler32": str(member.adler32),
        "mtime_ns": int(member.mtime_ns),
        "mode": int(member.mode),
    })
    bundle_entry["size"] += int(member.size)


def _finalize_bundle_download_entries(bundle_entries_by_remote: dict[str, dict]) -> list[dict]:
    out: list[dict] = []
    for remote_path in sorted(bundle_entries_by_remote):
        entry = bundle_entries_by_remote[remote_path]
        entry["bundle_members"].sort(key=lambda member: (member["rel"], str(member["local_path"])))
        entry.pop("_member_keys", None)
        out.append(entry)
    return out


def _merge_bundle_download_entries(entries: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for entry in entries:
        remote_path = str(entry["remote_path"])
        current = merged.get(remote_path)
        if current is None:
            current = {
                key: value
                for key, value in entry.items()
                if key != "bundle_members"
            }
            current["bundle_members"] = []
            current["_member_keys"] = set()
            current["size"] = 0
            merged[remote_path] = current
        for member in _bundle_members_for_entry(entry):
            request_key = (str(member["remote_path"]), str(member["local_path"]))
            if request_key in current["_member_keys"]:
                continue
            current["_member_keys"].add(request_key)
            current["bundle_members"].append(member)
            current["size"] += int(member.get("size", 0))
    return _finalize_bundle_download_entries(merged)


def _plan_file_list_downloads_with_bundles(
    rclone_config: Path,
    remote: str,
    files: list[dict],
    resolver: _BundleDownloadResolver,
    *,
    allow_resumed_move: bool = False,
) -> tuple[list[dict], list[dict]]:
    grouped: dict[str, set[str]] = {}
    for entry in files:
        remote_path = str(entry["remote_path"]).strip("/")
        parent = posixpath.dirname(remote_path)
        name = posixpath.basename(remote_path)
        grouped.setdefault(parent, set()).add(name)

    dir_sizes: dict[str, dict[str, int]] = {}
    for parent in grouped:
        try:
            dir_entries = _rclone_lsjson(
                rclone_config,
                remote,
                parent,
                recursive=False,
                missing_ok=allow_resumed_move,
            )
        except subprocess.CalledProcessError as exc:
            if not _rclone_lsjson_reports_missing_path(exc):
                raise
            LOG.debug(
                "treating missing physical directory %s as empty during bundle-aware file-list planning",
                parent or "/",
            )
            dir_entries = []
        dir_sizes[parent] = {
            str(dir_entry.get("Path")): int(dir_entry.get("Size", 0))
            for dir_entry in dir_entries
            if not dir_entry.get("IsDir", False) and isinstance(dir_entry.get("Path"), str)
        }

    plain_entries: list[dict] = []
    bundle_entries_by_remote: dict[str, dict] = {}
    missing: list[str] = []

    for entry in files:
        remote_path = str(entry["remote_path"]).strip("/")
        parent = posixpath.dirname(remote_path)
        name = posixpath.basename(remote_path)
        if name in dir_sizes[parent]:
            plain_entry = dict(entry)
            plain_entry["size"] = dir_sizes[parent][name]
            plain_entries.append(plain_entry)
            continue
        resolved = resolver.resolve_logical_entry(entry)
        if allow_resumed_move and resolved is None and Path(entry["local_path"]).exists():
            LOG.info("skip %s (already moved)", entry["rel"])
            continue
        if resolved is None:
            missing.append(remote_path)
            continue
        _add_bundle_download_request(bundle_entries_by_remote, entry, resolved)

    if missing:
        preview = ", ".join(sorted(missing)[:5])
        suffix = "" if len(missing) <= 5 else f" (+{len(missing) - 5} more)"
        raise FileNotFoundError(f"remote file(s) not found in file list: {preview}{suffix}")

    return plain_entries, _finalize_bundle_download_entries(bundle_entries_by_remote)


def _plan_download_source_with_bundles(
    rclone_config: Path,
    remote: str,
    remote_path: str,
    local_dest: Path,
    recursive: bool,
    resolver: _BundleDownloadResolver,
    spinner: "_EnumSpinner | None" = None,
) -> tuple[list[dict], list[dict]]:
    remote_path = str(remote_path).strip("/")
    local_dest = local_dest.expanduser().resolve()

    if not recursive:
        try:
            return plan_download(rclone_config, remote, remote_path, local_dest, recursive=False, spinner=spinner), []
        except FileNotFoundError:
            pass
        except subprocess.CalledProcessError as exc:
            if not _rclone_lsjson_reports_missing_path(exc):
                raise
            LOG.debug(
                "treating missing physical path %s as a candidate logical bundle member",
                remote_path or "/",
            )

        logical_entry = {
            "remote_path": remote_path,
            "local_path": local_dest,
            "rel": posixpath.basename(remote_path),
        }
        resolved = resolver.resolve_logical_entry(logical_entry)
        if resolved is None:
            raise FileNotFoundError(f"remote file not found: {remote_path}")
        bundle_entries_by_remote: dict[str, dict] = {}
        _add_bundle_download_request(bundle_entries_by_remote, logical_entry, resolved)
        if spinner:
            spinner.tick()
        return [], _finalize_bundle_download_entries(bundle_entries_by_remote)

    physical_entries = plan_download(rclone_config, remote, remote_path, local_dest, recursive=True, spinner=spinner)
    bundle_object_entries = {
        str(entry["remote_path"]).strip("/"): entry
        for entry in physical_entries
        if _is_bundle_object_path(str(entry["remote_path"]).strip("/"))
    }
    if not bundle_object_entries:
        return physical_entries, []

    bundle_entries_by_remote: dict[str, dict] = {}
    referenced_bundle_paths: set[str] = set()

    candidate_anchor_dirs = sorted(
        anchor_dir
        for anchor_dir in {_bundle_anchor_dir_from_object(path) for path in bundle_object_entries}
        if anchor_dir is not None
    )

    for anchor_dir in candidate_anchor_dirs:
        routes = resolver.anchor_routes(anchor_dir)
        if not routes:
            continue
        for anchor_rel, bundle_id in sorted(routes.items()):
            logical_remote_path = posixpath.join(anchor_dir, anchor_rel) if anchor_dir else anchor_rel
            if not _remote_path_within(remote_path, logical_remote_path):
                continue
            bundle_remote_path = bundle_object_remote_path(anchor_dir, bundle_id)
            bundle_entry = bundle_object_entries.get(bundle_remote_path)
            if bundle_entry is None:
                raise FileNotFoundError(
                    f"bundle object missing for logical path {logical_remote_path}: {bundle_remote_path}"
                )
            resolved = resolver.resolve_logical_entry({
                "remote_path": logical_remote_path,
                "local_path": local_dest / posixpath.relpath(logical_remote_path, remote_path),
                "rel": posixpath.relpath(logical_remote_path, remote_path),
                "bundle_size": int(bundle_entry.get("size", 0)),
            })
            if resolved is None:
                continue
            referenced_bundle_paths.add(bundle_remote_path)
            _add_bundle_download_request(bundle_entries_by_remote, {
                "remote_path": logical_remote_path,
                "local_path": local_dest / posixpath.relpath(logical_remote_path, remote_path),
                "rel": posixpath.relpath(logical_remote_path, remote_path),
            }, resolved)

    plain_entries = [
        entry
        for entry in physical_entries
        if not _is_bundle_object_path(str(entry["remote_path"]).strip("/"))
        or str(entry["remote_path"]).strip("/") not in referenced_bundle_paths
    ]
    return plain_entries, _finalize_bundle_download_entries(bundle_entries_by_remote)


def _bundle_entry_fully_verified(entry: dict) -> bool:
    bundle_members = _bundle_members_for_entry(entry)
    if not bundle_members:
        return False
    for member in bundle_members:
        local_path = Path(member["local_path"])
        if not local_path.exists():
            return False
        try:
            local_adler = adler32_local(local_path)
        except Exception:
            return False
        if normalize_adler(local_adler) != normalize_adler(str(member["adler32"])):
            return False
    return True


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
        cmd = _ada_tokenfile_cmd(self.ada_cmd, self.tokenfile, self.api) + ["--space", self.poolgroup]
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
        self.webdav_url = remote_cfg.get("url", fallback=None) if remote_cfg else None
        self.webdav_bearer_token = remote_cfg.get("bearer_token", fallback=None) if remote_cfg else None
        self.webdav_bearer_token_command = remote_cfg.get("bearer_token_command", fallback=None) if remote_cfg else None
        self._resolved_webdav_bearer_token = (self.webdav_bearer_token or "").strip() or None

    def _base_cmd(self) -> list[str]:
        return _ada_tokenfile_cmd(self.ada_cmd, self.tokenfile, self.api)

    def can_prime_via_webdav_range(self) -> bool:
        return bool(self.webdav_url and (self._resolved_webdav_bearer_token or self.webdav_bearer_token_command))

    def _resolve_webdav_bearer_token(self) -> str | None:
        if self._resolved_webdav_bearer_token:
            return self._resolved_webdav_bearer_token
        if not self.webdav_bearer_token_command:
            return None
        try:
            cmd = shlex.split(self.webdav_bearer_token_command)
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except Exception as exc:
            LOG.warning("could not obtain WebDAV bearer token from configured command: %s", exc)
            return None
        token = ""
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if stripped:
                token = stripped
        self._resolved_webdav_bearer_token = token or None
        return self._resolved_webdav_bearer_token

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

        url = self._webdav_file_url(remote_path)
        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--max-time",
            str(DEFAULT_STAGE_FALLBACK_MAX_TIME),
            "--range",
            "0-1",
            "--dump-header",
            "-",
            "--output",
            "/dev/null",
            "--write-out",
            "\n%{http_code}",
            "--header",
            f"Authorization: Bearer {token}",
            url,
        ]
        redacted_cmd = cmd[:-2] + ["--header", "Authorization: Bearer <redacted>", url]
        redacted_cmd_text = shlex.join(redacted_cmd)
        LOG.debug("webdav stage fallback command: %s", redacted_cmd_text)
        attempt = 0
        while True:
            result = subprocess.run(
                cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
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
                time.sleep(wait)
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
                time.sleep(wait)
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
                time.sleep(wait)
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
        with named_tempfile("w", suffix=".txt", delete=False) as fh:
            for p in remote_paths:
                fh.write("/" + p.strip("/") + "\n")
            list_file = fh.name
        try:
            cmd = self._base_cmd() + ["--stage", "--from-file", list_file, "--lifetime", lifetime]
            LOG.debug("stage command: %s", shlex.join(cmd))
            result = run_command(cmd)
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
        with named_tempfile("w", suffix=".txt", delete=False) as fh:
            for p in remote_paths:
                fh.write("/" + p.strip("/") + "\n")
            list_file = fh.name
        try:
            cmd = self._base_cmd() + ["--unstage", "--from-file", list_file]
            LOG.debug("unstage command: %s", shlex.join(cmd))
            run_command(cmd)
        finally:
            os.unlink(list_file)

    def _stat_json(self, remote_path: str) -> tuple[dict | None, str | None]:
        """Return parsed ``ada --stat`` JSON for a path, or an error string."""
        cmd = self._base_cmd() + ["--stat", "/" + remote_path.strip("/")]
        result = run_command(cmd, check=False)
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
        cmd = self._base_cmd() + ["--stat-request", request_id]
        result = run_command(cmd, check=False)
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
            online, _ = self.poll_online_statuses(candidates)
            if online:
                return online[0]
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
        self._bundle_mount_manager: _BundleMountManager | None = None
        self._bundle_mount_lock = threading.Lock()

        if not self.rclone_config.exists():
            raise FileNotFoundError(f"rclone config does not exist: {self.rclone_config}")

    def close(self) -> None:
        with self._bundle_mount_lock:
            manager = self._bundle_mount_manager
            self._bundle_mount_manager = None
        if manager is not None:
            manager.close()

    def _get_bundle_mount_manager(self) -> _BundleMountManager:
        with self._bundle_mount_lock:
            if self._bundle_mount_manager is None:
                self._bundle_mount_manager = _BundleMountManager(self.rclone_config, self.remote)
            return self._bundle_mount_manager

    def _set_progress_status(self, status: str) -> None:
        if self.progress:
            self.progress.status = status

    def _hash_local_adler(self, local_path: Path, *, display_name: str | None = None) -> str:
        if display_name:
            self._set_progress_status(f"hashing {display_name}")
        try:
            return adler32_local(local_path)
        finally:
            if display_name:
                self._set_progress_status("")

    def _fetch_remote_adler_with_status(self, remote_path: str, *, display_name: str | None = None) -> str:
        if display_name:
            self._set_progress_status(f"waiting checksum {display_name}")
        try:
            return self._remote_adler(remote_path)
        finally:
            if display_name:
                self._set_progress_status("")

    @staticmethod
    def _adlers_match(local_adler: str, remote_adler: str) -> bool:
        return normalize_adler(local_adler) == normalize_adler(remote_adler)

    def _read_verified_adlers(
        self,
        local_path: Path,
        remote_path: str,
        *,
        display_name: str,
        local_adler: str | None = None,
        overlap_local_hash: bool = False,
    ) -> tuple[str, str]:
        if overlap_local_hash and local_adler is None:
            self._set_progress_status(f"hashing {display_name}")
            wait_for_local_adler = _run_in_daemon_thread(
                adler32_local,
                local_path,
                name=f"hash-{display_name}",
            )
            self._set_progress_status(f"waiting checksum {display_name}")
            try:
                remote_adler = self._remote_adler(remote_path)
                local_adler = wait_for_local_adler()
            finally:
                self._set_progress_status("")
            return local_adler, remote_adler

        remote_adler = self._fetch_remote_adler_with_status(remote_path, display_name=display_name)
        if local_adler is None:
            local_adler = self._hash_local_adler(local_path, display_name=display_name)
        return local_adler, remote_adler

    @staticmethod
    def _create_download_temp_path(local_path: Path) -> Path:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with named_tempfile(
            prefix=f".{local_path.name}.",
            suffix=".dcache_cp.part",
            dir=local_path.parent,
            delete=False,
        ) as fh:
            return Path(fh.name)

    @staticmethod
    def _create_bundle_temp_path(bundle_name: str) -> Path:
        with named_tempfile(
            prefix=f".{bundle_name}.",
            suffix=".dcache_cp.bundle.part",
            delete=False,
        ) as fh:
            return Path(fh.name)

    @staticmethod
    def _install_verified_download(temp_path: Path, local_path: Path) -> None:
        os.replace(temp_path, local_path)

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
                local_adler, remote_adler = self._read_verified_adlers(
                    local_path,
                    remote_path,
                    display_name=local_path.name,
                )
                if self._adlers_match(local_adler, remote_adler):
                    self._delete_uploaded_source(entry)
                    LOG.debug("skip %s (verified)", rel)
                    return self._result(rel, remote_path, entry, local_adler, remote_adler, 0, True)
            except FileNotFoundError:
                LOG.debug("remote file missing for %s; uploading", rel)
            except Exception:
                LOG.debug("remote checksum unavailable for %s; uploading", rel)

        for attempt in range(self.max_retries + 1):
            self._rclone_mkdir(remote_dir)
            self._rclone_copyto(str(local_path), f"{self.remote}:{remote_path}")
            # Fetch remote checksum (may wait for dCache to compute it).
            # Compute local hash concurrently in a thread so we don't add
            # extra wall-clock time on top of the checksum wait.
            try:
                local_adler, remote_adler = self._read_verified_adlers(
                    local_path,
                    remote_path,
                    display_name=local_path.name,
                    local_adler=local_adler,
                    overlap_local_hash=(local_adler is None),
                )
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

            if self._adlers_match(local_adler, remote_adler):
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
                local_adler, remote_adler = self._read_verified_adlers(
                    local_path,
                    remote_path,
                    display_name=local_path.name,
                )
                if self._adlers_match(local_adler, remote_adler):
                    self._delete_downloaded_source(remote_path)
                    LOG.debug("skip %s (verified)", rel)
                    return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, 0, True)
            except Exception:
                LOG.debug("checksum comparison failed for %s; downloading", rel)

        for attempt in range(self.max_retries + 1):
            temp_local_path = self._create_download_temp_path(local_path)
            try:
                self._rclone_copyto(f"{self.remote}:{remote_path}", str(temp_local_path))
                local_adler, remote_adler = self._read_verified_adlers(
                    temp_local_path,
                    remote_path,
                    display_name=local_path.name,
                )
            except Exception as exc:
                LOG.warning("verification failed %s: %s (attempt %d/%d)",
                            rel, exc, attempt + 1, self.max_retries + 1)
                temp_local_path.unlink(missing_ok=True)
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                    continue
                raise RuntimeError(f"verification failed for {rel}: {exc}") from exc

            if self._adlers_match(local_adler, remote_adler):
                try:
                    self._install_verified_download(temp_local_path, local_path)
                except Exception as exc:
                    LOG.warning("download finalization failed %s: %s (attempt %d/%d)",
                                rel, exc, attempt + 1, self.max_retries + 1)
                    temp_local_path.unlink(missing_ok=True)
                    if attempt < self.max_retries:
                        time.sleep(self.retry_wait)
                        continue
                    raise RuntimeError(f"download finalization failed for {rel}: {exc}") from exc
                self._delete_downloaded_source(remote_path)
                return self._dl_result(rel, remote_path, local_path, size, local_adler, remote_adler, attempt + 1, False)
            LOG.warning("checksum mismatch %s: local=%s remote=%s (attempt %d/%d)",
                        rel, local_adler, remote_adler, attempt + 1, self.max_retries + 1)
            temp_local_path.unlink(missing_ok=True)
            if attempt < self.max_retries:
                time.sleep(self.retry_wait)

        raise RuntimeError(f"checksum mismatch for {rel}: local={local_adler} remote={remote_adler}")

    def _extract_bundle_members(
        self,
        bundle_path: Path,
        bundle_members: list[dict],
        *,
        bundle_format: str,
    ) -> list[tuple[Path, Path, dict]]:
        if bundle_format != DEFAULT_BUNDLE_FORMAT:
            raise RuntimeError(f"unsupported bundle format for extraction: {bundle_format}")
        return self._extract_squashfs_bundle_members(bundle_path, bundle_members)

    @staticmethod
    def _should_try_sparse_bundle_read(entry: dict, bundle_members: list[dict], bundle_format: str) -> bool:
        if bundle_format != DEFAULT_BUNDLE_FORMAT:
            return False
        if not _BundleMountManager.available():
            return False
        bundle_member_total = int(entry.get("bundle_member_total", len(bundle_members)))
        if bundle_member_total <= len(bundle_members):
            return False
        requested_bytes = sum(int(member.get("size", 0)) for member in bundle_members)
        bundle_size = int(entry.get("stage_size", entry.get("size", 0)))
        if bundle_size > 0 and requested_bytes * 2 >= bundle_size and len(bundle_members) * 2 >= bundle_member_total:
            return False
        return True

    def _extract_squashfs_bundle_members_via_mount(
        self,
        bundle_remote_path: str,
        bundle_members: list[dict],
    ) -> list[tuple[Path, Path, dict]]:
        sqfscat = shutil.which("sqfscat")
        bundle_path = self._get_bundle_mount_manager().bundle_local_path(bundle_remote_path)
        if not sqfscat:
            return self._extract_squashfs_bundle_members(bundle_path, bundle_members)

        staged_members: list[tuple[Path, Path, dict]] = []
        try:
            for member in bundle_members:
                anchor_rel = str(member["anchor_rel"])
                local_path = Path(member["local_path"])
                temp_local_path = self._create_download_temp_path(local_path)
                with temp_local_path.open("wb") as handle:
                    result = subprocess.run(
                        [sqfscat, str(bundle_path), anchor_rel],
                        check=False,
                        stdout=handle,
                        stderr=subprocess.PIPE,
                    )
                if result.returncode != 0:
                    temp_local_path.unlink(missing_ok=True)
                    detail = result.stderr.decode("utf-8", errors="replace").strip() or "sqfscat failed"
                    raise RuntimeError(f"sqfscat failed for {bundle_remote_path}:{anchor_rel}: {detail}")

                local_adler = adler32_local(temp_local_path)
                expected_adler = str(member["adler32"])
                if not self._adlers_match(local_adler, expected_adler):
                    temp_local_path.unlink(missing_ok=True)
                    raise RuntimeError(
                        f"bundle member checksum mismatch for {member['rel']}: "
                        f"local={local_adler} expected={expected_adler}"
                    )

                staged_members.append((temp_local_path, local_path, member))
        except Exception:
            for temp_local_path, _local_path, _member in staged_members:
                temp_local_path.unlink(missing_ok=True)
            raise
        return staged_members

    def _extract_squashfs_bundle_members(self, bundle_path: Path, bundle_members: list[dict]) -> list[tuple[Path, Path, dict]]:
        unsquashfs = shutil.which("unsquashfs")
        if not unsquashfs:
            raise RuntimeError("bundle format squashfs requires unsquashfs to be installed")

        workspace_root = Path(dcache_mkdtemp(prefix="dcache-unsquashfs-"))
        extraction_root = workspace_root / "extract"
        staged_members: list[tuple[Path, Path, dict]] = []
        requested_paths = [str(member["anchor_rel"]) for member in bundle_members]
        try:
            result = subprocess.run(
                [
                    unsquashfs,
                    "-no-progress",
                    "-dest",
                    str(extraction_root),
                    str(bundle_path),
                    *requested_paths,
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                detail = (result.stderr.strip() or result.stdout.strip() or "unsquashfs failed")
                raise RuntimeError(f"unsquashfs failed for {bundle_path.name}: {detail}")

            for member in bundle_members:
                anchor_rel = str(member["anchor_rel"])
                extracted_path = extraction_root / anchor_rel
                if not extracted_path.is_file():
                    raise RuntimeError(f"bundle member missing from squashfs archive: {anchor_rel}")

                local_path = Path(member["local_path"])
                temp_local_path = self._create_download_temp_path(local_path)
                shutil.copyfile(extracted_path, temp_local_path)

                local_adler = adler32_local(temp_local_path)
                expected_adler = str(member["adler32"])
                if not self._adlers_match(local_adler, expected_adler):
                    raise RuntimeError(
                        f"bundle member checksum mismatch for {member['rel']}: "
                        f"local={local_adler} expected={expected_adler}"
                    )

                staged_members.append((temp_local_path, local_path, member))
        except Exception:
            for temp_local_path, _local_path, _member in staged_members:
                temp_local_path.unlink(missing_ok=True)
            raise
        finally:
            shutil.rmtree(workspace_root, ignore_errors=True)
        return staged_members

    def download_bundle(self, entry: dict) -> dict:
        bundle_remote_path = str(entry["remote_path"]).strip("/")
        bundle_members = _bundle_members_for_entry(entry)
        if not bundle_members:
            raise ValueError(f"bundle download entry has no members: {bundle_remote_path}")
        bundle_format = str(entry.get("bundle_format", DEFAULT_BUNDLE_FORMAT))

        if self.skip_verified and _bundle_entry_fully_verified(entry):
            LOG.debug("skip bundle %s (all requested members already verified)", bundle_remote_path)
            return {
                "rel": entry["rel"],
                "remote_path": bundle_remote_path,
                "attempt": 0,
                "skipped": True,
                "bundle_members": bundle_members,
            }

        if self._should_try_sparse_bundle_read(entry, bundle_members, bundle_format):
            try:
                staged_members = self._extract_squashfs_bundle_members_via_mount(bundle_remote_path, bundle_members)
                for temp_local_path, local_path, member in staged_members:
                    self._install_verified_download(temp_local_path, local_path)
                    try:
                        os.chmod(local_path, int(member["mode"]))
                    except OSError:
                        LOG.debug("could not restore mode for %s", local_path)
                return {
                    "rel": entry["rel"],
                    "remote_path": bundle_remote_path,
                    "attempt": 1,
                    "skipped": False,
                    "bundle_members": bundle_members,
                    "sparse": True,
                }
            except Exception as exc:
                LOG.info("bundle sparse read fallback for %s: %s", bundle_remote_path, exc)

        bundle_name = posixpath.basename(bundle_remote_path) or "bundle"
        last_local_adler = ""
        last_remote_adler = ""

        for attempt in range(self.max_retries + 1):
            temp_bundle_path = self._create_bundle_temp_path(bundle_name)
            staged_members: list[tuple[Path, Path, dict]] = []
            try:
                self._rclone_copyto(f"{self.remote}:{bundle_remote_path}", str(temp_bundle_path))
                last_local_adler, last_remote_adler = self._read_verified_adlers(
                    temp_bundle_path,
                    bundle_remote_path,
                    display_name=bundle_name,
                )
                if not self._adlers_match(last_local_adler, last_remote_adler):
                    raise RuntimeError(
                        f"bundle checksum mismatch: local={last_local_adler} remote={last_remote_adler}"
                    )

                staged_members = self._extract_bundle_members(
                    temp_bundle_path,
                    bundle_members,
                    bundle_format=bundle_format,
                )
                for temp_local_path, local_path, member in staged_members:
                    self._install_verified_download(temp_local_path, local_path)
                    try:
                        os.chmod(local_path, int(member["mode"]))
                    except OSError:
                        LOG.debug("could not restore mode for %s", local_path)
            except Exception as exc:
                LOG.warning(
                    "bundle download failed %s: %s (attempt %d/%d)",
                    bundle_remote_path,
                    exc,
                    attempt + 1,
                    self.max_retries + 1,
                )
                temp_bundle_path.unlink(missing_ok=True)
                for temp_local_path, _local_path, _member in staged_members:
                    temp_local_path.unlink(missing_ok=True)
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                    continue
                raise RuntimeError(f"bundle download failed for {bundle_remote_path}: {exc}") from exc

            temp_bundle_path.unlink(missing_ok=True)
            return {
                "rel": entry["rel"],
                "remote_path": bundle_remote_path,
                "attempt": attempt + 1,
                "skipped": False,
                "bundle_members": bundle_members,
                "local_adler": last_local_adler,
                "remote_adler": last_remote_adler,
            }

        raise RuntimeError(f"bundle download failed for {bundle_remote_path}")

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
        cmd = _ada_tokenfile_cmd(self.ada_cmd, self.rclone_config, self.api) + ["--checksum", "/" + remote_path.strip("/")]

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
                if _ada_reports_transient_transport_error(output):
                    wait = min(2 ** attempt * 2, 30)  # 2 s … 30 s
                    if time.monotonic() + wait < deadline:
                        LOG.warning(
                            "ada transport failure for %s, retrying in %ds",
                            remote_path,
                            wait,
                        )
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
    *,
    result_handler=None,
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
            _handle_worker_result(entry, exc, result, progress, bar, result_handler=result_handler)
    except KeyboardInterrupt:
        pool.stop()
        raise
    finally:
        pool.join(timeout=1)


def _expected_bundle_member_map(job) -> dict[str, BundleMemberMetadata]:
    return {
        member.anchor_rel: BundleMemberMetadata(
            anchor_rel=member.anchor_rel,
            adler32=member.adler32,
            size=member.size,
            mtime_ns=member.mtime_ns,
            mode=member.mode,
        )
        for member in job.members
    }


def _remote_bundle_xattrs(xattr_client: NamespaceXattrClient, remote_path: str) -> dict[str, str] | None:
    try:
        return xattr_client.list_xattrs(remote_path)
    except NamespaceXattrError as exc:
        if exc.status == 404:
            return None
        raise


def _bundle_job_can_reuse_remote(job, xattr_client: NamespaceXattrClient) -> bool:
    xattrs = _remote_bundle_xattrs(xattr_client, job.remote_path)
    if xattrs is None:
        return False

    remote_bundle_id = xattrs.get("dcache_cp.bundle.id")
    if remote_bundle_id and remote_bundle_id != job.bundle_id:
        raise RuntimeError(
            f"existing bundle object {job.remote_path} has bundle id {remote_bundle_id}, expected {job.bundle_id}; refusing to overwrite"
        )
    remote_anchor_dir = xattrs.get("dcache_cp.bundle.anchor_dir")
    if remote_anchor_dir and remote_anchor_dir != job.anchor_dir:
        raise RuntimeError(
            f"existing bundle object {job.remote_path} belongs to anchor {remote_anchor_dir}, expected {job.anchor_dir}; refusing to overwrite"
        )
    remote_format = xattrs.get("dcache_cp.bundle.format")
    if remote_format and remote_format != job.bundle_object_xattrs["dcache_cp.bundle.format"]:
        raise RuntimeError(
            f"existing bundle object {job.remote_path} has format {remote_format}, expected {job.bundle_object_xattrs['dcache_cp.bundle.format']}; refusing to overwrite"
        )

    remote_members = decode_bundle_members(xattrs)
    expected_members = _expected_bundle_member_map(job)
    if set(remote_members) != set(expected_members):
        raise RuntimeError(
            f"existing bundle object {job.remote_path} contains different members than expected; refusing to overwrite"
        )
    for anchor_rel, expected_member in expected_members.items():
        remote_member = remote_members.get(anchor_rel)
        if remote_member is None or not _bundle_member_matches_remote(
            BundleMember(
                rel=anchor_rel,
                anchor_rel=expected_member.anchor_rel,
                source=Path(anchor_rel),
                resolved_source=Path(anchor_rel),
                remote_path=job.remote_path,
                size=expected_member.size,
                mtime_ns=expected_member.mtime_ns,
                mode=expected_member.mode,
                adler32=expected_member.adler32,
            ),
            remote_member,
        ):
            raise RuntimeError(
                f"existing bundle object {job.remote_path} has different metadata for member {anchor_rel}; refusing to overwrite"
            )
    return True


def _publish_anchor_route_generation(xattr_client: NamespaceXattrClient, anchor_plan: AnchorBundlePlan) -> None:
    anchor_xattrs = build_anchor_xattrs(anchor_plan)
    active_generation = anchor_xattrs.pop("dcache_cp.bundle_anchor.active_generation")
    if anchor_xattrs:
        xattr_client.set_xattrs(anchor_plan.anchor_dir, anchor_xattrs)
    xattr_client.set_xattrs(
        anchor_plan.anchor_dir,
        {"dcache_cp.bundle_anchor.active_generation": active_generation},
    )


def _publish_anchor_route_map(
    xattr_client: NamespaceXattrClient,
    anchor_dir: str,
    route_map: dict[str, str],
    generation: str,
) -> None:
    anchor_xattrs = build_anchor_xattrs_for_routes(route_map, generation)
    active_generation = anchor_xattrs.pop("dcache_cp.bundle_anchor.active_generation")
    if anchor_xattrs:
        xattr_client.set_xattrs(anchor_dir, anchor_xattrs)
    xattr_client.set_xattrs(
        anchor_dir,
        {"dcache_cp.bundle_anchor.active_generation": active_generation},
    )


def _apply_bundle_deprecations(xattr_client: NamespaceXattrClient, anchor_plan: AnchorBundlePlan) -> None:
    for deprecation_plan in anchor_plan.deprecations:
        existing_xattrs = xattr_client.list_xattrs(deprecation_plan.remote_path)
        merged = decode_bundle_deprecated(existing_xattrs)
        for record in deprecation_plan.members:
            merged[record.anchor_rel] = record
        xattr_client.set_xattrs(
            deprecation_plan.remote_path,
            build_bundle_deprecated_xattrs(merged),
        )


def _delete_bundle_sources(members: tuple[BundleMember, ...] | list[BundleMember]) -> None:
    for member in members:
        Path(member.source).unlink(missing_ok=True)


def _bundle_is_fully_retired(
    bundle_members: dict[str, BundleMemberMetadata],
    route_map: dict[str, str],
    bundle_id: str,
    deleted: dict[str, BundleDeletedRecord],
    deprecated: dict[str, BundleDeprecationRecord],
) -> bool:
    if any(current_bundle_id == bundle_id for current_bundle_id in route_map.values()):
        return False
    retired_members = set(deleted) | set(deprecated)
    return set(bundle_members).issubset(retired_members)


def _bundle_route_cleanup_targets(
    bundle_id: str,
    route_map: dict[str, str],
    deleted: dict[str, BundleDeletedRecord],
    deprecated: dict[str, BundleDeprecationRecord],
) -> set[str]:
    retired_members = set(deleted) | set(deprecated)
    return {
        anchor_rel
        for anchor_rel in retired_members
        if route_map.get(anchor_rel) == bundle_id
    }


def _commit_bundle_download_move(
    entry: dict,
    result: dict,
    xattr_client: NamespaceXattrClient,
    transferer: Transferer,
) -> None:
    bundle_members = result.get("bundle_members")
    if not isinstance(bundle_members, list) or not bundle_members:
        raise RuntimeError("bundle move commit requires resolved bundle members")

    bundle_remote_path = str(result.get("remote_path", entry["remote_path"])).strip("/")
    anchor_dir = str(entry.get("bundle_anchor_dir", "")).strip("/")
    bundle_id = str(entry.get("bundle_id", "")).strip()
    if not anchor_dir or not bundle_id:
        raise RuntimeError(f"bundle move commit missing anchor metadata for {bundle_remote_path}")

    deleted_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    requested_anchor_rels = [str(member["anchor_rel"]) for member in bundle_members]
    latest_bundle_member_map: dict[str, BundleMemberMetadata] = {}
    latest_deleted: dict[str, BundleDeletedRecord] = {}
    latest_deprecated: dict[str, BundleDeprecationRecord] = {}
    latest_route_map: dict[str, str] = {}

    for attempt in range(8):
        commit_generation = make_bundle_generation()

        bundle_xattrs = xattr_client.list_xattrs(bundle_remote_path)
        remote_bundle_id = str(bundle_xattrs.get("dcache_cp.bundle.id") or "").strip()
        if remote_bundle_id and remote_bundle_id != bundle_id:
            raise RuntimeError(
                f"bundle move commit refused: bundle object {bundle_remote_path} advertises bundle id {remote_bundle_id}, expected {bundle_id}"
            )
        remote_anchor_dir = str(bundle_xattrs.get("dcache_cp.bundle.anchor_dir") or "").strip("/")
        if remote_anchor_dir and remote_anchor_dir != anchor_dir:
            raise RuntimeError(
                f"bundle move commit refused: bundle object {bundle_remote_path} belongs to anchor {remote_anchor_dir or '/'}; expected {anchor_dir or '/'}"
            )

        latest_bundle_member_map = decode_bundle_members(bundle_xattrs)
        latest_deleted = decode_bundle_deleted(bundle_xattrs)
        latest_deprecated = decode_bundle_deprecated(bundle_xattrs)

        anchor_xattrs = xattr_client.list_xattrs(anchor_dir)
        route_map = decode_anchor_routes(anchor_xattrs)

        for anchor_rel in requested_anchor_rels:
            if anchor_rel not in latest_bundle_member_map:
                raise RuntimeError(
                    f"bundle move commit refused: {bundle_remote_path} has no member {anchor_rel!r}"
                )
            current_bundle_id = route_map.get(anchor_rel)
            if current_bundle_id != bundle_id:
                raise RuntimeError(
                    f"bundle move commit refused: active route for {anchor_dir or '/'}:{anchor_rel} points to {current_bundle_id or '<missing>'}, expected {bundle_id}"
                )
            latest_deleted[anchor_rel] = BundleDeletedRecord(
                anchor_rel=anchor_rel,
                deleted_generation=commit_generation,
                deleted_at=deleted_at,
            )

        xattr_client.set_xattrs(bundle_remote_path, build_bundle_deleted_xattrs(latest_deleted))

        current_bundle_xattrs = xattr_client.list_xattrs(bundle_remote_path)
        latest_bundle_member_map = decode_bundle_members(current_bundle_xattrs)
        latest_deleted = decode_bundle_deleted(current_bundle_xattrs)
        latest_deprecated = decode_bundle_deprecated(current_bundle_xattrs)

        if any(anchor_rel not in latest_deleted for anchor_rel in requested_anchor_rels):
            continue

        current_anchor_xattrs = xattr_client.list_xattrs(anchor_dir)
        route_map = decode_anchor_routes(current_anchor_xattrs)
        cleanup_targets = _bundle_route_cleanup_targets(bundle_id, route_map, latest_deleted, latest_deprecated)
        if cleanup_targets:
            for anchor_rel in cleanup_targets:
                route_map.pop(anchor_rel, None)
            _publish_anchor_route_map(xattr_client, anchor_dir, route_map, commit_generation)

        current_anchor_xattrs = xattr_client.list_xattrs(anchor_dir)
        latest_route_map = decode_anchor_routes(current_anchor_xattrs)
        stale_targets = _bundle_route_cleanup_targets(bundle_id, latest_route_map, latest_deleted, latest_deprecated)
        if not stale_targets:
            break
    else:
        raise RuntimeError(
            f"bundle move commit refused: could not publish a stable route update for {bundle_remote_path} after repeated retries"
        )

    if _bundle_is_fully_retired(
        latest_bundle_member_map,
        latest_route_map,
        bundle_id,
        latest_deleted,
        latest_deprecated,
    ):
        try:
            transferer._rclone_deletefile(f"{transferer.remote}:{bundle_remote_path}")
        except Exception as exc:
            LOG.warning("bundle cleanup after move failed for %s: %s", bundle_remote_path, exc)
        else:
            LOG.info("removed fully deleted bundle object %s", bundle_remote_path)


def _execute_bundle_uploads(
    bundle_plan: BundleUploadPlan,
    transferer: Transferer,
    xattr_client: NamespaceXattrClient,
    progress: Progress,
    bar: ProgressBar,
    *,
    delete_source: bool,
    keep_temp: bool,
):
    """Upload prepared bundle objects and commit their anchor xattrs per directory."""
    for anchor_plan in bundle_plan.anchors:
        materialized_entries: list[dict] = []
        uploaded_jobs: list[dict[str, object]] = []
        try:
            for job in anchor_plan.bundles:
                if _bundle_job_can_reuse_remote(job, xattr_client):
                    uploaded_jobs.append({
                        "job": job,
                        "result": {"attempt": 0, "remote_path": job.remote_path, "skipped": True},
                        "reused": True,
                        "uploaded": False,
                        "xattrs_published": True,
                    })
                    continue

                entry = materialize_bundle_job(job, keep_temp=keep_temp)
                materialized_entries.append(entry)
                result = transferer.upload(entry)
                uploaded_jobs.append({
                    "job": job,
                    "result": result,
                    "reused": False,
                    "uploaded": True,
                    "xattrs_published": False,
                })
                xattr_client.set_xattrs(job.remote_path, job.bundle_object_xattrs)
                uploaded_jobs[-1]["xattrs_published"] = True

            if anchor_plan.commit_required:
                _publish_anchor_route_generation(xattr_client, anchor_plan)
            if anchor_plan.deprecations:
                _apply_bundle_deprecations(xattr_client, anchor_plan)
            if delete_source:
                _delete_bundle_sources(anchor_plan.reused_members)
                for state in uploaded_jobs:
                    _delete_bundle_sources(state["job"].members)

        except Exception as exc:
            for state in uploaded_jobs:
                if not state.get("uploaded") or state.get("xattrs_published"):
                    continue
                job = state["job"]
                try:
                    transferer._rclone_deletefile(f"{transferer.remote}:{job.remote_path}")
                except Exception as cleanup_exc:
                    LOG.warning("bundle cleanup failed for %s: %s", job.remote_path, cleanup_exc)
                else:
                    LOG.warning("removed incomplete bundle object %s after upload/xattr failure", job.remote_path)
            for member in anchor_plan.reused_members:
                progress.failure(member.rel, member.size, exc)
            for job in anchor_plan.bundles:
                for member in job.members:
                    progress.failure(member.rel, member.size, exc)
            bar.finish()
            LOG.error("%s\u2718%s bundle anchor %s: %s", _C.RED, _C.RESET, anchor_plan.anchor_dir or "/", exc)
            bar.update(anchor_plan.anchor_dir or "/")
        else:
            if anchor_plan.reused_members:
                for member in anchor_plan.reused_members:
                    progress.success(member.rel, member.size, attempts=0, skipped=True)
                bar.finish()
                LOG.info(
                    "%s\u2714%s reused %s%d%s bundle-backed file(s) for %s",
                    _C.CYAN,
                    _C.RESET,
                    _C.CYAN,
                    len(anchor_plan.reused_members),
                    _C.RESET,
                    anchor_plan.anchor_dir or "/",
                )
                bar.update(anchor_plan.anchor_dir or "/")

            for state in uploaded_jobs:
                job = state["job"]
                result = state["result"]
                attempts = int(result.get("attempt", 1))
                skipped = bool(result.get("skipped", False) or state.get("reused"))
                for index, member in enumerate(job.members):
                    progress.success(member.rel, member.size, attempts=attempts if index == 0 else 1, skipped=skipped)
                bar.finish()
                if skipped:
                    LOG.info(
                        "%s\u2714%s reused bundle %s %s(%d files, %s)%s",
                        _C.CYAN,
                        _C.RESET,
                        job.remote_path,
                        _C.DIM,
                        len(job.members),
                        format_bytes(job.logical_total_bytes),
                        _C.RESET,
                    )
                else:
                    LOG.info(
                        "%s\u2714%s bundle %s %s(%d files, %s)%s",
                        _C.GREEN,
                        _C.RESET,
                        job.remote_path,
                        _C.DIM,
                        len(job.members),
                        format_bytes(job.logical_total_bytes),
                        _C.RESET,
                    )
                bar.update(job.remote_path)
        finally:
            if keep_temp:
                continue
            for entry in materialized_entries:
                Path(entry["resolved_source"]).unlink(missing_ok=True)


def _handle_failed_result(entry: dict, exc: BaseException, progress: Progress, bar: ProgressBar):
    stage_key = entry.get("remote_path")
    if isinstance(stage_key, str):
        stage_key = "/" + stage_key.strip("/")
    bundle_members = _bundle_members_for_entry(entry)
    if bundle_members:
        for index, member in enumerate(bundle_members):
            progress.failure(
                member["rel"],
                int(member.get("size", 0)),
                exc,
                stage_key=stage_key if index == 0 and isinstance(stage_key, str) else None,
            )
        bar.finish()
        LOG.error("%s\u2718%s bundle %s: %s", _C.RED, _C.RESET, entry["remote_path"], exc)
        bar.update(str(entry["remote_path"]))
        return None
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
    bundle_members = result.get("bundle_members")
    if isinstance(bundle_members, list) and bundle_members:
        attempts = int(result.get("attempt", 1))
        logical_total = sum(int(member.get("size", 0)) for member in bundle_members)
        for index, member in enumerate(bundle_members):
            progress.success(
                member["rel"],
                int(member.get("size", 0)),
                attempts=attempts if index == 0 else 1,
                skipped=skipped,
                stage_key=stage_key if index == 0 and isinstance(stage_key, str) else None,
            )
        if skipped:
            LOG.debug("%s\u2714%s bundle %s %s(verified)%s", _C.CYAN, _C.RESET, result["remote_path"], _C.DIM, _C.RESET)
        else:
            bar.finish()
            LOG.info(
                "%s\u2714%s bundle %s %s(%d files, %s)%s",
                _C.GREEN,
                _C.RESET,
                result["remote_path"],
                _C.DIM,
                len(bundle_members),
                format_bytes(logical_total),
                _C.RESET,
            )
        bar.update(str(result.get("remote_path", entry["rel"])))
        return result
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
    *,
    result_handler=None,
):
    if exc is not None:
        return _handle_failed_result(entry, exc, progress, bar)
    if result is None:
        return _handle_failed_result(entry, RuntimeError("worker finished without a result"), progress, bar)
    if result_handler is not None:
        try:
            result_handler(entry, result)
        except BaseException as handler_exc:
            return _handle_failed_result(entry, handler_exc, progress, bar)
    return _handle_completed_result(result, entry, progress, bar)


def _filter_verified_download_entries(
    files: list[dict],
    transferer: Transferer,
) -> tuple[list[dict], list[dict]]:
    """Remove download entries whose local target already matches the remote checksum."""
    remaining: list[dict] = []
    skipped: list[dict] = []

    for entry in files:
        bundle_members = _bundle_members_for_entry(entry)
        if bundle_members:
            if transferer.skip_verified and _bundle_entry_fully_verified(entry):
                LOG.debug("skip bundle %s (verified before staging)", entry["remote_path"])
                skipped.append(entry)
                continue
            remaining.append(entry)
            continue

        local_path = Path(entry["local_path"])
        if not transferer.skip_verified or not local_path.exists():
            remaining.append(entry)
            continue

        remote_path = str(entry["remote_path"]).strip("/")
        try:
            remote_adler = transferer._remote_adler(remote_path)
            local_adler = adler32_local(local_path)
            if normalize_adler(local_adler) == normalize_adler(remote_adler):
                transferer._delete_downloaded_source(remote_path)
                LOG.debug("skip %s (verified before staging)", entry["rel"])
                skipped.append(entry)
                continue
        except Exception:
            LOG.debug("pre-stage checksum comparison failed for %s; downloading", entry["rel"])

        remaining.append(entry)

    return remaining, skipped


def _apply_pre_skipped_entries(progress: Progress, entries: list[dict]) -> None:
    for entry in entries:
        bundle_members = _bundle_members_for_entry(entry)
        if bundle_members:
            for member in bundle_members:
                progress.success(member["rel"], int(member.get("size", 0)), attempts=0, skipped=True)
            continue
        progress.success(entry["rel"], int(entry.get("size", 0)), attempts=0, skipped=True)


def _build_stage_batches(files: list[dict], max_files: int, max_bytes: int) -> list[list[dict]]:
    """Split download entries into batches bounded by file count and staged bytes."""
    batches: list[list[dict]] = []
    current: list[dict] = []
    current_bytes = 0

    for entry in files:
        entry_size = _entry_stage_bytes(entry)
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
    for remote_path in remote_paths:
        try:
            stage_mgr.unstage([remote_path])
            LOG.debug("destaged %s from batch %d", remote_path, batch_num)
        except Exception as exc:
            LOG.warning("destage failed for %s (batch %d): %s", remote_path, batch_num, exc)
            failures.append(remote_path)
    if bar is not None:
        bar.update()
    return failures


class _BundleMountManager:
    def __init__(self, rclone_config: Path, remote: str):
        self.rclone_config = Path(rclone_config).expanduser()
        self.remote = remote
        self._mountpoint: Path | None = None
        self._lock = threading.Lock()
        self._cleanup_registered = False

    @staticmethod
    def _find_unmount_cmd() -> list[str] | None:
        for command in ("fusermount3", "fusermount", "umount"):
            executable = shutil.which(command)
            if not executable:
                continue
            if command.startswith("fuser"):
                return [executable, "-u"]
            return [executable]
        return None

    @classmethod
    def available(cls) -> bool:
        return bool(shutil.which("rclone") and shutil.which("unsquashfs") and cls._find_unmount_cmd())

    def ensure_mounted(self) -> Path:
        with self._lock:
            if self._mountpoint is not None:
                return self._mountpoint
            if not self.available():
                raise RuntimeError("sparse bundle reads require rclone mount, unsquashfs, and a FUSE unmount helper")

            mountpoint = Path(dcache_mkdtemp(prefix=f"dcache-rclone-mount-{self.remote}-"))
            result = run_command(
                [
                    "rclone",
                    "--config",
                    str(self.rclone_config),
                    "mount",
                    "--daemon",
                    "--read-only",
                    "--dir-cache-time",
                    "1m",
                    "--poll-interval",
                    "0",
                    "--vfs-cache-mode",
                    "off",
                    "--attr-timeout",
                    "1s",
                    f"{self.remote}:",
                    str(mountpoint),
                ],
                check=False,
            )
            if result.returncode != 0:
                shutil.rmtree(mountpoint, ignore_errors=True)
                detail = (result.stderr.strip() or result.stdout.strip() or "rclone mount failed")
                raise RuntimeError(f"sparse bundle mount failed for {self.remote}: {detail}")

            self._mountpoint = mountpoint
            if not self._cleanup_registered:
                atexit.register(self.close)
                self._cleanup_registered = True
            return mountpoint

    def bundle_local_path(self, remote_path: str) -> Path:
        mountpoint = self.ensure_mounted()
        clean_remote_path = str(remote_path).strip("/")
        return mountpoint / clean_remote_path

    def close(self) -> None:
        with self._lock:
            mountpoint = self._mountpoint
            self._mountpoint = None
        if mountpoint is None:
            return

        unmount_cmd = self._find_unmount_cmd()
        try:
            if unmount_cmd is not None:
                result = run_command([*unmount_cmd, str(mountpoint)], check=False)
                if result.returncode != 0:
                    detail = result.stderr.strip() or result.stdout.strip() or "unmount failed"
                    LOG.warning("bundle mount cleanup failed for %s: %s", mountpoint, detail)
            shutil.rmtree(mountpoint, ignore_errors=True)
        except Exception as exc:
            LOG.warning("bundle mount cleanup failed for %s: %s", mountpoint, exc)


def _execute_pipeline_download(
    files: list[dict],
    transferer: Transferer,
    stage_mgr: StageManager,
    workers: int,
    progress: Progress,
    bar: ProgressBar,
    stage_batch: int,
    stage_batch_bytes: int,
    stage_lifetime: str,
    stage_poll: int,
    stage_timeout: int,
    destage: bool,
    worker_fn=None,
    result_handler=None,
):
    """Pipeline: stage a batch → download as files come online → destage completed files.

    This avoids filling the staging area with more data than can be held at once.
    Files are processed in batches of ``stage_batch``.
    """
    batches = _build_stage_batches(files, stage_batch, stage_batch_bytes)
    stage_started_at: dict[int, float] = {}
    prestaged_batches: set[int] = set()
    stage_request_ids: dict[int, list[str]] = {}

    # Process in batches
    for batch_index, batch_entries in enumerate(batches):
        batch_paths = ["/" + e["remote_path"].strip("/") for e in batch_entries]
        batch_path_to_entry = dict(zip(batch_paths, batch_entries))
        batch_stage_entries = [(path, int(entry.get("size", 0))) for path, entry in batch_path_to_entry.items()]
        batch_num = batch_index + 1
        total_batches = len(batches)

        if total_batches > 1:
            LOG.info("batch %d/%d: staging %d file(s)", batch_num, total_batches, len(batch_paths))

        fallback_enabled = stage_mgr.can_prime_via_webdav_range()

        # Stage this batch
        if batch_index not in prestaged_batches:
            stage_request_ids[batch_index] = stage_mgr.stage(batch_paths, lifetime=stage_lifetime)
            stage_started_at[batch_index] = time.monotonic()
            if not fallback_enabled:
                for path, size in batch_stage_entries:
                    progress.mark_stage_requested(path, size)
            bar.update()

        # Wait for files in this batch to come online, then download them in daemon workers.
        pending_stage = set(batch_paths)
        pending_downloads = 0
        stage_error_messages: dict[str, str] = {}
        deferred_destage_paths: list[str] = []
        pool = _DaemonWorkerPool(worker_fn or transferer.download, workers, name_prefix="stage-download-worker")
        fallback_pool: _DaemonWorkerPool | None = None
        if fallback_enabled:
            fallback_pool = _DaemonWorkerPool(
                stage_mgr.prime_via_webdav_range,
                min(DEFAULT_STAGE_FALLBACK_WORKERS, max(len(batch_entries), 1)),
                name_prefix="stage-fallback-worker",
            )
        fallback_requested_paths: set[str] = set()
        fallback_inflight_paths: set[str] = set()
        fallback_success_paths: set[str] = set()
        fallback_failure_paths: set[str] = set()
        fallback_success_log_count = 0
        batch_total_bytes = sum(int(entry.get("size", 0)) for entry in batch_entries)
        batch_completed_bytes = 0
        batch_completed_files = 0
        next_batch_index = batch_index + 1
        next_batch_paths = ["/" + e["remote_path"].strip("/") for e in batches[next_batch_index]] if next_batch_index < total_batches else []
        next_batch_stage_entries = [
            ("/" + entry["remote_path"].strip("/"), int(entry.get("size", 0)))
            for entry in batches[next_batch_index]
        ] if next_batch_index < total_batches else []
        batch_request_ids = stage_request_ids.get(batch_index, [])
        prefetched_next_batch = False

        try:
            is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
            stage_start = stage_started_at.get(batch_index, time.monotonic())

            while pending_stage or pending_downloads:
                if fallback_pool is not None:
                    while True:
                        try:
                            fallback_entry, fallback_exc, _ = fallback_pool.get_result_nowait()
                        except queue.Empty:
                            break
                        fallback_path = "/" + str(fallback_entry["remote_path"]).strip("/")
                        fallback_inflight_paths.discard(fallback_path)
                        if fallback_exc is None:
                            if fallback_path in batch_path_to_entry:
                                fallback_success_paths.add(fallback_path)
                                timed_out = bool(fallback_entry.get("timed_out"))
                                progress.mark_stage_requested(
                                    fallback_path,
                                    int(batch_path_to_entry[fallback_path].get("size", 0)),
                                )
                                previous_stage_error = (
                                    stage_error_messages[fallback_path]
                                    if fallback_path in stage_error_messages
                                    else "ada stage failed"
                                )
                                if timed_out:
                                    stage_error_messages[fallback_path] = previous_stage_error
                                else:
                                    stage_error_messages[fallback_path] = (
                                        f"{previous_stage_error}; WebDAV fallback request completed successfully; waiting for ONLINE"
                                    )
                                if not fallback_inflight_paths and len(fallback_success_paths) > fallback_success_log_count:
                                    bar.finish()
                                    LOG.info(
                                        "fallback: WebDAV priming requests finished for %d file(s) in batch %d/%d; waiting for them to come online",
                                        len(fallback_success_paths),
                                        batch_num,
                                        total_batches,
                                    )
                                    bar.update()
                                    fallback_success_log_count = len(fallback_success_paths)
                            continue
                        if fallback_path not in pending_stage:
                            continue
                        fallback_failure_paths.add(fallback_path)
                        stage_error_messages[fallback_path] = str(fallback_exc)
                        pending_stage.discard(fallback_path)
                        progress.clear_stage_state(fallback_path, int(batch_path_to_entry[fallback_path].get("size", 0)))
                        _handle_failed_result(batch_path_to_entry[fallback_path], fallback_exc, progress, bar)

                # Check which pending files are now online
                newly_online: list[str] = []
                if pending_stage:
                    request_failures = stage_mgr.poll_stage_request_errors(batch_request_ids, pending_stage)
                    for path, error in request_failures.items():
                        if path not in fallback_requested_paths:
                            stage_error_messages[path] = error
                    newly_failed_stage_paths = [path for path in request_failures if path in pending_stage]
                    new_fallback_paths: list[str] = []
                    terminal_stage_failures: list[str] = []
                    for path in newly_failed_stage_paths:
                        if fallback_pool is not None:
                            if path in fallback_requested_paths:
                                continue
                            new_fallback_paths.append(path)
                            continue
                        terminal_stage_failures.append(path)

                    if new_fallback_paths:
                        preview = ", ".join(batch_path_to_entry[path]["rel"] for path in new_fallback_paths[:3])
                        suffix = "" if len(new_fallback_paths) <= 3 else f" (+{len(new_fallback_paths) - 3} more)"
                        bar.finish()
                        LOG.info(
                            "fallback: switching %d file(s) in batch %d/%d to WebDAV range-read staging after ada stage failure: %s%s",
                            len(new_fallback_paths),
                            batch_num,
                            total_batches,
                            preview,
                            suffix,
                        )
                        bar.update()
                        for path in new_fallback_paths:
                            fallback_pool.submit({
                                "remote_path": path,
                                "rel": batch_path_to_entry[path]["rel"],
                                "size": batch_path_to_entry[path].get("size", 0),
                            })
                            fallback_requested_paths.add(path)
                            fallback_inflight_paths.add(path)
                            stage_error_messages[path] = f"{request_failures[path]}; trying WebDAV range-read fallback"
                        bar.update()

                    for path in terminal_stage_failures:
                        pending_stage.discard(path)
                        progress.clear_stage_state(path, int(batch_path_to_entry[path].get("size", 0)))
                        _handle_failed_result(
                            batch_path_to_entry[path],
                            RuntimeError(request_failures[path]),
                            progress,
                            bar,
                        )

                    newly_online, poll_errors = stage_mgr.poll_online_statuses(list(pending_stage))
                    for path, error in poll_errors.items():
                        stage_error_messages[path] = error
                    for path in newly_online:
                        pending_stage.discard(path)
                        stage_error_messages.pop(path, None)
                        progress.mark_stage_online(path, int(batch_path_to_entry[path].get("size", 0)))
                        entry = batch_path_to_entry[path]
                        pool.submit(entry)
                        pending_downloads += 1
                    if newly_online:
                        bar.update()

                # Check completed downloads
                done_results = 0
                newly_destageable_paths: list[str] = []
                while pending_downloads:
                    try:
                        entry, exc, result = pool.get_result_nowait()
                    except queue.Empty:
                        break
                    pending_downloads -= 1
                    done_results += 1
                    handled = _handle_worker_result(entry, exc, result, progress, bar, result_handler=result_handler)
                    if handled is not None:
                        batch_completed_bytes += int(entry.get("size", 0))
                        batch_completed_files += 1
                        newly_destageable_paths.append("/" + str(entry["remote_path"]).strip("/"))

                if destage and newly_destageable_paths:
                    deferred_destage_paths.extend(
                        _destage_paths(stage_mgr, newly_destageable_paths, batch_num, bar)
                    )

                if (
                    not prefetched_next_batch
                    and next_batch_paths
                    and not pending_stage
                ):
                    if batch_total_bytes > 0:
                        completion_fraction = batch_completed_bytes / batch_total_bytes
                    else:
                        completion_fraction = batch_completed_files / len(batch_entries) if batch_entries else 1.0
                    if completion_fraction >= 0.5:
                        bar.finish()
                        stage_request_ids[next_batch_index] = stage_mgr.stage(next_batch_paths, lifetime=stage_lifetime)
                        stage_started_at[next_batch_index] = time.monotonic()
                        prestaged_batches.add(next_batch_index)
                        if not fallback_enabled:
                            for path, size in next_batch_stage_entries:
                                progress.mark_stage_requested(path, size)
                        prefetched_next_batch = True
                        LOG.info("pre-staging batch %d/%d while batch %d is still copying", next_batch_index + 1, total_batches, batch_num)
                        bar.update()

                # Check staging timeout
                if pending_stage:
                    if time.monotonic() - stage_start > stage_timeout:
                        if is_tty:
                            sys.stderr.write("\r\033[K")
                            sys.stderr.flush()
                        raise TimeoutError(_format_stage_timeout_error(pending_stage, stage_error_messages))
                    if not done_results and not newly_online:
                        time.sleep(stage_poll)
                elif pending_downloads:
                    # All staged, just wait for downloads to finish
                    try:
                        entry, exc, result = pool.get_result(timeout=10)
                    except queue.Empty:
                        pass
                    else:
                        pending_downloads -= 1
                        handled = _handle_worker_result(entry, exc, result, progress, bar, result_handler=result_handler)
                        if handled is not None:
                            batch_completed_bytes += int(entry.get("size", 0))
                            batch_completed_files += 1
                            completed_path = "/" + str(entry["remote_path"]).strip("/")
                            if destage:
                                deferred_destage_paths.extend(
                                    _destage_paths(stage_mgr, [completed_path], batch_num, bar)
                                )

        except BaseException:
            pool.stop()
            if fallback_pool is not None:
                fallback_pool.stop()
            progress.clear_stage_states([(path, int(entry.get("size", 0))) for path, entry in batch_path_to_entry.items()])
            if destage and deferred_destage_paths:
                _destage_paths(stage_mgr, deferred_destage_paths, batch_num, bar)
            if prefetched_next_batch and next_batch_paths:
                _destage_paths(stage_mgr, next_batch_paths, next_batch_index + 1, bar)
                progress.clear_stage_states(next_batch_stage_entries)
            bar.update()
            raise
        finally:
            pool.finish_submissions()
            pool.join(timeout=1)
            if fallback_pool is not None:
                fallback_pool.finish_submissions()
                fallback_pool.join(timeout=1)

        if destage and deferred_destage_paths:
            _destage_paths(stage_mgr, deferred_destage_paths, batch_num, bar)


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
    p.add_argument(
        "--literal-file-list",
        action="store_true",
        help=(
            "Treat download file-list sources as exact physical paths; skip "
            "parent-directory enumeration and bundle resolution"
        ),
    )
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
    p.add_argument("--bundle-small-files", action="store_true",
                    help="Bundle eligible upload directories into SquashFS archives before upload with append-only remote reuse")
    p.add_argument("--bundle-format", default=DEFAULT_BUNDLE_FORMAT,
                    choices=[DEFAULT_BUNDLE_FORMAT],
                    help="Bundle archive format (currently only squashfs is supported)")
    p.add_argument("--bundle-target-size", type=parse_size_literal, default=DEFAULT_BUNDLE_TARGET_SIZE,
                    metavar="SIZE",
                    help="Target uncompressed bytes per bundle (default: 1GiB)")
    p.add_argument("--bundle-max-file-size", type=parse_size_literal, default=DEFAULT_BUNDLE_MAX_FILE_SIZE,
                    metavar="SIZE",
                    help="Maximum file size eligible for bundling (default: 64MiB)")
    p.add_argument("--bundle-min-dir-total", type=parse_size_literal, default=DEFAULT_BUNDLE_MIN_DIR_TOTAL,
                    metavar="SIZE",
                    help="Minimum total bytes in a directory before bundling activates (default: 256MiB)")
    p.add_argument("--bundle-max-members", type=int, default=DEFAULT_BUNDLE_MAX_MEMBERS,
                    metavar="N",
                    help="Maximum members per bundle (default: 10000)")
    p.add_argument("--bundle-keep-temp", action="store_true",
                    help="Keep temporary local bundle archives after upload for debugging")
    p.add_argument("--no-unpack-bundles", action="store_true",
                    help="Download raw .dcpbundle objects instead of transparently unpacking bundled content")
    p.add_argument("--ignore-bundle-xattrs", action="store_true",
                    help="Ignore bundle xattrs and treat remote layout as plain physical files")
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
        if args.bundle_small_files:
            LOG.error("--bundle-small-files is not supported together with --file-list")
            return 1
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

    if args.bundle_small_files:
        if direction != "upload":
            LOG.error("--bundle-small-files is only supported for uploads")
            return 1
        if args.file_list:
            LOG.error("--bundle-small-files is not supported together with --file-list yet")
            return 1
        if not args.recursive:
            LOG.error("--bundle-small-files requires -R/--recursive")
            return 1
        for src_raw in sources_raw:
            bundle_source = Path(parse_remote_prefix(src_raw)[1]).expanduser()
            if not bundle_source.is_dir():
                LOG.error("--bundle-small-files requires directory sources, but %s is not a directory", bundle_source)
                return 1

    # ---- Resolve config ----
    rclone_config = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(rclone_config)
    remote = resolve_remote_name(config, args.remote)
    api = resolve_api_url(args.api, config[remote])
    if args.literal_file_list:
        if not args.file_list or direction != "download":
            LOG.error("--literal-file-list requires a download --file-list")
            return 1
        if delete_source:
            LOG.error("--literal-file-list is not supported for download moves")
            return 1
    download_bundle_enabled = (
        direction == "download"
        and not args.literal_file_list
        and not args.no_unpack_bundles
        and not args.ignore_bundle_xattrs
    )
    download_bundle_entries: list[dict] = []
    download_bundle_resolver: _BundleDownloadResolver | None = None
    bundle_upload_resolver: _BundleUploadResolver | None = None

    def _get_download_bundle_resolver() -> _BundleDownloadResolver:
        nonlocal download_bundle_resolver
        if download_bundle_resolver is None:
            download_bundle_resolver = _BundleDownloadResolver(rclone_config, remote, api, config[remote])
        return download_bundle_resolver

    if args.file_list and delete_source and direction == "upload":
        files = _filter_resumed_move_upload_file_list_entries(rclone_config, remote, files)

    if args.file_list and direction == "download":
        try:
            if args.literal_file_list:
                # The caller guarantees these are exact physical paths.
                # Keep their unknown size at zero for progress accounting;
                # rclone reports missing paths and Adler-32 verification still
                # gates atomic installation of every downloaded file.
                pass
            elif download_bundle_enabled:
                files, download_bundle_entries = _plan_file_list_downloads_with_bundles(
                    rclone_config,
                    remote,
                    files,
                    _get_download_bundle_resolver(),
                    allow_resumed_move=delete_source,
                )
            else:
                files = _fill_file_list_download_sizes(
                    rclone_config,
                    remote,
                    files,
                    allow_resumed_move=delete_source,
                )
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            LOG.error("%s", exc)
            return 1

    # ---- Plan downloads if not from file-list ----
    if not args.file_list and direction == "download":
        plain_download_entries: list[dict] = []
        pending_bundle_entries: list[dict] = []
        with _EnumSpinner("listing remote") as spinner:
            for src_raw in sources_raw:
                _, src_path = parse_remote_prefix(src_raw)
                try:
                    if download_bundle_enabled:
                        plain_part, bundle_part = _plan_download_source_with_bundles(
                            rclone_config,
                            remote,
                            src_path,
                            Path(dst_path_dir),
                            args.recursive,
                            _get_download_bundle_resolver(),
                            spinner=spinner,
                        )
                        plain_download_entries.extend(plain_part)
                        pending_bundle_entries.extend(bundle_part)
                    else:
                        plain_download_entries.extend(
                            plan_download(
                                rclone_config,
                                remote,
                                src_path,
                                Path(dst_path_dir),
                                args.recursive,
                                spinner=spinner,
                            )
                        )
                except (FileNotFoundError, RuntimeError, ValueError) as exc:
                    LOG.error("%s", exc)
                    return 1
        files = plain_download_entries
        download_bundle_entries = _merge_bundle_download_entries(pending_bundle_entries)

    # ---- Validate ----
    if args.workers < 1:
        LOG.error("--workers must be >= 1"); return 1
    if args.max_retries < 0:
        LOG.error("--max-retries must be >= 0"); return 1
    if args.stage_batch < 1:
        LOG.error("--stage-batch must be >= 1"); return 1
    if args.stage_batch_bytes < 1:
        LOG.error("--stage-batch-bytes must be >= 1"); return 1

    if not files and not download_bundle_entries:
        LOG.info("no files to process")
        return 0

    planned_entries = [*files, *download_bundle_entries]
    planned_total_files = sum(_entry_logical_file_count(entry) for entry in planned_entries)
    planned_total_bytes = sum(_entry_logical_bytes(entry) for entry in planned_entries)

    bundle_plan: BundleUploadPlan | None = None
    if direction == "upload" and args.bundle_small_files:
        bundle_options = BundleOptions(
            format=args.bundle_format,
            target_size=args.bundle_target_size,
            max_file_size=args.bundle_max_file_size,
            min_dir_total=args.bundle_min_dir_total,
            max_members=args.bundle_max_members,
            keep_temp=args.bundle_keep_temp,
        )
        try:
            bundle_upload_resolver = _BundleUploadResolver(rclone_config, remote, api, config[remote])
            bundle_plan = _build_incremental_bundle_upload_plan(files, bundle_options, bundle_upload_resolver)
        except (NamespaceXattrError, RuntimeError, ValueError) as exc:
            LOG.error("%s", exc)
            return 1
        files = list(bundle_plan.plain_entries)

    # ---- Transferer ----
    transferer = Transferer(
        rclone_config=rclone_config, remote=remote, ada_cmd=args.ada,
        api=api, max_retries=args.max_retries, retry_wait=args.retry_wait,
        copy_timeout=args.copy_timeout, checksum_timeout=args.checksum_timeout,
        skip_verified=args.skip_verified, delete_source=delete_source,
    )

    pre_skipped_entries: list[dict] = []
    if direction == "download" and args.skip_verified and not delete_source:
        LOG.info("resume  : checking for already verified local files before download")
        files, skipped_plain_entries = _filter_verified_download_entries(files, transferer)
        pre_skipped_entries.extend(skipped_plain_entries)
        if download_bundle_entries:
            download_bundle_entries, skipped_bundle_entries = _filter_verified_download_entries(download_bundle_entries, transferer)
            pre_skipped_entries.extend(skipped_bundle_entries)

    total_bytes = planned_total_bytes
    remaining_bytes = sum(_entry_logical_bytes(entry) for entry in [*files, *download_bundle_entries])

    # ---- Dry run ----
    if args.dry_run:
        LOG.info("dry run (%s): %d logical file(s), %s", direction, planned_total_files, format_bytes(planned_total_bytes))
        plain_remaining_bytes = sum(_entry_logical_bytes(entry) for entry in files)
        if bundle_plan and bundle_plan.bundled_file_count:
            LOG.info(
                "bundles : %d new bundle object(s) covering %d logical file(s), %s",
                bundle_plan.bundle_count,
                bundle_plan.bundled_file_count,
                format_bytes(bundle_plan.bundled_total_bytes),
            )
            if bundle_plan.reused_file_count:
                LOG.info(
                    "resume  : %d bundled logical file(s) already present remotely and will be reused",
                    bundle_plan.reused_file_count,
                )
            for anchor_plan in bundle_plan.anchors:
                if anchor_plan.reused_members:
                    LOG.info(
                        "  reuse %s (%d files)",
                        anchor_plan.anchor_dir or "/",
                        len(anchor_plan.reused_members),
                    )
                for job in anchor_plan.bundles:
                    LOG.info(
                        "  bundle %s -> %s (%d files, %s)",
                        job.bundle_id[:12],
                        job.remote_path,
                        job.logical_file_count,
                        format_bytes(job.logical_total_bytes),
                    )
        elif download_bundle_entries:
            bundled_file_count = sum(_entry_logical_file_count(entry) for entry in download_bundle_entries)
            bundled_total_bytes = sum(_entry_logical_bytes(entry) for entry in download_bundle_entries)
            LOG.info(
                "bundles : %d bundle object(s) covering %d logical file(s), %s",
                len(download_bundle_entries),
                bundled_file_count,
                format_bytes(bundled_total_bytes),
            )
            for entry in download_bundle_entries:
                bundle_members = _bundle_members_for_entry(entry)
                sparse_planned = Transferer._should_try_sparse_bundle_read(
                    entry,
                    bundle_members,
                    str(entry.get("bundle_format", DEFAULT_BUNDLE_FORMAT)),
                )
                LOG.info(
                    "  bundle %s -> %s (%d files, %s, %s)",
                    str(entry["bundle_id"])[:12],
                    entry["remote_path"],
                    _entry_logical_file_count(entry),
                    format_bytes(_entry_logical_bytes(entry)),
                    "sparse member read" if sparse_planned else "full bundle read",
                )
        if files:
            LOG.info("plain   : %d physical file(s), %s", len(files), format_bytes(plain_remaining_bytes))
        for e in files:
            if direction == "upload":
                LOG.info("  %s -> %s (%s)", e["rel"], e["remote_path"], format_bytes(e.get("size", 0)))
            else:
                LOG.info("  %s -> %s (%s)", e["remote_path"], e.get("local_path", "?"), format_bytes(e.get("size", 0)))
        for entry in download_bundle_entries:
            for member in _bundle_members_for_entry(entry):
                LOG.info(
                    "  %s -> %s (%s)%s",
                    member["remote_path"],
                    member["local_path"],
                    format_bytes(int(member.get("size", 0))),
                    f" via {entry['remote_path']}" if args.verbose else "",
                )
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
    LOG.info("%sfiles%s   : %s%d%s (%s)", _C.DIM, _C.RESET, _C.CYAN, planned_total_files, _C.RESET, format_bytes(total_bytes))
    LOG.info("%sworkers%s : %d  retries: %d  skip-verified: %s",
             _C.DIM, _C.RESET, args.workers, args.max_retries, "yes" if args.skip_verified else "no")
    if delete_source:
        LOG.info("%ssource%s  : delete after verified transfer", _C.DIM, _C.RESET)
    if bundle_plan is not None:
        if bundle_plan.bundled_file_count:
            LOG.info(
                "%sbundles%s : %s%d%s new bundle object(s), covering %s%d%s logical files (%s)",
                _C.DIM,
                _C.RESET,
                _C.CYAN,
                bundle_plan.bundle_count,
                _C.RESET,
                _C.CYAN,
                bundle_plan.bundled_file_count,
                _C.RESET,
                format_bytes(bundle_plan.bundled_total_bytes),
            )
            if bundle_plan.reused_file_count:
                LOG.info(
                    "%sresume%s  : %s%d%s bundled logical file(s) already present remotely and will be reused",
                    _C.DIM,
                    _C.RESET,
                    _C.CYAN,
                    bundle_plan.reused_file_count,
                    _C.RESET,
                )
        else:
            LOG.info("%sbundles%s : enabled, but no fully eligible directories met the current thresholds", _C.DIM, _C.RESET)
    elif direction == "download" and download_bundle_enabled:
        if download_bundle_entries:
            LOG.info(
                "%sbundles%s : transparent unpack via %s%d%s bundle object(s), covering %s%d%s logical files (%s)",
                _C.DIM,
                _C.RESET,
                _C.CYAN,
                len(download_bundle_entries),
                _C.RESET,
                _C.CYAN,
                sum(_entry_logical_file_count(entry) for entry in download_bundle_entries),
                _C.RESET,
                format_bytes(sum(_entry_logical_bytes(entry) for entry in download_bundle_entries)),
            )
        else:
            LOG.info("%sbundles%s : transparent unpack enabled", _C.DIM, _C.RESET)
    if direction == "download" and not args.no_stage:
        LOG.info("%sstaging%s : max-files=%d  max-bytes=%s  lifetime=%s  poll=%ds  timeout=%s",
                 _C.DIM, _C.RESET, args.stage_batch, format_bytes(args.stage_batch_bytes), args.stage_lifetime, args.stage_poll,
                 fmt_duration(args.stage_timeout))
    if pre_skipped_entries:
        LOG.info("%sresume%s  : %s%d%s already verified, skipping stage/copy for %s",
                 _C.DIM, _C.RESET, _C.CYAN,
                 sum(_entry_logical_file_count(entry) for entry in pre_skipped_entries), _C.RESET,
                 format_bytes(sum(_entry_logical_bytes(entry) for entry in pre_skipped_entries)))
    if quota and quota.ok:
        LOG.info("%squota%s   : %s", _C.DIM, _C.RESET, quota.summary_line())
    LOG.info("")

    has_bundle_upload_work = direction == "upload" and bundle_plan is not None and bundle_plan.bundled_file_count > 0
    if not files and not download_bundle_entries and not has_bundle_upload_work:
        LOG.info("all planned download files are already verified locally")
        return 0

    bundle_transferer: Transferer | None = None
    bundle_xattr_client: NamespaceXattrClient | None = None
    download_bundle_transferer: Transferer | None = None
    download_bundle_result_handler = None
    if bundle_plan is not None and bundle_plan.bundled_file_count:
        assert bundle_upload_resolver is not None
        bundle_xattr_client = bundle_upload_resolver._require_client()
        bundle_transferer = Transferer(
            rclone_config=rclone_config,
            remote=remote,
            ada_cmd=args.ada,
            api=api,
            max_retries=args.max_retries,
            retry_wait=args.retry_wait,
            copy_timeout=args.copy_timeout,
            checksum_timeout=args.checksum_timeout,
            skip_verified=False,
            delete_source=False,
        )
    if direction == "download" and download_bundle_entries and delete_source:
        try:
            bundle_move_xattr_client = _get_download_bundle_resolver()._require_client()
        except RuntimeError as exc:
            LOG.error("%s", exc)
            return 1
        download_bundle_transferer = Transferer(
            rclone_config=rclone_config,
            remote=remote,
            ada_cmd=args.ada,
            api=api,
            max_retries=args.max_retries,
            retry_wait=args.retry_wait,
            copy_timeout=args.copy_timeout,
            checksum_timeout=args.checksum_timeout,
            skip_verified=args.skip_verified,
            delete_source=False,
        )

        def _download_bundle_result_handler(entry: dict, result: dict) -> None:
            assert download_bundle_transferer is not None
            _commit_bundle_download_move(entry, result, bundle_move_xattr_client, download_bundle_transferer)

        download_bundle_result_handler = _download_bundle_result_handler

    progress = Progress(total_files=planned_total_files, total_bytes=total_bytes)
    _apply_pre_skipped_entries(progress, pre_skipped_entries)
    transferer.progress = progress
    if bundle_transferer is not None:
        bundle_transferer.progress = progress
    if download_bundle_transferer is not None:
        download_bundle_transferer.progress = progress
    bar = ProgressBar(progress, quota=quota)

    interrupted = False
    try:
        if direction == "upload":
            if files:
                _execute_simple(files, transferer.upload, args.workers, progress, bar)
            if bundle_plan is not None and bundle_plan.bundled_file_count:
                assert bundle_transferer is not None
                assert bundle_xattr_client is not None
                _execute_bundle_uploads(
                    bundle_plan,
                    bundle_transferer,
                    bundle_xattr_client,
                    progress,
                    bar,
                    delete_source=delete_source,
                    keep_temp=args.bundle_keep_temp,
                )
        elif args.no_stage:
            if files:
                _execute_simple(files, transferer.download, args.workers, progress, bar)
            if download_bundle_entries:
                active_bundle_download_transferer = download_bundle_transferer or transferer
                _execute_simple(
                    download_bundle_entries,
                    active_bundle_download_transferer.download_bundle,
                    args.workers,
                    progress,
                    bar,
                    result_handler=download_bundle_result_handler,
                )
        else:
            stage_mgr = StageManager(args.ada, rclone_config, api, config[remote])
            if files:
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
                    worker_fn=transferer.download,
                )
            if download_bundle_entries:
                active_bundle_download_transferer = download_bundle_transferer or transferer
                _execute_pipeline_download(
                    files=download_bundle_entries,
                    transferer=active_bundle_download_transferer,
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
                    worker_fn=active_bundle_download_transferer.download_bundle,
                    result_handler=download_bundle_result_handler,
                )
    except KeyboardInterrupt:
        interrupted = True
    finally:
        bar.stop()
        bar.finish()
        if quota_poller:
            quota_poller.stop()
        transferer.close()
        if bundle_transferer is not None:
            bundle_transferer.close()
        if download_bundle_transferer is not None:
            download_bundle_transferer.close()

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
