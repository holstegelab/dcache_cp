#!/usr/bin/env python3
"""dcache_ls — list dCache directory contents like ``ls``.

Usage::

    dcache_ls dcache:/data/
    dcache_ls -l analysis:/archive/run42/
    dcache_ls -lH --pin dcache:/data/
    dcache_ls -R dcache:/data/

The remote prefix (e.g. ``dcache:``) selects the config file
following the same conventions as ``dcache_cp``.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .bundles import bundle_object_remote_path, decode_anchor_routes, decode_bundle_deleted, decode_bundle_deprecated, decode_bundle_members

BUNDLE_STORE_DIR = ".dcpacks"
from .cli import (
    MACAROON_DIR,
    _ada_reports_missing_path,
    _ada_tokenfile_cmd,
    _default_ada,
    format_bytes,
    parse_remote_prefix,
    resolve_config_for_prefix,
    load_rclone_config,
    resolve_remote_name,
    resolve_api_url,
    run_command,
)
from .xattrs import NamespaceXattrClient, NamespaceXattrError, extract_bearer_token

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
    # bundle membership (set for files pulled out of a bundle)
    bundle_number: int | None = None
    bundle_remote_path: str = ""
    status_tag: str = ""


@dataclass
class _BundleMemberView:
    name: str
    logical_remote_path: str
    anchor_rel: str
    size: int
    mode: int
    mtime_ns: int
    adler32: str
    is_deleted: bool = False
    is_deprecated: bool = False
    replacement_bundle_id: str = ""


@dataclass
class _BundleGroupView:
    bundle_remote_path: str
    display_path: str
    members: list[_BundleMemberView] = field(default_factory=list)
    total_member_count: int = 0
    deleted_count: int = 0
    deprecated_count: int = 0
    locality: str = ""


def _active_member_count(group: _BundleGroupView) -> int:
    return max(0, group.total_member_count - group.deleted_count - group.deprecated_count)


def _clean_remote_path(remote_path: str) -> str:
    return str(remote_path).strip("/")


def _candidate_anchor_dirs_for_directory(remote_dir: str) -> list[str]:
    current = _clean_remote_path(remote_dir)
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


class _BundleLsResolver:
    def __init__(
        self,
        rclone_config: Path,
        api: str | None,
        remote_cfg,
        *,
        ada_cmd: str | None = None,
    ):
        self.rclone_config = rclone_config
        self.api = api
        self.remote_cfg = remote_cfg
        self.ada_cmd = ada_cmd
        self._client: NamespaceXattrClient | None = None
        self._anchor_routes_cache: dict[str, dict[str, str]] = {}
        self._bundle_cache: dict[str, dict[str, object]] = {}
        self._bundle_locality_cache: dict[str, dict[str, str]] = {}
        self._warning_emitted = False

    def _warn_once(self, message: str) -> None:
        if self._warning_emitted:
            return
        print(f"warning: {message}", file=sys.stderr)
        self._warning_emitted = True

    def _maybe_client(self) -> NamespaceXattrClient | None:
        if self._client is not None:
            return self._client
        if not self.api:
            self._warn_once("bundle view requires a resolved dCache API URL; showing physical listing only")
            return None
        token = extract_bearer_token(self.remote_cfg, self.rclone_config)
        if not token:
            self._warn_once("bundle view requires a bearer token in the selected config; showing physical listing only")
            return None
        self._client = NamespaceXattrClient(self.api, token)
        return self._client

    def anchor_routes(self, anchor_dir: str) -> dict[str, str]:
        anchor_dir = _clean_remote_path(anchor_dir)
        if anchor_dir in self._anchor_routes_cache:
            return dict(self._anchor_routes_cache[anchor_dir])
        client = self._maybe_client()
        if client is None:
            return {}
        try:
            xattrs = client.list_xattrs(anchor_dir)
        except NamespaceXattrError as exc:
            if exc.status == 404:
                self._anchor_routes_cache[anchor_dir] = {}
                return {}
            self._warn_once(f"bundle view xattr lookup failed for {anchor_dir or '/'}: {exc}")
            return {}
        routes = decode_anchor_routes(xattrs)
        self._anchor_routes_cache[anchor_dir] = routes
        return dict(routes)

    def bundle_details(self, anchor_dir: str, bundle_id: str) -> dict[str, object] | None:
        anchor_dir = _clean_remote_path(anchor_dir)
        bundle_remote_path = bundle_object_remote_path(anchor_dir, bundle_id)
        if bundle_remote_path in self._bundle_cache:
            cached = self._bundle_cache[bundle_remote_path]
            return dict(cached)
        client = self._maybe_client()
        if client is None:
            return None
        try:
            xattrs = client.list_xattrs(bundle_remote_path)
        except NamespaceXattrError as exc:
            if exc.status == 404:
                self._warn_once(f"bundle view skipped missing bundle object {bundle_remote_path}")
                return None
            self._warn_once(f"bundle view xattr lookup failed for {bundle_remote_path}: {exc}")
            return None
        details = {
            "anchor_dir": anchor_dir,
            "bundle_id": bundle_id,
            "remote_path": bundle_remote_path,
            "members": decode_bundle_members(xattrs),
            "deleted": decode_bundle_deleted(xattrs),
            "deprecated": decode_bundle_deprecated(xattrs),
        }
        self._bundle_cache[bundle_remote_path] = details
        return dict(details)

    def _bundle_store_dir(self, anchor_dir: str) -> str:
        anchor_dir = _clean_remote_path(anchor_dir)
        return posixpath.join(anchor_dir, BUNDLE_STORE_DIR, "bundles") if anchor_dir else posixpath.join(BUNDLE_STORE_DIR, "bundles")

    def bundle_locality_map(self, anchor_dir: str) -> dict[str, str]:
        anchor_dir = _clean_remote_path(anchor_dir)
        if anchor_dir in self._bundle_locality_cache:
            return dict(self._bundle_locality_cache[anchor_dir])
        if not self.ada_cmd:
            return {}
        store_dir = self._bundle_store_dir(anchor_dir)
        try:
            data = _ada_stat(self.ada_cmd, self.rclone_config, self.api, store_dir)
        except RuntimeError:
            self._bundle_locality_cache[anchor_dir] = {}
            return {}
        children = data.get("children") if isinstance(data, dict) else None
        out: dict[str, str] = {}
        if isinstance(children, list):
            for entry in children:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("fileName")
                if not isinstance(name, str) or not name.endswith(".dcpbundle"):
                    continue
                bundle_id = name[: -len(".dcpbundle")]
                locality = entry.get("fileLocality")
                if isinstance(locality, str):
                    out[bundle_id] = locality
        self._bundle_locality_cache[anchor_dir] = out
        return dict(out)

    def bundle_groups_for_directory(self, remote_dir: str, *, include_deleted: bool = False) -> list[_BundleGroupView]:
        remote_dir = _clean_remote_path(remote_dir)
        visible_members: dict[str, tuple[str, _BundleMemberView]] = {}

        for anchor_dir in _candidate_anchor_dirs_for_directory(remote_dir):
            routes = self.anchor_routes(anchor_dir)
            if not routes:
                continue
            directory_rel = posixpath.relpath(remote_dir, anchor_dir) if anchor_dir else remote_dir
            prefix = "" if directory_rel in {"", "."} else directory_rel.strip("/") + "/"

            for anchor_rel, bundle_id in sorted(routes.items()):
                if prefix:
                    if not anchor_rel.startswith(prefix):
                        continue
                    rel_from_dir = anchor_rel[len(prefix):]
                else:
                    rel_from_dir = anchor_rel
                if not rel_from_dir or "/" in rel_from_dir:
                    continue

                logical_remote_path = posixpath.join(remote_dir, rel_from_dir) if remote_dir else rel_from_dir
                if logical_remote_path in visible_members:
                    continue

                details = self.bundle_details(anchor_dir, bundle_id)
                if details is None:
                    continue

                member = details["members"].get(anchor_rel)
                if member is None:
                    continue

                deleted = details.get("deleted")
                is_deleted = isinstance(deleted, dict) and anchor_rel in deleted
                if is_deleted and not include_deleted:
                    continue
                deprecated = details.get("deprecated")
                is_deprecated = isinstance(deprecated, dict) and anchor_rel in deprecated
                replacement_id = ""
                if is_deprecated:
                    replacement_id = deprecated[anchor_rel].replacement_bundle_id

                visible_members[logical_remote_path] = (
                    str(details["remote_path"]),
                    _BundleMemberView(
                        name=rel_from_dir,
                        logical_remote_path=logical_remote_path,
                        anchor_rel=anchor_rel,
                        size=int(member.size),
                        mode=int(member.mode),
                        mtime_ns=int(member.mtime_ns),
                        adler32=str(member.adler32),
                        is_deleted=is_deleted,
                        is_deprecated=is_deprecated,
                        replacement_bundle_id=replacement_id,
                    ),
                )

        grouped: dict[str, _BundleGroupView] = {}
        group_origin: dict[str, tuple[str, str]] = {}
        for bundle_remote_path, member_view in visible_members.values():
            group = grouped.get(bundle_remote_path)
            if group is None:
                display_path = posixpath.relpath(bundle_remote_path, remote_dir) if remote_dir else bundle_remote_path
                group = _BundleGroupView(
                    bundle_remote_path=bundle_remote_path,
                    display_path=display_path,
                )
                grouped[bundle_remote_path] = group
                cached = self._bundle_cache.get(bundle_remote_path)
                if cached is not None:
                    group_origin[bundle_remote_path] = (
                        str(cached.get("anchor_dir", "")),
                        str(cached.get("bundle_id", "")),
                    )
            group.total_member_count += 1
            if member_view.is_deleted:
                group.deleted_count += 1
            if member_view.is_deprecated:
                group.deprecated_count += 1
            group.members.append(member_view)

        out = [grouped[path] for path in sorted(grouped)]
        locality_by_anchor: dict[str, dict[str, str]] = {}
        for group in out:
            origin = group_origin.get(group.bundle_remote_path)
            if origin is not None:
                anchor_dir, bundle_id = origin
                if anchor_dir not in locality_by_anchor:
                    locality_by_anchor[anchor_dir] = self.bundle_locality_map(anchor_dir)
                group.locality = locality_by_anchor[anchor_dir].get(bundle_id, "")
            group.members.sort(key=lambda item: (item.is_deleted, item.is_deprecated, item.name.lower()))
        return out

    def logical_member_for_path(
        self,
        remote_path: str,
        *,
        include_deleted: bool = False,
    ) -> tuple[_BundleGroupView, _BundleMemberView] | None:
        remote_path = _clean_remote_path(remote_path)
        parent_dir = posixpath.dirname(remote_path)

        for anchor_dir in _candidate_anchor_dirs_for_directory(parent_dir):
            anchor_rel = posixpath.relpath(remote_path, anchor_dir) if anchor_dir else remote_path
            routes = self.anchor_routes(anchor_dir)
            bundle_id = routes.get(anchor_rel)
            if not bundle_id:
                continue

            details = self.bundle_details(anchor_dir, bundle_id)
            if details is None:
                continue
            member = details["members"].get(anchor_rel)
            if member is None:
                return None

            deleted = details.get("deleted")
            is_deleted = isinstance(deleted, dict) and anchor_rel in deleted
            if is_deleted and not include_deleted:
                return None
            deprecated = details.get("deprecated")
            is_deprecated = isinstance(deprecated, dict) and anchor_rel in deprecated
            replacement_id = deprecated[anchor_rel].replacement_bundle_id if is_deprecated else ""

            member_view = _BundleMemberView(
                name=posixpath.basename(remote_path),
                logical_remote_path=remote_path,
                anchor_rel=anchor_rel,
                size=int(member.size),
                mode=int(member.mode),
                mtime_ns=int(member.mtime_ns),
                adler32=str(member.adler32),
                is_deleted=is_deleted,
                is_deprecated=is_deprecated,
                replacement_bundle_id=replacement_id,
            )
            group = _BundleGroupView(
                bundle_remote_path=str(details["remote_path"]),
                display_path=posixpath.relpath(str(details["remote_path"]), parent_dir) if parent_dir else str(details["remote_path"]),
                members=[member_view],
                total_member_count=1,
                deleted_count=1 if is_deleted else 0,
                deprecated_count=1 if is_deprecated else 0,
                locality=self.bundle_locality_map(anchor_dir).get(bundle_id, ""),
            )
            return group, member_view

        return None


def _bundle_header_display(group: _BundleGroupView) -> str:
    path = group.bundle_remote_path
    name = posixpath.basename(path)
    if name.endswith(".dcpbundle"):
        bundle_id = name[: -len(".dcpbundle")]
        return f"bundle {bundle_id}"
    return group.display_path


def _bundle_status_summary(group: _BundleGroupView) -> str:
    parts: list[str] = []
    active = _active_member_count(group)
    if group.deleted_count:
        parts.append(f"{_C.RED}{group.deleted_count} deleted{_C.RESET}")
    if group.deprecated_count:
        parts.append(f"{_C.YELLOW}{group.deprecated_count} deprecated{_C.RESET}")
    if group.deleted_count == group.total_member_count:
        parts.insert(0, f"{_C.RED}retired{_C.RESET}")
    elif parts:
        parts.insert(0, f"{active}/{group.total_member_count} active")
    return f"  ({', '.join(parts)})" if parts else ""


def _bundle_member_row(
    member: _BundleMemberView,
    *,
    human: bool,
    bundle_number: int | None = None,
    bundle_remote_path: str = "",
    bundle_locality: str = "",
) -> _Row:
    if member.is_deleted:
        name_color = _C.DIM
        status_tag = f" {_C.RED}[deleted]{_C.RESET}"
    elif member.is_deprecated:
        name_color = _C.YELLOW
        status_tag = f" {_C.YELLOW}[deprecated]{_C.RESET}"
    else:
        name_color = _C.MAGENTA
        status_tag = ""
    return _Row(
        mode=_filemode("REG", member.mode),
        nlink="1",
        owner="-",
        group="-",
        size=_format_size(member.size, human),
        mtime=_format_mtime(member.mtime_ns),
        name=member.name,
        name_color=name_color,
        locality=bundle_locality,
        locality_color=_locality_color(bundle_locality) if bundle_locality else "",
        pin="",
        qos="",
        checksum=member.adler32,
        is_dir=False,
        bundle_number=bundle_number,
        bundle_remote_path=bundle_remote_path,
        status_tag=status_tag,
    )


def _merge_rows(
    physical_rows: list[_Row],
    groups: list[_BundleGroupView],
    *,
    human: bool,
) -> list[_Row]:
    rows = list(physical_rows)
    for i, group in enumerate(groups):
        for member in group.members:
            rows.append(_bundle_member_row(
                member,
                human=human,
                bundle_number=i + 1,
                bundle_remote_path=group.bundle_remote_path,
                bundle_locality=group.locality,
            ))
    rows.sort(key=lambda r: (not r.is_dir, r.name.lower()))
    return rows


def _render_bundle_legend(groups: list[_BundleGroupView]) -> None:
    if not groups:
        return
    print(f"{_C.DIM}bundles:{_C.RESET}")
    w_num = len(str(len(groups)))
    for i, group in enumerate(groups, start=1):
        header = _bundle_header_display(group)
        locality_text = ""
        if group.locality:
            color = _locality_color(group.locality)
            locality_text = f"  {color}[{group.locality}]{_C.RESET}" if color else f"  [{group.locality}]"
        status = _bundle_status_summary(group)
        print(f"  {i:>{w_num}}  {_C.MAGENTA}{header}{_C.RESET}{locality_text}{status}")


def _bundle_cell(r: _Row, *, show_bundle_path: bool) -> str:
    if r.bundle_number is None:
        return ""
    if show_bundle_path:
        return f"bundle={r.bundle_remote_path}"
    return f"bundle={r.bundle_number}"


def _render_long(
    rows: list[_Row],
    *,
    show_pin: bool,
    show_locality: bool,
    show_checksum: bool,
    show_bundle_path: bool = False,
):
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

    show_bundle = any(r.bundle_number is not None for r in rows)
    if show_bundle:
        bundle_cells = [_bundle_cell(r, show_bundle_path=show_bundle_path) for r in rows]
        w_bundle = max(len(c) for c in bundle_cells)
    else:
        bundle_cells = [""] * len(rows)
        w_bundle = 0

    for r, bundle_cell in zip(rows, bundle_cells):
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
        if show_bundle:
            parts.append(f"{bundle_cell:<{w_bundle}}")

        name_display = f"{r.name_color}{r.name}{_C.RESET}" if r.name_color else r.name
        print(" ".join(parts) + " " + name_display + r.status_tag)


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
    cmd = _ada_tokenfile_cmd(ada_cmd, tokenfile, api) + ["--stat", "/" + remote_path.strip("/")]
    result = run_command(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"ada --stat failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        detail = (result.stderr.strip() or result.stdout.strip())
        if _ada_reports_missing_path(detail):
            raise RuntimeError(detail)
        stdout_preview = (result.stdout or "").strip()[:200]
        stderr_preview = (result.stderr or "").strip()[:200]
        raise RuntimeError(
            f"ada --stat returned non-JSON output for /{remote_path.strip('/')}.\n"
            f"stdout: {stdout_preview!r}\n"
            f"stderr: {stderr_preview!r}\n"
            f"Upgrade ada or set --ada to point to the bundled version."
        )


def _ada_checksum(ada_cmd: str, tokenfile: Path, api: str | None, remote_path: str) -> str:
    cmd = _ada_tokenfile_cmd(ada_cmd, tokenfile, api) + ["--checksum", "/" + remote_path.strip("/")]
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
    show_all: bool = False,
) -> list[_Row]:
    """List a single remote path. Returns rows for rendering."""
    data = _ada_stat(ada_cmd, tokenfile, api, remote_path)

    entries: list[dict]
    is_directory_listing = isinstance(data, dict) and isinstance(data.get("children"), list)
    if is_directory_listing:
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

        if is_directory_listing and not show_all and name == BUNDLE_STORE_DIR and file_type == "DIR":
            continue

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
    show_bundles: bool,
    show_deleted_bundles: bool,
    show_all: bool,
    show_bundle_path: bool,
    bundle_resolver: _BundleLsResolver | None,
    _first: bool = True,
) -> None:
    """Recursively list a directory tree."""
    physical_rows = _list_path(
        ada_cmd, tokenfile, api, remote_path,
        human=human, show_pin=show_pin, show_checksum=show_checksum,
        show_all=show_all,
    )

    groups: list[_BundleGroupView] = []
    if show_bundles and bundle_resolver is not None:
        groups = bundle_resolver.bundle_groups_for_directory(remote_path, include_deleted=show_deleted_bundles)

    rows = _merge_rows(physical_rows, groups, human=human)

    if not _first:
        print()
    path_display = "/" + remote_path.strip("/")
    print(f"{_C.BOLD}{path_display}:{_C.RESET}")

    if long_format:
        _render_long(
            rows,
            show_pin=show_pin,
            show_locality=show_locality,
            show_checksum=show_checksum,
            show_bundle_path=show_bundle_path,
        )
        if groups and not show_bundle_path:
            print()
            _render_bundle_legend(groups)
    else:
        _render_short(rows)

    # Recurse into subdirectories
    for r in physical_rows:
        if r.is_dir:
            subpath = remote_path.rstrip("/") + "/" + r.name.rstrip("/")
            _list_recursive(
                ada_cmd, tokenfile, api, subpath,
                human=human, show_pin=show_pin, show_checksum=show_checksum,
                long_format=long_format, show_locality=show_locality,
                show_bundles=show_bundles,
                show_deleted_bundles=show_deleted_bundles,
                show_all=show_all,
                show_bundle_path=show_bundle_path,
                bundle_resolver=bundle_resolver,
                _first=False,
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
            "  dcache_ls -lH dcache:/data/\n"
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
    p.add_argument("-a", "--all", dest="all", action="store_true",
                   help=f"Show implementation entries such as the {BUNDLE_STORE_DIR}/ bundle store")
    p.add_argument("--pin", action="store_true",
                   help="Show pin/staging lifetime column")
    p.add_argument("--locality", action="store_true", default=None,
                   help="Show file locality (ONLINE/NEARLINE) column (default with -l)")
    p.add_argument("--no-locality", dest="locality", action="store_false",
                   help="Hide file locality column")
    p.add_argument("--checksum", action="store_true",
                   help="Show Adler-32 checksum column (slow — one API call per file)")
    p.add_argument("--bundles", dest="bundles", action="store_true", default=None,
                   help="Show logical bundle membership grouped by bundle object under each listed directory (default)")
    p.add_argument("--no-bundles", dest="bundles", action="store_false",
                   help="Hide bundle-aware logical members and show only the physical listing")
    p.add_argument("--show-deleted-bundles", action="store_true",
                   help="Reveal logically deleted bundle members in bundle-aware output")
    p.add_argument("--bundle-path", dest="bundle_path", action="store_true",
                   help="In long format, show the full bundle remote path instead of bundle=<n>")
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
    show_bundles = True if args.bundles is None else args.bundles
    show_deleted_bundles = bool(args.show_deleted_bundles)
    bundle_resolver = _BundleLsResolver(rclone_config, api, config[remote], ada_cmd=args.ada) if api else None

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
                show_bundles=show_bundles,
                show_deleted_bundles=show_deleted_bundles,
                show_all=args.all,
                show_bundle_path=args.bundle_path,
                bundle_resolver=bundle_resolver,
            )
        else:
            physical_rows: list[_Row] = []
            groups: list[_BundleGroupView] = []
            try:
                physical_rows = _list_path(
                    args.ada, rclone_config, api, remote_path,
                    human=args.human_readable,
                    show_pin=args.pin,
                    show_checksum=args.checksum,
                    show_all=args.all,
                )
            except RuntimeError:
                if bundle_resolver is None:
                    raise
                resolved = bundle_resolver.logical_member_for_path(
                    remote_path, include_deleted=show_deleted_bundles,
                )
                fallback_groups = [resolved[0]] if resolved is not None else bundle_resolver.bundle_groups_for_directory(
                    remote_path, include_deleted=show_deleted_bundles,
                )
                if not fallback_groups:
                    raise
                if not show_bundles:
                    return 0
                groups = fallback_groups
            else:
                if show_bundles and bundle_resolver is not None:
                    groups = bundle_resolver.bundle_groups_for_directory(
                        remote_path, include_deleted=show_deleted_bundles,
                    )

            rows = _merge_rows(physical_rows, groups, human=args.human_readable)

            if args.long:
                _render_long(
                    rows,
                    show_pin=args.pin,
                    show_locality=show_locality,
                    show_checksum=args.checksum,
                    show_bundle_path=args.bundle_path,
                )
                if groups and not args.bundle_path:
                    print()
                    _render_bundle_legend(groups)
                if rows:
                    print(f"{_C.DIM}{_summary_line(rows)}{_C.RESET}")
            else:
                _render_short(rows)
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
