from __future__ import annotations

import base64
import hashlib
import json
import os
import posixpath
import shutil
import stat
import subprocess
import tempfile
import time
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path

from .tempfiles import mkdtemp as dcache_mkdtemp, mkstemp as dcache_mkstemp


DEFAULT_BUNDLE_FORMAT = "squashfs"
DEFAULT_BUNDLE_TARGET_SIZE = 1024 ** 3
DEFAULT_BUNDLE_MAX_FILE_SIZE = 64 * 1024 ** 2
DEFAULT_BUNDLE_MIN_DIR_TOTAL = 256 * 1024 ** 2
DEFAULT_BUNDLE_MAX_MEMBERS = 10_000
DEFAULT_XATTR_SHARD_BYTES = 256 * 1024
DEFAULT_SQUASHFS_COMPRESSOR = "zstd"
DEFAULT_SQUASHFS_BLOCK_SIZE = 1024 ** 2

MANIFEST_ENTRY = "._dcache_cp_manifest.v1.json"
ANCHOR_SCHEMA = "v1"
BUNDLE_SCHEMA = "v1"
ROUTE_CODEC = "tsv+zlib+base64url"
MEMBERS_CODEC = "tsv+zlib+base64url"
DEPRECATED_CODEC = "tsv+zlib+base64url"
DELETED_CODEC = "tsv+zlib+base64url"
DELETED_MEMBER_PREFIX = "dcache_cp.bundle.deleted.member."


@dataclass(frozen=True)
class BundleOptions:
    format: str = DEFAULT_BUNDLE_FORMAT
    target_size: int = DEFAULT_BUNDLE_TARGET_SIZE
    max_file_size: int = DEFAULT_BUNDLE_MAX_FILE_SIZE
    min_dir_total: int = DEFAULT_BUNDLE_MIN_DIR_TOTAL
    max_members: int = DEFAULT_BUNDLE_MAX_MEMBERS
    keep_temp: bool = False

    def validate(self) -> None:
        if self.format != DEFAULT_BUNDLE_FORMAT:
            raise ValueError(f"bundle format must be {DEFAULT_BUNDLE_FORMAT}")
        if self.target_size < 1:
            raise ValueError("bundle target size must be >= 1")
        if self.max_file_size < 1:
            raise ValueError("bundle max file size must be >= 1")
        if self.min_dir_total < 1:
            raise ValueError("bundle min dir total must be >= 1")
        if self.max_members < 1:
            raise ValueError("bundle max members must be >= 1")


@dataclass(frozen=True)
class BundleMember:
    rel: str
    anchor_rel: str
    source: Path
    resolved_source: Path
    remote_path: str
    size: int
    mtime_ns: int
    mode: int
    adler32: str


@dataclass(frozen=True)
class BundleJob:
    anchor_dir: str
    generation: str
    bundle_id: str
    remote_path: str
    created_at: str
    members: tuple[BundleMember, ...]
    manifest: dict[str, object]
    bundle_object_xattrs: dict[str, str]

    @property
    def logical_file_count(self) -> int:
        return len(self.members)

    @property
    def logical_total_bytes(self) -> int:
        return sum(member.size for member in self.members)


@dataclass(frozen=True)
class AnchorBundlePlan:
    anchor_dir: str
    generation: str
    bundles: tuple[BundleJob, ...]
    route_map: tuple[tuple[str, str], ...] = ()
    reused_members: tuple[BundleMember, ...] = ()
    deprecations: tuple[BundleDeprecationPlan, ...] = ()
    commit_required: bool = True

    @property
    def logical_file_count(self) -> int:
        return sum(bundle.logical_file_count for bundle in self.bundles) + len(self.reused_members)

    @property
    def logical_total_bytes(self) -> int:
        return sum(bundle.logical_total_bytes for bundle in self.bundles) + sum(member.size for member in self.reused_members)

    @property
    def reused_file_count(self) -> int:
        return len(self.reused_members)


@dataclass(frozen=True)
class BundleUploadPlan:
    plain_entries: tuple[dict, ...]
    anchors: tuple[AnchorBundlePlan, ...]

    @property
    def bundle_count(self) -> int:
        return sum(len(anchor.bundles) for anchor in self.anchors)

    @property
    def bundled_file_count(self) -> int:
        return sum(anchor.logical_file_count for anchor in self.anchors)

    @property
    def bundled_total_bytes(self) -> int:
        return sum(anchor.logical_total_bytes for anchor in self.anchors)

    @property
    def reused_file_count(self) -> int:
        return sum(anchor.reused_file_count for anchor in self.anchors)


@dataclass(frozen=True)
class AnchorEntryPlan:
    anchor_dir: str
    entries: tuple[dict, ...]


@dataclass(frozen=True)
class BundleMemberMetadata:
    anchor_rel: str
    adler32: str
    size: int
    mtime_ns: int
    mode: int


@dataclass(frozen=True)
class BundleDeprecationRecord:
    anchor_rel: str
    replacement_bundle_id: str
    replacement_generation: str


@dataclass(frozen=True)
class BundleDeprecationPlan:
    bundle_id: str
    remote_path: str
    members: tuple[BundleDeprecationRecord, ...]


@dataclass(frozen=True)
class BundleDeletedRecord:
    anchor_rel: str
    deleted_generation: str
    deleted_at: str


def _adler32_local(path: Path) -> str:
    checksum = 1
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(8 * 1024 * 1024)
            if not chunk:
                break
            checksum = zlib.adler32(chunk, checksum)
    return f"{checksum & 0xFFFFFFFF:08x}"


def _encode_payload(text: str) -> str:
    compressed = zlib.compress(text.encode("utf-8"), level=9)
    return base64.urlsafe_b64encode(compressed).decode("ascii")


def _decode_payload(value: str) -> str:
    raw = base64.urlsafe_b64decode(value.encode("ascii"))
    return zlib.decompress(raw).decode("utf-8")


def _shard_records(records: list[str], *, max_bytes: int = DEFAULT_XATTR_SHARD_BYTES) -> list[str]:
    if not records:
        return []
    shards: list[str] = []
    current: list[str] = []
    current_bytes = 0
    for record in sorted(records):
        record_bytes = len(record.encode("utf-8")) + 1
        if current and current_bytes + record_bytes > max_bytes:
            shards.append(_encode_payload("\n".join(current)))
            current = []
            current_bytes = 0
        current.append(record)
        current_bytes += record_bytes
    if current:
        shards.append(_encode_payload("\n".join(current)))
    return shards


def _make_generation() -> str:
    return f"{time.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def make_bundle_generation() -> str:
    return _make_generation()


def _anchor_dir(remote_path: str) -> str:
    return posixpath.dirname(str(remote_path).strip("/"))


def _remote_path_within(anchor_dir: str, remote_path: str) -> bool:
    clean_anchor = str(anchor_dir).strip("/")
    clean_path = str(remote_path).strip("/")
    if not clean_anchor:
        return True
    return clean_path == clean_anchor or clean_path.startswith(clean_anchor + "/")


def _entry_bundle_root(entry: dict) -> str | None:
    root = entry.get("bundle_root")
    if root is None:
        return None
    return str(root).strip("/")


def _infer_bundle_root(files: list[dict]) -> str:
    remote_paths = [str(entry["remote_path"]).strip("/") for entry in files]
    if not remote_paths:
        return ""
    if len(remote_paths) == 1:
        return posixpath.dirname(remote_paths[0])
    return posixpath.commonpath(remote_paths)


def _entry_eligible(entry: dict, options: BundleOptions) -> bool:
    source = Path(entry["resolved_source"])
    if not source.is_file():
        return False
    return int(entry.get("size", 0)) <= options.max_file_size


def _member_from_entry(entry: dict, *, anchor_dir: str | None = None) -> BundleMember:
    resolved_source = Path(entry["resolved_source"])
    source = Path(entry.get("source", resolved_source))
    remote_path = str(entry["remote_path"]).strip("/")
    member_anchor_dir = str(anchor_dir).strip("/") if anchor_dir is not None else posixpath.dirname(remote_path)
    if not _remote_path_within(member_anchor_dir, remote_path):
        raise ValueError(f"remote path {remote_path} is not within anchor {member_anchor_dir or '/'}")
    st = resolved_source.stat()
    return BundleMember(
        rel=str(entry["rel"]),
        anchor_rel=posixpath.relpath(remote_path, member_anchor_dir) if member_anchor_dir else remote_path,
        source=source,
        resolved_source=resolved_source,
        remote_path=remote_path,
        size=int(entry.get("size", st.st_size)),
        mtime_ns=int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
        mode=stat.S_IMODE(st.st_mode),
        adler32=_adler32_local(resolved_source),
    )


def build_bundle_member(entry: dict, *, anchor_dir: str | None = None) -> BundleMember:
    return _member_from_entry(entry, anchor_dir=anchor_dir)


def _compute_bundle_id(anchor_dir: str, bundle_format: str, members: list[BundleMember]) -> str:
    hasher = hashlib.blake2b(digest_size=16)
    hasher.update(anchor_dir.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(bundle_format.encode("utf-8"))
    hasher.update(b"\0")
    for member in sorted(members, key=lambda item: item.anchor_rel):
        line = (
            f"{member.anchor_rel}\t{member.size}\t{member.adler32}\t"
            f"{member.mtime_ns}\t{member.mode:04o}\n"
        )
        hasher.update(line.encode("utf-8"))
    return hasher.hexdigest()


def _build_member_records(members: list[BundleMember]) -> list[str]:
    return [
        f"{member.anchor_rel}\t{member.adler32}\t{member.size}\t{member.mtime_ns}\t{member.mode:04o}"
        for member in sorted(members, key=lambda item: item.anchor_rel)
    ]


def _build_manifest(
    *,
    anchor_dir: str,
    generation: str,
    bundle_id: str,
    remote_path: str,
    created_at: str,
    members: list[BundleMember],
    bundle_format: str,
) -> dict[str, object]:
    if bundle_format != DEFAULT_BUNDLE_FORMAT:
        raise ValueError(f"unsupported bundle format for manifest: {bundle_format}")
    bundle_object = (
        posixpath.relpath(remote_path, anchor_dir)
        if anchor_dir
        else remote_path
    )
    manifest = {
        "schema": "dcache_cp.bundle_manifest.v1",
        "bundle_id": bundle_id,
        "anchor_dir": anchor_dir,
        "generation": generation,
        "created_at": created_at,
        "format": bundle_format,
        "bundle_object": bundle_object,
        "bundle_adler32": None,
        "bundle_size": None,
        "members": [
            {
                "path": member.anchor_rel,
                "size": member.size,
                "adler32": member.adler32,
                "mtime_ns": member.mtime_ns,
                "mode": f"{member.mode:04o}",
            }
            for member in sorted(members, key=lambda item: item.anchor_rel)
        ],
    }
    manifest["squashfs"] = {
        "compressor": DEFAULT_SQUASHFS_COMPRESSOR,
        "block_size": DEFAULT_SQUASHFS_BLOCK_SIZE,
        "no_duplicates": True,
    }
    return manifest


def _build_bundle_object_xattrs(
    *,
    anchor_dir: str,
    generation: str,
    bundle_id: str,
    created_at: str,
    members: list[BundleMember],
    bundle_format: str,
) -> dict[str, str]:
    if bundle_format != DEFAULT_BUNDLE_FORMAT:
        raise ValueError(f"unsupported bundle format for xattrs: {bundle_format}")
    member_shards = _shard_records(_build_member_records(members))
    xattrs = {
        "dcache_cp.bundle.schema": BUNDLE_SCHEMA,
        "dcache_cp.bundle.id": bundle_id,
        "dcache_cp.bundle.anchor_dir": anchor_dir,
        "dcache_cp.bundle.format": bundle_format,
        "dcache_cp.bundle.generation": generation,
        "dcache_cp.bundle.created_at": created_at,
        "dcache_cp.bundle.members.codec": MEMBERS_CODEC,
        "dcache_cp.bundle.members.shards": str(len(member_shards)),
        "dcache_cp.bundle.members.count": str(len(members)),
        "dcache_cp.bundle.manifest_entry": MANIFEST_ENTRY,
        "dcache_cp.bundle.squashfs.compressor": DEFAULT_SQUASHFS_COMPRESSOR,
        "dcache_cp.bundle.squashfs.block_size": str(DEFAULT_SQUASHFS_BLOCK_SIZE),
        "dcache_cp.bundle.squashfs.no_duplicates": "true",
    }
    for index, payload in enumerate(member_shards):
        xattrs[f"dcache_cp.bundle.members.{index}"] = payload
    return xattrs


def _build_bundle_job(anchor_dir: str, generation: str, entries: list[dict], bundle_format: str) -> BundleJob:
    members = [_member_from_entry(entry, anchor_dir=anchor_dir) for entry in entries]
    bundle_id = _compute_bundle_id(anchor_dir, bundle_format, members)
    remote_path = bundle_object_remote_path(anchor_dir, bundle_id)
    created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    manifest = _build_manifest(
        anchor_dir=anchor_dir,
        generation=generation,
        bundle_id=bundle_id,
        remote_path=remote_path,
        created_at=created_at,
        members=members,
        bundle_format=bundle_format,
    )
    return BundleJob(
        anchor_dir=anchor_dir,
        generation=generation,
        bundle_id=bundle_id,
        remote_path=remote_path,
        created_at=created_at,
        members=tuple(members),
        manifest=manifest,
        bundle_object_xattrs=_build_bundle_object_xattrs(
            anchor_dir=anchor_dir,
            generation=generation,
            bundle_id=bundle_id,
            created_at=created_at,
            members=members,
            bundle_format=bundle_format,
        ),
    )


def plan_anchor_bundle_jobs(
    anchor_dir: str,
    entries: list[dict],
    options: BundleOptions,
    *,
    generation: str | None = None,
) -> tuple[BundleJob, ...]:
    if not entries:
        return ()

    bundle_generation = generation or _make_generation()
    bundle_entries: list[list[dict]] = []
    current: list[dict] = []
    current_bytes = 0

    for entry in sorted(entries, key=lambda item: str(item["remote_path"])):
        size = int(entry.get("size", 0))
        if current and (current_bytes + size > options.target_size or len(current) >= options.max_members):
            bundle_entries.append(current)
            current = []
            current_bytes = 0
        current.append(entry)
        current_bytes += size

    if current:
        bundle_entries.append(current)

    return tuple(
        _build_bundle_job(anchor_dir, bundle_generation, bundle_group, options.format)
        for bundle_group in bundle_entries
    )


def _plan_bundle_anchor_groups_for_root(
    files: list[dict],
    options: BundleOptions,
    *,
    bundle_root: str,
) -> tuple[tuple[dict, ...], tuple[AnchorEntryPlan, ...]]:
    files_by_dir: dict[str, list[dict]] = {}
    children_by_dir: dict[str, set[str]] = {}

    for entry in sorted(files, key=lambda item: str(item["remote_path"])):
        remote_path = str(entry["remote_path"]).strip("/")
        if not _remote_path_within(bundle_root, remote_path):
            raise ValueError(
                f"remote path {remote_path} escapes bundle root {bundle_root or '/'}"
            )
        parent_dir = posixpath.dirname(remote_path)
        files_by_dir.setdefault(parent_dir, []).append(entry)
        current_dir = parent_dir
        while current_dir != bundle_root:
            parent_of_dir = posixpath.dirname(current_dir)
            children_by_dir.setdefault(parent_of_dir, set()).add(current_dir)
            current_dir = parent_of_dir

    def visit(dir_path: str) -> tuple[list[dict], list[AnchorEntryPlan], list[dict]]:
        plain_entries: list[dict] = []
        anchor_groups: list[AnchorEntryPlan] = []
        promoted_entries: list[dict] = []

        for entry in files_by_dir.get(dir_path, []):
            if _entry_eligible(entry, options):
                promoted_entries.append(entry)
            else:
                plain_entries.append(entry)

        for child_dir in sorted(children_by_dir.get(dir_path, ())):
            child_plain_entries, child_anchor_groups, child_promoted_entries = visit(child_dir)
            plain_entries.extend(child_plain_entries)
            anchor_groups.extend(child_anchor_groups)
            promoted_entries.extend(child_promoted_entries)

        promoted_entries.sort(key=lambda item: str(item["remote_path"]))
        promoted_total = sum(int(entry.get("size", 0)) for entry in promoted_entries)
        should_bundle_here = len(promoted_entries) >= 2 and (
            dir_path == bundle_root or promoted_total >= options.min_dir_total
        )
        if should_bundle_here:
            anchor_groups.append(
                AnchorEntryPlan(anchor_dir=dir_path, entries=tuple(promoted_entries))
            )
            return plain_entries, anchor_groups, []

        return plain_entries, anchor_groups, promoted_entries

    plain_entries, anchor_groups, promoted_entries = visit(bundle_root)
    plain_entries.extend(promoted_entries)
    return tuple(plain_entries), tuple(anchor_groups)


def plan_bundle_anchor_groups(files: list[dict], options: BundleOptions) -> tuple[tuple[dict, ...], tuple[AnchorEntryPlan, ...]]:
    options.validate()
    if not files:
        return (), ()

    explicit_roots = {
        root
        for root in (_entry_bundle_root(entry) for entry in files)
        if root is not None
    }
    grouped_by_root: dict[str, list[dict]] = {}
    if explicit_roots:
        for entry in files:
            root = _entry_bundle_root(entry)
            if root is None:
                raise ValueError("bundle_root metadata must be present on all bundled upload entries")
            grouped_by_root.setdefault(root, []).append(entry)
    else:
        inferred_root = _infer_bundle_root(files)
        grouped_by_root[inferred_root] = list(files)

    plain_entries: list[dict] = []
    anchor_groups: list[AnchorEntryPlan] = []
    for bundle_root, root_entries in sorted(grouped_by_root.items(), key=lambda item: item[0]):
        root_plain_entries, root_anchor_groups = _plan_bundle_anchor_groups_for_root(
            root_entries,
            options,
            bundle_root=bundle_root,
        )
        plain_entries.extend(root_plain_entries)
        anchor_groups.extend(root_anchor_groups)

    return tuple(plain_entries), tuple(anchor_groups)


def plan_bundle_uploads(files: list[dict], options: BundleOptions) -> BundleUploadPlan:
    plain_entries, anchor_groups = plan_bundle_anchor_groups(files, options)
    anchors: list[AnchorBundlePlan] = []
    for anchor_group in anchor_groups:
        generation = _make_generation()
        bundles = plan_anchor_bundle_jobs(anchor_group.anchor_dir, list(anchor_group.entries), options, generation=generation)
        anchors.append(
            AnchorBundlePlan(
                anchor_dir=anchor_group.anchor_dir,
                generation=generation,
                bundles=bundles,
            )
        )

    return BundleUploadPlan(
        plain_entries=plain_entries,
        anchors=tuple(anchors),
    )


def bundle_object_remote_path(anchor_dir: str, bundle_id: str) -> str:
    return posixpath.join(anchor_dir, ".dcpacks", "bundles", f"{bundle_id}.dcpbundle")


def build_anchor_xattrs_for_routes(route_map: dict[str, str], generation: str) -> dict[str, str]:
    route_records = [
        f"{anchor_rel}\t{bundle_id}"
        for anchor_rel, bundle_id in sorted(route_map.items())
    ]
    shards = _shard_records(route_records)
    return {
        "dcache_cp.bundle_anchor.schema": ANCHOR_SCHEMA,
        "dcache_cp.bundle_anchor.active_generation": generation,
        "dcache_cp.bundle_anchor.route.codec": ROUTE_CODEC,
        f"dcache_cp.bundle_anchor.route.{generation}.shards": str(len(shards)),
        **{
            f"dcache_cp.bundle_anchor.route.{generation}.{index}": payload
            for index, payload in enumerate(shards)
        },
    }


def build_anchor_xattrs(anchor_plan: AnchorBundlePlan) -> dict[str, str]:
    route_map = dict(anchor_plan.route_map)
    if not route_map:
        route_map = {
            member.anchor_rel: bundle.bundle_id
            for bundle in anchor_plan.bundles
            for member in bundle.members
        }
    return build_anchor_xattrs_for_routes(route_map, anchor_plan.generation)


def decode_anchor_routes(xattrs: dict[str, str]) -> dict[str, str]:
    generation = xattrs.get("dcache_cp.bundle_anchor.active_generation")
    if not generation:
        return {}
    codec = xattrs.get("dcache_cp.bundle_anchor.route.codec")
    if codec != ROUTE_CODEC:
        return {}
    count_text = xattrs.get(f"dcache_cp.bundle_anchor.route.{generation}.shards")
    if not count_text:
        return {}
    count = int(count_text)
    route_map: dict[str, str] = {}
    for index in range(count):
        payload = xattrs.get(f"dcache_cp.bundle_anchor.route.{generation}.{index}")
        if not payload:
            continue
        text = _decode_payload(payload)
        for line in text.splitlines():
            if not line.strip():
                continue
            rel, bundle_id = line.split("\t", 1)
            route_map[rel] = bundle_id
    return route_map


def decode_bundle_members(xattrs: dict[str, str]) -> dict[str, BundleMemberMetadata]:
    codec = xattrs.get("dcache_cp.bundle.members.codec")
    if codec != MEMBERS_CODEC:
        return {}
    count_text = xattrs.get("dcache_cp.bundle.members.shards")
    if not count_text:
        return {}
    count = int(count_text)
    members: dict[str, BundleMemberMetadata] = {}
    for index in range(count):
        payload = xattrs.get(f"dcache_cp.bundle.members.{index}")
        if not payload:
            continue
        text = _decode_payload(payload)
        for line in text.splitlines():
            if not line.strip():
                continue
            anchor_rel, adler32, size_text, mtime_ns_text, mode_text = line.split("\t", 4)
            members[anchor_rel] = BundleMemberMetadata(
                anchor_rel=anchor_rel,
                adler32=adler32,
                size=int(size_text),
                mtime_ns=int(mtime_ns_text),
                mode=int(mode_text, 8),
            )
    return members


def build_bundle_deprecated_xattrs(
    records: dict[str, BundleDeprecationRecord] | list[BundleDeprecationRecord] | tuple[BundleDeprecationRecord, ...],
) -> dict[str, str]:
    if isinstance(records, dict):
        values = list(records.values())
    else:
        values = list(records)
    payload_records = [
        f"{record.anchor_rel}\t{record.replacement_bundle_id}\t{record.replacement_generation}"
        for record in sorted(values, key=lambda item: item.anchor_rel)
    ]
    shards = _shard_records(payload_records)
    xattrs = {
        "dcache_cp.bundle.deprecated.codec": DEPRECATED_CODEC,
        "dcache_cp.bundle.deprecated.count": str(len(values)),
        "dcache_cp.bundle.deprecated.shards": str(len(shards)),
    }
    for index, payload in enumerate(shards):
        xattrs[f"dcache_cp.bundle.deprecated.{index}"] = payload
    return xattrs


def decode_bundle_deprecated(xattrs: dict[str, str]) -> dict[str, BundleDeprecationRecord]:
    codec = xattrs.get("dcache_cp.bundle.deprecated.codec")
    if codec != DEPRECATED_CODEC:
        return {}
    count_text = xattrs.get("dcache_cp.bundle.deprecated.shards")
    if not count_text:
        return {}
    count = int(count_text)
    deprecated: dict[str, BundleDeprecationRecord] = {}
    for index in range(count):
        payload = xattrs.get(f"dcache_cp.bundle.deprecated.{index}")
        if not payload:
            continue
        text = _decode_payload(payload)
        for line in text.splitlines():
            if not line.strip():
                continue
            anchor_rel, replacement_bundle_id, replacement_generation = line.split("\t", 2)
            deprecated[anchor_rel] = BundleDeprecationRecord(
                anchor_rel=anchor_rel,
                replacement_bundle_id=replacement_bundle_id,
                replacement_generation=replacement_generation,
            )
    return deprecated


def build_bundle_deleted_xattrs(
    records: dict[str, BundleDeletedRecord] | list[BundleDeletedRecord] | tuple[BundleDeletedRecord, ...],
) -> dict[str, str]:
    if isinstance(records, dict):
        values = list(records.values())
    else:
        values = list(records)
    xattrs = {
        "dcache_cp.bundle.deleted.codec": DELETED_CODEC,
        "dcache_cp.bundle.deleted.count": str(len(values)),
    }
    for record in sorted(values, key=lambda item: item.anchor_rel):
        encoded_anchor_rel = base64.urlsafe_b64encode(record.anchor_rel.encode("utf-8")).decode("ascii").rstrip("=")
        xattrs[f"{DELETED_MEMBER_PREFIX}{encoded_anchor_rel}"] = (
            f"{record.deleted_generation}\t{record.deleted_at}"
        )
    return xattrs


def decode_bundle_deleted(xattrs: dict[str, str]) -> dict[str, BundleDeletedRecord]:
    deleted: dict[str, BundleDeletedRecord] = {}
    for key, payload in xattrs.items():
        if not key.startswith(DELETED_MEMBER_PREFIX):
            continue
        encoded_anchor_rel = key[len(DELETED_MEMBER_PREFIX):]
        if not encoded_anchor_rel:
            continue
        padding = "=" * (-len(encoded_anchor_rel) % 4)
        anchor_rel = base64.urlsafe_b64decode((encoded_anchor_rel + padding).encode("ascii")).decode("utf-8")
        deleted_generation, deleted_at = payload.split("\t", 1)
        deleted[anchor_rel] = BundleDeletedRecord(
            anchor_rel=anchor_rel,
            deleted_generation=deleted_generation,
            deleted_at=deleted_at,
        )
    if deleted:
        return deleted

    codec = xattrs.get("dcache_cp.bundle.deleted.codec")
    if codec != DELETED_CODEC:
        return {}
    count_text = xattrs.get("dcache_cp.bundle.deleted.shards")
    if not count_text:
        return {}
    count = int(count_text)
    for index in range(count):
        payload = xattrs.get(f"dcache_cp.bundle.deleted.{index}")
        if not payload:
            continue
        text = _decode_payload(payload)
        for line in text.splitlines():
            if not line.strip():
                continue
            anchor_rel, deleted_generation, deleted_at = line.split("\t", 2)
            deleted[anchor_rel] = BundleDeletedRecord(
                anchor_rel=anchor_rel,
                deleted_generation=deleted_generation,
                deleted_at=deleted_at,
            )
    return deleted


def materialize_bundle_job(job: BundleJob, *, keep_temp: bool = False) -> dict:
    if not job.members:
        raise ValueError("cannot materialize an empty bundle")

    fd, temp_name = dcache_mkstemp(prefix=f"dcache-bundle-{job.bundle_id[:12]}-", suffix=".dcpbundle")
    os.close(fd)
    temp_path = Path(temp_name)
    temp_path.unlink(missing_ok=True)

    manifest_bytes = json.dumps(job.manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    bundle_format = str(job.bundle_object_xattrs.get("dcache_cp.bundle.format", DEFAULT_BUNDLE_FORMAT))
    try:
        if bundle_format != DEFAULT_BUNDLE_FORMAT:
            raise RuntimeError(f"unsupported bundle format for materialization: {bundle_format}")

        mksquashfs = shutil.which("mksquashfs")
        if not mksquashfs:
            raise RuntimeError("bundle format squashfs requires mksquashfs to be installed")

        staging_root = Path(dcache_mkdtemp(prefix=f"dcache-bundle-root-{job.bundle_id[:12]}-"))
        try:
            manifest_path = staging_root / MANIFEST_ENTRY
            manifest_path.write_bytes(manifest_bytes)
            os.chmod(manifest_path, 0o644)

            for member in sorted(job.members, key=lambda item: item.anchor_rel):
                staged_member_path = staging_root / member.anchor_rel
                staged_member_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    os.link(member.resolved_source, staged_member_path)
                except OSError:
                    shutil.copy2(member.resolved_source, staged_member_path)

            result = subprocess.run(
                [
                    mksquashfs,
                    str(staging_root),
                    str(temp_path),
                    "-comp",
                    DEFAULT_SQUASHFS_COMPRESSOR,
                    "-b",
                    str(DEFAULT_SQUASHFS_BLOCK_SIZE),
                    "-no-duplicates",
                    "-noappend",
                    "-quiet",
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                detail = (result.stderr.strip() or result.stdout.strip() or "mksquashfs failed")
                raise RuntimeError(f"mksquashfs failed for bundle {job.bundle_id}: {detail}")
            if not temp_path.exists():
                raise RuntimeError(f"mksquashfs did not produce bundle archive {temp_path}")
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    return {
        "source": temp_path,
        "resolved_source": temp_path,
        "rel": f"bundle:{job.anchor_dir or '/'}:{job.bundle_id[:12]}",
        "size": temp_path.stat().st_size,
        "remote_path": job.remote_path,
        "bundle_job": job,
        "bundle_members": [
            {"rel": member.rel, "size": member.size, "source": member.source}
            for member in job.members
        ],
        "bundle_keep_temp": keep_temp,
    }
