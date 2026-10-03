#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import posixpath
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dcache_cp.bundles import bundle_object_remote_path, decode_anchor_routes, decode_bundle_deleted, decode_bundle_members
from dcache_cp.cli import _rclone_lsjson, load_rclone_config, parse_remote_prefix, resolve_api_url, resolve_config_for_prefix, resolve_remote_name, run_command
from dcache_cp.tempfiles import mkdtemp as dcache_mkdtemp
from dcache_cp.xattrs import NamespaceXattrClient, NamespaceXattrError, extract_bearer_token


LOG = logging.getLogger("dcache_bundle_live_suite")

BUNDLE_MIN_DIR_TOTAL = "12KiB"
BUNDLE_MAX_FILE_SIZE = "8KiB"
BUNDLE_TARGET_SIZE = "24KiB"
BUNDLE_MAX_MEMBERS = "4"


class SuiteError(RuntimeError):
    pass


@dataclass
class StepOutput:
    name: str
    rc: int
    log_path: Path
    output: str


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _slugify(value: str) -> str:
    out = []
    for char in value.lower():
        if char.isalnum():
            out.append(char)
        elif out and out[-1] != "-":
            out.append("-")
    return "".join(out).strip("-") or "step"


def _sha256_local(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _file_map(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(root)).replace(os.sep, "/")
        out[rel] = _sha256_local(path)
    return out


def _make_bytes(label: str, size: int) -> bytes:
    chunks: list[bytes] = []
    counter = 0
    remaining = size
    while remaining > 0:
        digest = hashlib.sha256(f"{label}:{counter}".encode("utf-8")).digest()
        chunk = digest[:remaining]
        chunks.append(chunk)
        remaining -= len(chunk)
        counter += 1
    return b"".join(chunks)


def _write_file(path: Path, label: str, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_make_bytes(label, size))


def _contains(text: str, needle: str) -> bool:
    return needle in text


def _delete_case_move_rerun_resumed_cleanly(output: str) -> bool:
    return any(
        needle in output
        for needle in (
            "all planned download files are already verified locally",
            "already verified",
            "already moved",
            "no files to process",
        )
    )


def _rel_remote(base: str, *parts: str) -> str:
    clean_parts = [base.strip("/")]
    clean_parts.extend(str(part).strip("/") for part in parts if str(part).strip("/"))
    return posixpath.join(*clean_parts)


def _summary_step(summary: dict[str, object], name: str, payload: dict[str, object]) -> None:
    steps = summary.setdefault("steps", {})
    if not isinstance(steps, dict):
        raise SuiteError("summary steps container is corrupted")
    steps[name] = payload


class LiveSuite:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_id = time.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]

        prefix, target_path = parse_remote_prefix(args.target)
        if not prefix:
            raise SuiteError("target must include a remote prefix such as dcache:")
        self.prefix = prefix
        self.target_base = target_path.strip("/")
        if not self.target_base:
            raise SuiteError("target path must not be empty")

        self.remote_run_root = _rel_remote(self.target_base, f"bundle-live-suite-{self.run_id}")
        self.remote_run_root_prefixed = f"{self.prefix}:/{self.remote_run_root}"

        self.config_path = resolve_config_for_prefix(self.prefix, args.config)
        self.config = load_rclone_config(self.config_path)
        self.remote_name = resolve_remote_name(self.config, args.remote)
        self.api = resolve_api_url(args.api, self.config[self.remote_name])
        self.token = extract_bearer_token(self.config[self.remote_name], self.config_path)
        if not self.api:
            raise SuiteError("could not resolve dCache API URL; pass --api or configure it in the selected remote")
        if not self.token:
            raise SuiteError("could not resolve bearer token from the selected rclone config")
        self.xattrs = NamespaceXattrClient(self.api, self.token)

        self.workdir = args.workdir or Path(dcache_mkdtemp(prefix="dcache-bundle-live-suite-"))
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.log_dir = self.workdir / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.env = os.environ.copy()
        existing_pythonpath = self.env.get("PYTHONPATH", "")
        pythonpath_parts = [str(SRC)]
        if existing_pythonpath:
            pythonpath_parts.append(existing_pythonpath)
        self.env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        self.env["NO_COLOR"] = "1"
        self.env["PYTHONUNBUFFERED"] = "1"
        self.env.setdefault("LC_ALL", "C")

        self.step_index = 0
        self.summary: dict[str, object] = {
            "status": "running",
            "run_id": self.run_id,
            "target": args.target,
            "remote_run_root": self.remote_run_root,
            "workdir": str(self.workdir),
            "log_dir": str(self.log_dir),
            "config_path": str(self.config_path),
            "remote_name": self.remote_name,
            "api": self.api,
            "download_mode": args.download_mode,
            "workers": args.workers,
            "bundle_options": {
                "bundle_min_dir_total": BUNDLE_MIN_DIR_TOTAL,
                "bundle_max_file_size": BUNDLE_MAX_FILE_SIZE,
                "bundle_target_size": BUNDLE_TARGET_SIZE,
                "bundle_max_members": BUNDLE_MAX_MEMBERS,
            },
            "steps": {},
        }

        self.dataset_root = self.workdir / "datasets"
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        self.download_root = self.workdir / "downloads"
        self.download_root.mkdir(parents=True, exist_ok=True)
        self.reports_dir = self.workdir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.case_paths: dict[str, str] = {}
        self.expected_maps: dict[str, dict[str, str]] = {}

    def _log_step_header(self, name: str, command: list[str], log_path: Path) -> None:
        LOG.info("[%02d] %s", self.step_index, name)
        LOG.info("       command : %s", shlex.join(command))
        LOG.info("       log     : %s", log_path)

    def run_process(
        self,
        name: str,
        command: list[str],
        *,
        expected_rc: int | tuple[int, ...] = 0,
        extra_env: dict[str, str] | None = None,
    ) -> StepOutput:
        self.step_index += 1
        slug = _slugify(name)
        log_path = self.log_dir / f"{self.step_index:02d}-{slug}.log"
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)

        self._log_step_header(name, command, log_path)
        lines: list[str] = []
        with log_path.open("w", encoding="utf-8") as handle:
            handle.write(f"$ {shlex.join(command)}\n\n")
            process = subprocess.Popen(
                command,
                cwd=str(ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert process.stdout is not None
            for line in process.stdout:
                sys.stdout.write(line)
                handle.write(line)
                lines.append(line)
            rc = process.wait()
            handle.write(f"\n[exit-code] {rc}\n")

        allowed = expected_rc if isinstance(expected_rc, tuple) else (expected_rc,)
        output = "".join(lines)
        payload = {
            "rc": rc,
            "log_path": str(log_path),
        }
        _summary_step(self.summary, name, payload)
        if rc not in allowed:
            raise SuiteError(f"step {name!r} failed with exit code {rc}; see {log_path}")
        return StepOutput(name=name, rc=rc, log_path=log_path, output=output)

    def run_module(self, name: str, module: str, args: list[str], *, expected_rc: int | tuple[int, ...] = 0) -> StepOutput:
        command = [sys.executable, "-m", module, *args]
        return self.run_process(name, command, expected_rc=expected_rc)

    def cp(self, name: str, args: list[str], *, expected_rc: int | tuple[int, ...] = 0) -> StepOutput:
        return self.run_module(name, "dcache_cp.cli", args, expected_rc=expected_rc)

    def mv(self, name: str, args: list[str], *, expected_rc: int | tuple[int, ...] = 0) -> StepOutput:
        return self.run_module(name, "dcache_cp.mv", args, expected_rc=expected_rc)

    def ls(self, name: str, args: list[str], *, expected_rc: int | tuple[int, ...] = 0) -> StepOutput:
        return self.run_module(name, "dcache_cp.ls", args, expected_rc=expected_rc)

    def connection_args(self, *, include_verbose: bool = True, include_workers: bool = False) -> list[str]:
        args = [
            "--config",
            str(self.config_path),
            "--remote",
            self.remote_name,
            "--api",
            self.api,
        ]
        if include_workers:
            args.extend([
                "--workers",
                str(self.args.workers),
            ])
        if self.args.ada:
            args.extend(["--ada", self.args.ada])
        if include_verbose:
            args.append("--verbose")
        return args

    def bundle_upload_args(self) -> list[str]:
        return [
            "--bundle-small-files",
            "--bundle-min-dir-total",
            BUNDLE_MIN_DIR_TOTAL,
            "--bundle-max-file-size",
            BUNDLE_MAX_FILE_SIZE,
            "--bundle-target-size",
            BUNDLE_TARGET_SIZE,
            "--bundle-max-members",
            BUNDLE_MAX_MEMBERS,
        ]

    def require(self, condition: bool, message: str) -> None:
        if not condition:
            raise SuiteError(message)

    def require_contains(self, text: str, needle: str, *, context: str) -> None:
        self.require(_contains(text, needle), f"{context}: expected to find {needle!r}")

    def require_not_contains(self, text: str, needle: str, *, context: str) -> None:
        self.require(not _contains(text, needle), f"{context}: unexpected {needle!r} in output")

    def lsjson_paths(self, remote_path: str) -> list[str]:
        entries = _rclone_lsjson(self.config_path, self.remote_name, remote_path, recursive=True, missing_ok=True)
        out: list[str] = []
        for entry in entries:
            if entry.get("IsDir", False):
                continue
            rel = str(entry.get("Path", ""))
            if rel:
                out.append(rel)
        return sorted(out)

    def list_xattrs(self, remote_path: str) -> dict[str, str]:
        return self.xattrs.list_xattrs(remote_path)

    def anchor_routes(self, anchor_dir: str) -> dict[str, str]:
        return decode_anchor_routes(self.list_xattrs(anchor_dir))

    def bundle_members(self, bundle_remote_path: str) -> dict[str, object]:
        return decode_bundle_members(self.list_xattrs(bundle_remote_path))

    def bundle_deleted(self, bundle_remote_path: str) -> dict[str, object]:
        return decode_bundle_deleted(self.list_xattrs(bundle_remote_path))

    def remote_bundle_path_for_member(self, anchor_dir: str, anchor_rel: str) -> str:
        routes = self.anchor_routes(anchor_dir)
        self.require(anchor_rel in routes, f"route {anchor_rel!r} missing from anchor {anchor_dir}")
        return bundle_object_remote_path(anchor_dir, str(routes[anchor_rel]))

    def write_report(self) -> Path:
        report_path = self.args.report or (self.reports_dir / "summary.json")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(self.summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return report_path

    def cleanup(self) -> None:
        if self.args.cleanup_remote and self.summary.get("status") == "ok":
            try:
                run_command([
                    "rclone",
                    "--config",
                    str(self.config_path),
                    "purge",
                    f"{self.remote_name}:{self.remote_run_root}",
                ], check=False)
            except Exception as exc:
                LOG.warning("remote cleanup failed for %s: %s", self.remote_run_root_prefixed, exc)
        if self.args.cleanup_local and self.summary.get("status") == "ok":
            shutil.rmtree(self.workdir, ignore_errors=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dcache_bundle_live_suite.py",
        description=(
            "Run a comprehensive live dCache bundling test suite against a remote subfolder. "
            "The suite exercises bundled upload, transparent download, ls overlays, reruns, "
            "bundle-backed moves, parent-anchor reuse, and bundle cleanup."
        ),
    )
    parser.add_argument(
        "target",
        help="Remote base directory with prefix, e.g. dcache:/pnfs/.../my-test-area/",
    )
    parser.add_argument("--config", type=Path, help="rclone config / macaroon file override")
    parser.add_argument("--remote", help="rclone remote name override")
    parser.add_argument("--api", help="dCache API URL override")
    parser.add_argument("--ada", help="ada executable override")
    parser.add_argument(
        "--download-mode",
        choices=["no-stage", "stage", "both"],
        default="both",
        help="Which download paths to exercise for the recursive copy case (default: both)",
    )
    parser.add_argument("--workers", type=int, default=2, help="Worker count for dcache_cp/dcache_mv commands (default: 2)")
    parser.add_argument("--workdir", type=Path, help="Use a specific local working directory instead of a new temp directory")
    parser.add_argument("--report", type=Path, help="Write the final JSON summary here")
    parser.add_argument("--cleanup-local", action="store_true", help="Remove the local workdir on success")
    parser.add_argument("--cleanup-remote", action="store_true", help="Purge the remote run directory on success")
    parser.add_argument("--verbose", action="store_true", help="Enable suite-level debug logging")
    return parser


def _detect_sparse_tooling() -> dict[str, object]:
    sqfscat = shutil.which("sqfscat")
    unsquashfs = shutil.which("unsquashfs")
    unmount_helpers = {
        "fusermount3": shutil.which("fusermount3"),
        "fusermount": shutil.which("fusermount"),
        "umount": shutil.which("umount"),
    }
    available = bool(unsquashfs and any(unmount_helpers.values()))
    missing: list[str] = []
    if not unsquashfs:
        missing.append("unsquashfs")
    if not any(unmount_helpers.values()):
        missing.append("fusermount3|fusermount|umount")
    return {
        "available": available,
        "unsquashfs": unsquashfs or "",
        "sqfscat": sqfscat or "",
        "fusermount3": unmount_helpers["fusermount3"] or "",
        "fusermount": unmount_helpers["fusermount"] or "",
        "umount": unmount_helpers["umount"] or "",
        "missing": missing,
    }


def _record_versions(suite: LiveSuite) -> None:
    commands = {
        "python": [sys.executable, "--version"],
        "rclone": ["rclone", "version"],
        "mksquashfs": ["mksquashfs", "-version"],
        "unsquashfs": ["unsquashfs", "-version"],
    }
    sparse_tools = _detect_sparse_tooling()
    for name in ("rclone", "mksquashfs", "unsquashfs"):
        suite.require(shutil.which(name) is not None, f"required command not found on PATH: {name}")

    versions: dict[str, str] = {}
    for name, command in commands.items():
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
        versions[name] = (result.stdout or "").strip()
    versions["sparse_backend"] = "sqfscat" if sparse_tools["sqfscat"] else "unsquashfs"
    versions["sqfscat"] = str(sparse_tools["sqfscat"])
    versions["fusermount3"] = str(sparse_tools["fusermount3"])
    versions["fusermount"] = str(sparse_tools["fusermount"])
    versions["umount"] = str(sparse_tools["umount"])
    suite.summary["versions"] = versions
    suite.summary["sparse_read"] = {
        "available": bool(sparse_tools["available"]),
        "missing": list(sparse_tools["missing"]),
    }


def _write_copy_case(case_root: Path) -> dict[str, str]:
    _write_file(case_root / "local-anchor" / "small-a.bin", "copy-local-anchor-small-a", 4096)
    _write_file(case_root / "local-anchor" / "small-b.bin", "copy-local-anchor-small-b", 4096)
    _write_file(case_root / "local-anchor" / "small-c.bin", "copy-local-anchor-small-c", 4096)
    _write_file(case_root / "local-anchor" / "plain-large.bin", "copy-local-anchor-plain-large", 20000)

    _write_file(case_root / "promoted" / "child1" / "p1.bin", "copy-promoted-child1-p1", 3072)
    _write_file(case_root / "promoted" / "child1" / "p2.bin", "copy-promoted-child1-p2", 3072)
    _write_file(case_root / "promoted" / "child2" / "p3.bin", "copy-promoted-child2-p3", 3072)
    _write_file(case_root / "promoted" / "child2" / "p4.bin", "copy-promoted-child2-p4", 3072)
    return _file_map(case_root)


def _write_parent_anchor_initial(case_root: Path) -> dict[str, str]:
    _write_file(case_root / "promoted" / "child1" / "p1.bin", "parent-initial-child1-p1", 3072)
    _write_file(case_root / "promoted" / "child1" / "p2.bin", "parent-initial-child1-p2", 3072)
    _write_file(case_root / "promoted" / "child2" / "p3.bin", "parent-initial-child2-p3", 3072)
    _write_file(case_root / "promoted" / "child2" / "p4.bin", "parent-initial-child2-p4", 3072)
    return _file_map(case_root)


def _write_parent_anchor_rerun(case_root: Path, source_root: Path) -> dict[str, str]:
    case_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_root / "p1.bin", case_root / "p1.bin")
    shutil.copy2(source_root / "p2.bin", case_root / "p2.bin")
    _write_file(case_root / "extra-1.bin", "parent-rerun-extra-1", 3072)
    _write_file(case_root / "extra-2.bin", "parent-rerun-extra-2", 3072)
    return _file_map(case_root)


def _write_delete_case(case_root: Path) -> dict[str, str]:
    _write_file(case_root / "bundle" / "del1.bin", "delete-case-del1", 4096)
    _write_file(case_root / "bundle" / "del2.bin", "delete-case-del2", 4096)
    _write_file(case_root / "bundle" / "del3.bin", "delete-case-del3", 4096)
    return _file_map(case_root)


def _write_upload_move_case(case_root: Path) -> dict[str, str]:
    _write_file(case_root / "um1.bin", "upload-move-um1", 4096)
    _write_file(case_root / "um2.bin", "upload-move-um2", 4096)
    _write_file(case_root / "um3.bin", "upload-move-um3", 4096)
    return _file_map(case_root)


def _prepare_datasets(suite: LiveSuite) -> None:
    copy_case = suite.dataset_root / "copy-case"
    parent_initial = suite.dataset_root / "parent-anchor-case"
    parent_rerun = suite.dataset_root / "child1"
    delete_case = suite.dataset_root / "delete-case"
    upload_move_case = suite.dataset_root / "upload-move-case"

    suite.expected_maps["copy-case"] = _write_copy_case(copy_case)
    suite.expected_maps["parent-anchor-case-initial"] = _write_parent_anchor_initial(parent_initial)
    suite.expected_maps["parent-anchor-case-child1-rerun"] = _write_parent_anchor_rerun(
        parent_rerun,
        parent_initial / "promoted" / "child1",
    )
    suite.expected_maps["delete-case"] = _write_delete_case(delete_case)
    suite.expected_maps["upload-move-case"] = _write_upload_move_case(upload_move_case)

    suite.case_paths = {
        "copy-case-local": str(copy_case),
        "copy-case-remote": _rel_remote(suite.remote_run_root, "copy-case"),
        "parent-anchor-local": str(parent_initial),
        "parent-anchor-remote": _rel_remote(suite.remote_run_root, "parent-anchor-case"),
        "parent-anchor-child1-local": str(parent_rerun),
        "delete-case-local": str(delete_case),
        "delete-case-remote": _rel_remote(suite.remote_run_root, "delete-case"),
        "upload-move-local": str(upload_move_case),
        "upload-move-remote": _rel_remote(suite.remote_run_root, "upload-move-case"),
    }
    suite.summary["datasets"] = dict(suite.case_paths)


def _verify_copy_case_upload(suite: LiveSuite) -> None:
    remote_copy_case = str(suite.case_paths["copy-case-remote"])
    paths = suite.lsjson_paths(remote_copy_case)
    suite.require("local-anchor/plain-large.bin" in paths, "plain large file should remain a physical remote object")
    suite.require("local-anchor/small-a.bin" not in paths, "bundled local-anchor member should not exist as a physical remote file")
    suite.require("promoted/child1/p1.bin" not in paths, "bundled promoted member should not exist as a physical remote file")
    bundle_paths = [path for path in paths if path.endswith(".dcpbundle")]
    suite.require(len(bundle_paths) >= 2, "expected at least two physical .dcpbundle objects in copy-case")

    local_anchor_dir = _rel_remote(remote_copy_case, "local-anchor")
    local_anchor_routes = suite.anchor_routes(local_anchor_dir)
    suite.require(set(local_anchor_routes) == {"small-a.bin", "small-b.bin", "small-c.bin"}, "local-anchor route set does not match expected bundled members")

    promoted_anchor_dir = _rel_remote(remote_copy_case, "promoted")
    promoted_routes = suite.anchor_routes(promoted_anchor_dir)
    expected_promoted = {
        "child1/p1.bin",
        "child1/p2.bin",
        "child2/p3.bin",
        "child2/p4.bin",
    }
    suite.require(expected_promoted.issubset(set(promoted_routes)), "promoted anchor is missing expected routed bundle members")

    _summary_step(
        suite.summary,
        "verify-copy-case-upload-state",
        {
            "physical_paths": paths,
            "local_anchor_routes": local_anchor_routes,
            "promoted_routes": promoted_routes,
        },
    )


def _download_compare(suite: LiveSuite, label: str, output_root: Path, expected_map: dict[str, str]) -> None:
    actual = _file_map(output_root)
    suite.require(actual == expected_map, f"{label}: downloaded file tree does not match expected content")
    suite.require(not any(path.name == ".dcpacks" for path in output_root.rglob(".dcpacks")), f"{label}: local transparent download unexpectedly materialized .dcpacks")
    _summary_step(
        suite.summary,
        f"verify-{label}",
        {
            "output_root": str(output_root),
            "file_count": len(actual),
            "files": actual,
        },
    )


def _write_file_list(path: Path, rows: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for src, dst in rows:
            handle.write(f"{src}\t{dst}\n")


def _run_suite(suite: LiveSuite) -> None:
    _record_versions(suite)
    _prepare_datasets(suite)
    sparse_read_available = bool((suite.summary.get("sparse_read") or {}).get("available"))

    copy_local = Path(str(suite.case_paths["copy-case-local"]))
    copy_remote = str(suite.case_paths["copy-case-remote"])
    copy_remote_prefixed = f"{suite.prefix}:/{copy_remote}"

    parent_local = Path(str(suite.case_paths["parent-anchor-local"]))
    parent_remote = str(suite.case_paths["parent-anchor-remote"])
    parent_remote_prefixed = f"{suite.prefix}:/{parent_remote}"
    parent_child1_local = Path(str(suite.case_paths["parent-anchor-child1-local"]))

    delete_local = Path(str(suite.case_paths["delete-case-local"]))
    delete_remote = str(suite.case_paths["delete-case-remote"])
    delete_remote_prefixed = f"{suite.prefix}:/{delete_remote}"

    upload_move_local = Path(str(suite.case_paths["upload-move-local"]))
    upload_move_remote = str(suite.case_paths["upload-move-remote"])

    copy_dry_run = suite.cp(
        "dry-run-upload-copy-case",
        [
            *suite.connection_args(include_workers=True),
            "--dry-run",
            *suite.bundle_upload_args(),
            "-R",
            str(copy_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )
    suite.require_contains(copy_dry_run.output, "bundles : 2 new bundle object(s)", context="copy-case initial dry-run")
    suite.require_not_contains(copy_dry_run.output, "resume  :", context="copy-case initial dry-run")

    suite.cp(
        "upload-copy-case",
        [
            *suite.connection_args(include_workers=True),
            *suite.bundle_upload_args(),
            "-R",
            str(copy_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )
    _verify_copy_case_upload(suite)

    ls_local_anchor = suite.ls(
        "ls-copy-case-local-anchor",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'local-anchor')}",
        ],
    )
    suite.require_contains(ls_local_anchor.output, "bundles:", context="dcache_ls local-anchor")
    suite.require_contains(ls_local_anchor.output, "small-a.bin", context="dcache_ls local-anchor")
    suite.require_contains(ls_local_anchor.output, "plain-large.bin", context="dcache_ls local-anchor")

    ls_promoted_child = suite.ls(
        "ls-copy-case-promoted-child1",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child1')}",
        ],
    )
    suite.require_contains(ls_promoted_child.output, "bundles:", context="dcache_ls promoted child1")
    suite.require_contains(ls_promoted_child.output, "../.dcpacks/bundles/", context="dcache_ls promoted child1 inherited anchor")
    suite.require_contains(ls_promoted_child.output, "p1.bin", context="dcache_ls promoted child1")

    ls_no_bundles = suite.ls(
        "ls-copy-case-promoted-child1-no-bundles",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            "--no-bundles",
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child1')}",
        ],
    )
    suite.require_not_contains(ls_no_bundles.output, "bundles:", context="dcache_ls --no-bundles child1")
    suite.require_not_contains(ls_no_bundles.output, "p1.bin", context="dcache_ls --no-bundles child1")

    direct_local_anchor = suite.ls(
        "ls-copy-case-direct-local-bundled-member",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'local-anchor', 'small-a.bin')}",
        ],
    )
    suite.require_contains(direct_local_anchor.output, "small-a.bin", context="dcache_ls direct local bundled member")

    direct_promoted = suite.ls(
        "ls-copy-case-direct-parent-bundled-member",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child1', 'p1.bin')}",
        ],
    )
    suite.require_contains(direct_promoted.output, "p1.bin", context="dcache_ls direct parent bundled member")

    copy_rerun_dry = suite.cp(
        "dry-run-rerun-copy-case",
        [
            *suite.connection_args(include_workers=True),
            "--dry-run",
            *suite.bundle_upload_args(),
            "-R",
            str(copy_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )
    suite.require_contains(copy_rerun_dry.output, "bundled logical file(s) already present remotely and will be reused", context="copy-case rerun dry-run")

    copy_rerun = suite.cp(
        "rerun-copy-case",
        [
            *suite.connection_args(include_workers=True),
            *suite.bundle_upload_args(),
            "-R",
            str(copy_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )
    suite.require(
        _contains(copy_rerun.output, "reused bundle") or _contains(copy_rerun.output, "bundle-backed file(s)"),
        "copy-case actual rerun: expected reuse log output",
    )

    suite.cp(
        "upload-parent-anchor-case",
        [
            *suite.connection_args(include_workers=True),
            *suite.bundle_upload_args(),
            "-R",
            str(parent_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )

    parent_child_rerun = suite.cp(
        "rerun-parent-anchor-child1",
        [
            *suite.connection_args(include_workers=True),
            *suite.bundle_upload_args(),
            "-R",
            str(parent_child1_local),
            f"{suite.prefix}:/{_rel_remote(parent_remote, 'promoted')}/",
        ],
    )
    suite.require_contains(parent_child_rerun.output, "reused", context="parent-anchor child1 rerun")

    parent_anchor_dir = _rel_remote(parent_remote, "promoted")
    parent_routes = suite.anchor_routes(parent_anchor_dir)
    suite.require("child1/extra-1.bin" in parent_routes, "rerun child1 extra-1.bin should route via parent anchor")
    suite.require("child1/extra-2.bin" in parent_routes, "rerun child1 extra-2.bin should route via parent anchor")
    new_bundle_path = suite.remote_bundle_path_for_member(parent_anchor_dir, "child1/extra-1.bin")
    suite.require("/promoted/.dcpacks/bundles/" in f"/{new_bundle_path}", "rerun child1 extra member should stay under promoted parent anchor")
    suite.require("/promoted/child1/.dcpacks/" not in f"/{new_bundle_path}", "rerun child1 extra member should not create a fresh child1 anchor")

    parent_child_ls = suite.ls(
        "ls-parent-anchor-child1-after-rerun",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{_rel_remote(parent_remote, 'promoted', 'child1')}",
        ],
    )
    suite.require_contains(parent_child_ls.output, "../.dcpacks/bundles/", context="parent-anchor child1 listing")
    suite.require_contains(parent_child_ls.output, "extra-1.bin", context="parent-anchor child1 listing")
    suite.require_contains(parent_child_ls.output, "extra-2.bin", context="parent-anchor child1 listing")

    sparse_subset_dest = suite.download_root / "subset-sparse"
    sparse_subset_tsv = suite.workdir / "subset-sparse.tsv"
    sparse_rows = [
        (
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child1', 'p1.bin')}",
            str(sparse_subset_dest / "promoted-child1-p1.bin"),
        ),
        (
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'local-anchor', 'plain-large.bin')}",
            str(sparse_subset_dest / "plain-large.bin"),
        ),
    ]
    _write_file_list(sparse_subset_tsv, sparse_rows)

    sparse_dry_run = suite.cp(
        "dry-run-download-sparse-subset",
        [
            *suite.connection_args(include_workers=True),
            "--dry-run",
            "--file-list",
            str(sparse_subset_tsv),
        ],
    )
    suite.require_contains(
        sparse_dry_run.output,
        "sparse member read" if sparse_read_available else "full bundle read",
        context="sparse subset dry-run",
    )

    sparse_download = suite.cp(
        "download-sparse-subset",
        [
            *suite.connection_args(include_workers=True),
            "--no-stage",
            "--file-list",
            str(sparse_subset_tsv),
        ],
    )
    if sparse_read_available:
        suite.require_not_contains(sparse_download.output, "bundle sparse read fallback", context="sparse subset actual download")
    sparse_expected = {
        "promoted-child1-p1.bin": suite.expected_maps["copy-case"]["promoted/child1/p1.bin"],
        "plain-large.bin": suite.expected_maps["copy-case"]["local-anchor/plain-large.bin"],
    }
    _download_compare(suite, "subset-sparse", sparse_subset_dest, sparse_expected)

    full_subset_dest = suite.download_root / "subset-full"
    full_subset_tsv = suite.workdir / "subset-full.tsv"
    full_rows = [
        (
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child1', 'p1.bin')}",
            str(full_subset_dest / "child1" / "p1.bin"),
        ),
        (
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child1', 'p2.bin')}",
            str(full_subset_dest / "child1" / "p2.bin"),
        ),
        (
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child2', 'p3.bin')}",
            str(full_subset_dest / "child2" / "p3.bin"),
        ),
        (
            f"{suite.prefix}:/{_rel_remote(copy_remote, 'promoted', 'child2', 'p4.bin')}",
            str(full_subset_dest / "child2" / "p4.bin"),
        ),
    ]
    _write_file_list(full_subset_tsv, full_rows)
    full_dry_run = suite.cp(
        "dry-run-download-full-bundle-subset",
        [
            *suite.connection_args(include_workers=True),
            "--dry-run",
            "--file-list",
            str(full_subset_tsv),
        ],
    )
    suite.require_contains(full_dry_run.output, "full bundle read", context="full bundle dry-run")

    if suite.args.download_mode in {"no-stage", "both"}:
        no_stage_dest = suite.download_root / "copy-case-no-stage"
        no_stage = suite.cp(
            "download-copy-case-no-stage",
            [
                *suite.connection_args(include_workers=True),
                "--no-stage",
                "-R",
                copy_remote_prefixed,
                str(no_stage_dest),
            ],
        )
        suite.require_contains(no_stage.output, "transparent unpack via", context="copy-case no-stage download")
        _download_compare(suite, "copy-case-no-stage", no_stage_dest, suite.expected_maps["copy-case"])

        no_stage_rerun = suite.cp(
            "rerun-download-copy-case-no-stage",
            [
                *suite.connection_args(include_workers=True),
                "--no-stage",
                "-R",
                copy_remote_prefixed,
                str(no_stage_dest),
            ],
        )
        suite.require_contains(no_stage_rerun.output, "already verified, skipping stage/copy", context="copy-case no-stage rerun")

    if suite.args.download_mode in {"stage", "both"}:
        stage_dest = suite.download_root / "copy-case-stage"
        staged = suite.cp(
            "download-copy-case-stage",
            [
                *suite.connection_args(include_workers=True),
                "-R",
                copy_remote_prefixed,
                str(stage_dest),
            ],
        )
        suite.require_contains(staged.output, "staging :", context="copy-case staged download")
        _download_compare(suite, "copy-case-stage", stage_dest, suite.expected_maps["copy-case"])

    suite.cp(
        "upload-delete-case",
        [
            *suite.connection_args(include_workers=True),
            *suite.bundle_upload_args(),
            "-R",
            str(delete_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )

    delete_anchor_dir = _rel_remote(delete_remote, "bundle")
    delete_bundle_path = suite.remote_bundle_path_for_member(delete_anchor_dir, "del1.bin")

    move_one_dest = suite.download_root / "delete-case-move-one" / "del1.bin"
    suite.mv(
        "move-delete-case-one-member",
        [
            *suite.connection_args(include_workers=True),
            "--no-stage",
            f"{suite.prefix}:/{_rel_remote(delete_remote, 'bundle', 'del1.bin')}",
            str(move_one_dest),
        ],
    )
    suite.require(move_one_dest.is_file(), "first delete-case moved file is missing locally")
    suite.require(_sha256_local(move_one_dest) == suite.expected_maps["delete-case"]["bundle/del1.bin"], "first delete-case moved file checksum mismatch")

    delete_routes_after_one = suite.anchor_routes(delete_anchor_dir)
    suite.require("del1.bin" not in delete_routes_after_one, "del1.bin route should be removed after move")
    suite.require({"del2.bin", "del3.bin"}.issubset(set(delete_routes_after_one)), "remaining delete-case members should still be routed")

    deleted_map = suite.bundle_deleted(delete_bundle_path)
    suite.require("del1.bin" in deleted_map, "delete-case bundle tombstones should include del1.bin after first move")

    delete_ls_after_one = suite.ls(
        "ls-delete-case-after-one-move",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{delete_anchor_dir}",
        ],
    )
    suite.require_not_contains(delete_ls_after_one.output, "del1.bin", context="delete-case listing after first move")
    suite.require_contains(delete_ls_after_one.output, "del2.bin", context="delete-case listing after first move")
    suite.require_contains(delete_ls_after_one.output, "del3.bin", context="delete-case listing after first move")

    move_remaining_dest = suite.download_root / "delete-case-move-rest"
    move_remaining_tsv = suite.workdir / "delete-case-move-rest.tsv"
    remaining_rows = [
        (
            f"{suite.prefix}:/{_rel_remote(delete_remote, 'bundle', 'del2.bin')}",
            str(move_remaining_dest / "del2.bin"),
        ),
        (
            f"{suite.prefix}:/{_rel_remote(delete_remote, 'bundle', 'del3.bin')}",
            str(move_remaining_dest / "del3.bin"),
        ),
    ]
    _write_file_list(move_remaining_tsv, remaining_rows)

    suite.mv(
        "move-delete-case-remaining-members",
        [
            *suite.connection_args(include_workers=True),
            "--no-stage",
            "--file-list",
            str(move_remaining_tsv),
        ],
    )
    suite.require((move_remaining_dest / "del2.bin").is_file(), "del2.bin was not moved locally")
    suite.require((move_remaining_dest / "del3.bin").is_file(), "del3.bin was not moved locally")

    delete_paths_after_full = suite.lsjson_paths(delete_remote)
    suite.require(not any(path.endswith(".dcpbundle") for path in delete_paths_after_full), "delete-case bundle object should be removed after all members are retired")
    delete_routes_after_full = suite.anchor_routes(delete_anchor_dir)
    suite.require(not delete_routes_after_full, "delete-case anchor routes should be empty after all members are moved out")

    rerun_remaining_move = suite.mv(
        "rerun-move-delete-case-remaining-members",
        [
            *suite.connection_args(include_workers=True),
            "--no-stage",
            "--file-list",
            str(move_remaining_tsv),
        ],
    )
    suite.require(
        _delete_case_move_rerun_resumed_cleanly(rerun_remaining_move.output),
        "rerun of delete-case file-list move should resume cleanly",
    )

    upload_move_source_map = dict(suite.expected_maps["upload-move-case"])
    suite.mv(
        "upload-move-case",
        [
            *suite.connection_args(include_workers=True),
            *suite.bundle_upload_args(),
            "-R",
            str(upload_move_local),
            f"{suite.prefix}:/{suite.remote_run_root}/",
        ],
    )
    remaining_local_files = [path for path in upload_move_local.rglob("*") if path.is_file()]
    suite.require(not remaining_local_files, "upload-move-case should delete local source files after verified bundled upload")

    upload_move_anchor = upload_move_remote
    upload_move_routes = suite.anchor_routes(upload_move_anchor)
    suite.require(set(upload_move_routes) == {"um1.bin", "um2.bin", "um3.bin"}, "upload-move-case routes do not match expected moved members")
    upload_move_ls = suite.ls(
        "ls-upload-move-case",
        [
            *suite.connection_args(include_verbose=False),
            "-l",
            f"{suite.prefix}:/{upload_move_anchor}",
        ],
    )
    suite.require_contains(upload_move_ls.output, "um1.bin", context="upload-move-case listing")
    suite.require_contains(upload_move_ls.output, "bundles:", context="upload-move-case listing")

    _summary_step(
        suite.summary,
        "upload-move-case-source-checks",
        {
            "source_files_before_move": upload_move_source_map,
            "source_files_after_move": [str(path) for path in remaining_local_files],
            "remote_routes": upload_move_routes,
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    suite: LiveSuite | None = None
    try:
        suite = LiveSuite(args)
        _run_suite(suite)
        suite.summary["status"] = "ok"
        report_path = suite.write_report()
        print(json.dumps(suite.summary, indent=2, sort_keys=True))
        LOG.info("summary: %s", report_path)
        LOG.info("remote run root: %s", suite.remote_run_root_prefixed)
        return 0
    except Exception as exc:
        if suite is None:
            print(json.dumps({"status": "error", "error": str(exc)}, indent=2, sort_keys=True))
            return 1
        suite.summary["status"] = "error"
        suite.summary["error"] = str(exc)
        report_path = suite.write_report()
        print(json.dumps(suite.summary, indent=2, sort_keys=True))
        LOG.exception("live suite failed")
        LOG.info("summary: %s", report_path)
        LOG.info("remote run root: %s", suite.remote_run_root_prefixed)
        return 1
    finally:
        if suite is not None:
            suite.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
