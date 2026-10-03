#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import posixpath
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dcache_cp.bundles import bundle_object_remote_path, decode_anchor_routes, decode_bundle_members
from dcache_cp.cli import (
    _rclone_lsjson,
    load_rclone_config,
    main as dcache_main,
    parse_remote_prefix,
    resolve_api_url,
    resolve_config_for_prefix,
    resolve_remote_name,
    run_command,
)
from dcache_cp.tempfiles import mkdtemp as dcache_mkdtemp
from dcache_cp.xattrs import NamespaceXattrClient, extract_bearer_token


LOG = logging.getLogger("dcache_bundle_roundtrip_probe")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _adler32_local(path: Path) -> str:
    import zlib

    checksum = 1
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(8 * 1024 * 1024)
            if not chunk:
                break
            checksum = zlib.adler32(chunk, checksum)
    return f"{checksum & 0xFFFFFFFF:08x}"


def _write_dataset(upload_root: Path) -> dict[str, str]:
    files = {
        "packed/a.txt": "bundle probe alpha\n" * 8,
        "packed/b.txt": "bundle probe beta\n" * 9,
        "packed/c.json": json.dumps({"probe": "gamma", "value": 3}, sort_keys=True) + "\n",
        "plain/lone.txt": "plain file should stay plain\n" * 3,
    }
    for relative_path, text in files.items():
        path = upload_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return files


def _relative_file_map(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(root)).replace(os.sep, "/")
        out[rel] = _adler32_local(path)
    return out


def _maybe_arg(flag: str, value: str | Path | None) -> list[str]:
    if value is None:
        return []
    return [flag, str(value)]


def _run_dcache_cp(argv: list[str]) -> int:
    LOG.info("dcache_cp %s", " ".join(argv))
    return dcache_main(argv)


def _purge_remote_tree(config_path: Path, remote_name: str, remote_root: str) -> None:
    run_command([
        "rclone",
        "--config",
        str(config_path),
        "purge",
        f"{remote_name}:{remote_root}",
    ])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dcache_bundle_roundtrip_probe.py",
        description=(
            "Exercise dcache_cp bundled upload + transparent download against a real dCache target. "
            "The script creates a tiny mixed dataset, uploads it with bundling enabled, inspects the remote xattrs, "
            "downloads it again, and emits a JSON summary you can paste back."
        ),
    )
    parser.add_argument(
        "target",
        help="Remote directory target with prefix, e.g. dcache:/pnfs/.../bundle-probe/",
    )
    parser.add_argument("--config", type=Path, help="rclone config / macaroon file override")
    parser.add_argument("--remote", help="rclone remote name override")
    parser.add_argument("--api", help="dCache API URL override")
    parser.add_argument("--ada", help="ada executable override")
    parser.add_argument(
        "--download-mode",
        choices=["no-stage", "stage"],
        default="no-stage",
        help="Use the simple download path or the staging pipeline (default: no-stage)",
    )
    parser.add_argument(
        "--check-raw-download",
        action="store_true",
        help="Also run a second download with --no-unpack-bundles and verify the raw .dcpbundle object appears locally",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep the temporary local working directory even on success",
    )
    parser.add_argument(
        "--keep-remote",
        action="store_true",
        help="Keep the uploaded remote probe tree even on success",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        help="Use a specific local working directory instead of an auto-created temp dir",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="Write the JSON summary to this path in addition to printing it",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    prefix, target_path = parse_remote_prefix(args.target)
    if not prefix:
        parser.error("target must include a remote prefix such as dcache:")
    remote_target_dir = target_path.strip("/")
    if not remote_target_dir:
        parser.error("target path must not be empty")

    config_path = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(config_path)
    remote_name = resolve_remote_name(config, args.remote)
    api = resolve_api_url(args.api, config[remote_name])
    token = extract_bearer_token(config[remote_name], config_path)
    if not api:
        raise RuntimeError("bundle probe requires a resolved dCache API URL")
    if not token:
        raise RuntimeError("bundle probe requires a bearer token in the selected config")
    xattr_client = NamespaceXattrClient(api, token)

    probe_id = time.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    dataset_name = f"bundle-roundtrip-{probe_id}"
    remote_root = posixpath.join(remote_target_dir, dataset_name)
    remote_root_prefixed = f"{prefix}:/{remote_root}"
    bundle_anchor_dir = posixpath.join(remote_root, "packed")

    auto_workdir = args.workdir is None
    workdir = args.workdir or Path(dcache_mkdtemp(prefix="dcache-bundle-roundtrip-"))
    workdir.mkdir(parents=True, exist_ok=True)
    upload_root = workdir / dataset_name
    download_root = workdir / "download-transparent"
    raw_download_root = workdir / "download-raw"

    summary: dict[str, object] = {
        "probe_id": probe_id,
        "target": args.target,
        "config_path": str(config_path),
        "remote_name": remote_name,
        "api": api,
        "workdir": str(workdir),
        "dataset_name": dataset_name,
        "remote_root": remote_root,
        "download_mode": args.download_mode,
        "steps": {},
        "artifacts_kept": {
            "local": True,
            "remote": True,
        },
    }

    success = False
    try:
        expected_text_files = _write_dataset(upload_root)
        expected_files = _relative_file_map(upload_root)
        summary["expected_files"] = expected_files

        upload_args = [
            *(_maybe_arg("--config", config_path)),
            *(_maybe_arg("--remote", remote_name)),
            *(_maybe_arg("--api", api)),
            *(_maybe_arg("--ada", args.ada)),
            "--bundle-small-files",
            "--bundle-min-dir-total",
            "1",
            "--bundle-max-file-size",
            "1MiB",
            "--bundle-target-size",
            "1MiB",
            "-R",
            str(upload_root),
            f"{prefix}:/{remote_target_dir}/",
        ]
        upload_rc = _run_dcache_cp(upload_args)
        summary["steps"]["upload"] = {"rc": upload_rc}
        if upload_rc != 0:
            raise RuntimeError(f"upload failed with exit code {upload_rc}")

        remote_listing = _rclone_lsjson(config_path, remote_name, remote_root, recursive=True)
        listing_paths = sorted(
            str(entry.get("Path"))
            for entry in remote_listing
            if not entry.get("IsDir", False) and isinstance(entry.get("Path"), str)
        )
        summary["steps"]["remote_listing"] = {
            "paths": listing_paths,
        }

        anchor_xattrs = xattr_client.list_xattrs(bundle_anchor_dir)
        routes = decode_anchor_routes(anchor_xattrs)
        if not routes:
            raise RuntimeError(f"no bundle routes found on anchor directory {bundle_anchor_dir}")
        bundle_ids = sorted(set(routes.values()))
        bundle_objects: list[dict[str, object]] = []
        for bundle_id in bundle_ids:
            bundle_remote_path = bundle_object_remote_path(bundle_anchor_dir, bundle_id)
            bundle_xattrs = xattr_client.list_xattrs(bundle_remote_path)
            members = decode_bundle_members(bundle_xattrs)
            bundle_objects.append(
                {
                    "bundle_id": bundle_id,
                    "remote_path": bundle_remote_path,
                    "member_paths": sorted(members),
                    "member_count": len(members),
                }
            )
        summary["steps"]["xattrs"] = {
            "anchor_dir": bundle_anchor_dir,
            "routes": routes,
            "bundle_objects": bundle_objects,
        }

        download_args = [
            *(_maybe_arg("--config", config_path)),
            *(_maybe_arg("--remote", remote_name)),
            *(_maybe_arg("--api", api)),
            *(_maybe_arg("--ada", args.ada)),
        ]
        if args.download_mode == "no-stage":
            download_args.append("--no-stage")
        download_args.extend([
            "-R",
            remote_root_prefixed,
            str(download_root),
        ])
        download_rc = _run_dcache_cp(download_args)
        summary["steps"]["download_transparent"] = {"rc": download_rc}
        if download_rc != 0:
            raise RuntimeError(f"transparent download failed with exit code {download_rc}")

        downloaded_files = _relative_file_map(download_root)
        missing_local_bundle_dir = not (download_root / "packed" / ".dcpacks").exists()
        plain_text = (download_root / "plain" / "lone.txt").read_text(encoding="utf-8")
        summary["steps"]["download_transparent"].update(
            {
                "files": downloaded_files,
                "matches_expected": downloaded_files == expected_files,
                "bundle_dir_hidden": missing_local_bundle_dir,
                "plain_text_preview": plain_text[:120],
            }
        )
        if downloaded_files != expected_files:
            raise RuntimeError("downloaded transparent file set does not match uploaded content")
        if not missing_local_bundle_dir:
            raise RuntimeError("transparent download unexpectedly materialized a local .dcpacks directory")

        if args.check_raw_download:
            raw_args = [
                *(_maybe_arg("--config", config_path)),
                *(_maybe_arg("--remote", remote_name)),
                *(_maybe_arg("--api", api)),
                *(_maybe_arg("--ada", args.ada)),
                "--no-unpack-bundles",
            ]
            if args.download_mode == "no-stage":
                raw_args.append("--no-stage")
            raw_args.extend([
                "-R",
                remote_root_prefixed,
                str(raw_download_root),
            ])
            raw_rc = _run_dcache_cp(raw_args)
            raw_bundle_paths = sorted(
                str(path.relative_to(raw_download_root)).replace(os.sep, "/")
                for path in raw_download_root.rglob("*.dcpbundle")
            )
            summary["steps"]["download_raw"] = {
                "rc": raw_rc,
                "bundle_paths": raw_bundle_paths,
            }
            if raw_rc != 0:
                raise RuntimeError(f"raw bundle download failed with exit code {raw_rc}")
            if not raw_bundle_paths:
                raise RuntimeError("raw bundle download did not produce any .dcpbundle objects locally")

        success = True
        summary["status"] = "ok"
    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = str(exc)
        LOG.exception("bundle round-trip probe failed")
    finally:
        keep_remote = args.keep_remote or not success
        keep_local = args.keep_local or not success
        summary["artifacts_kept"] = {
            "local": keep_local,
            "remote": keep_remote,
        }

        report_text = json.dumps(summary, indent=2, sort_keys=True)
        print(report_text)

        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(report_text + "\n", encoding="utf-8")
        elif keep_local:
            default_report = workdir / "bundle-roundtrip-report.json"
            default_report.write_text(report_text + "\n", encoding="utf-8")

        if success and not keep_remote:
            try:
                _purge_remote_tree(config_path, remote_name, remote_root)
                LOG.info("removed remote probe tree %s", remote_root_prefixed)
            except Exception as cleanup_exc:
                LOG.warning("remote cleanup failed for %s: %s", remote_root_prefixed, cleanup_exc)

        if success and not keep_local:
            try:
                shutil.rmtree(workdir)
                LOG.info("removed local workdir %s", workdir)
            except Exception as cleanup_exc:
                LOG.warning("local cleanup failed for %s: %s", workdir, cleanup_exc)

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
