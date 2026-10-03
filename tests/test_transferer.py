from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dcache_cp import bundles
from dcache_cp import cli


SQUASHFS_TOOLS_AVAILABLE = bool(shutil.which("mksquashfs") and shutil.which("unsquashfs"))


def _adler_hex(data: bytes) -> str:
    return f"{zlib.adler32(data, 1) & 0xFFFFFFFF:08x}"


class TransfererDownloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)
        self.config = self.root / "dcache.conf"
        self.config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")

    def _make_transferer(
        self,
        *,
        delete_source: bool = False,
        skip_verified: bool = True,
        max_retries: int = 0,
    ) -> cli.Transferer:
        return cli.Transferer(
            rclone_config=self.config,
            remote="dcache",
            ada_cmd="ada",
            api=None,
            max_retries=max_retries,
            retry_wait=0,
            copy_timeout="30s",
            skip_verified=skip_verified,
            delete_source=delete_source,
        )

    def test_move_download_deletes_remote_after_verified_replace(self) -> None:
        transferer = self._make_transferer(delete_source=True, skip_verified=False)
        local_path = self.root / "downloads" / "sample.tsv"
        entry = {
            "remote_path": "meta/sample.tsv",
            "local_path": local_path,
            "rel": "sample.tsv",
            "size": 4,
        }
        payload = b"abcd"
        deleted_targets: list[str] = []

        def fake_copy(_src: str, dst: str) -> None:
            Path(dst).write_bytes(payload)

        with mock.patch.object(transferer, "_rclone_copyto", side_effect=fake_copy), mock.patch.object(
            transferer,
            "_remote_adler",
            return_value=_adler_hex(payload),
        ), mock.patch.object(transferer, "_rclone_deletefile", side_effect=deleted_targets.append):
            result = transferer.download(entry)

        self.assertEqual(local_path.read_bytes(), payload)
        self.assertEqual(result["local_path"], str(local_path))
        self.assertFalse(result["skipped"])
        self.assertEqual(deleted_targets, ["dcache:meta/sample.tsv"])
        self.assertFalse(any(child.name.endswith(".dcache_cp.part") for child in local_path.parent.iterdir()))

    def test_failed_download_keeps_existing_local_file_intact(self) -> None:
        transferer = self._make_transferer(skip_verified=False, max_retries=0)
        local_path = self.root / "downloads" / "sample.tsv"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"existing-data")
        entry = {
            "remote_path": "meta/sample.tsv",
            "local_path": local_path,
            "rel": "sample.tsv",
            "size": 8,
        }
        downloaded_payload = b"new-data"
        remote_payload = b"other-data"

        def fake_copy(_src: str, dst: str) -> None:
            Path(dst).write_bytes(downloaded_payload)

        with mock.patch.object(transferer, "_rclone_copyto", side_effect=fake_copy), mock.patch.object(
            transferer,
            "_remote_adler",
            return_value=_adler_hex(remote_payload),
        ), mock.patch.object(transferer, "_rclone_deletefile") as delete_mock, mock.patch.object(cli.LOG, "warning"):
            with self.assertRaises(RuntimeError):
                transferer.download(entry)

        self.assertEqual(local_path.read_bytes(), b"existing-data")
        delete_mock.assert_not_called()
        self.assertFalse(any(child.name.endswith(".dcache_cp.part") for child in local_path.parent.iterdir()))

    def test_skip_verified_move_download_deletes_remote_without_copy(self) -> None:
        transferer = self._make_transferer(delete_source=True, skip_verified=True)
        local_path = self.root / "downloads" / "sample.tsv"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        payload = b"already-here"
        local_path.write_bytes(payload)
        entry = {
            "remote_path": "meta/sample.tsv",
            "local_path": local_path,
            "rel": "sample.tsv",
            "size": len(payload),
        }
        deleted_targets: list[str] = []

        with mock.patch.object(transferer, "_remote_adler", return_value=_adler_hex(payload)), mock.patch.object(
            transferer,
            "_rclone_copyto",
        ) as copy_mock, mock.patch.object(transferer, "_rclone_deletefile", side_effect=deleted_targets.append):
            result = transferer.download(entry)

        copy_mock.assert_not_called()
        self.assertTrue(result["skipped"])
        self.assertEqual(deleted_targets, ["dcache:meta/sample.tsv"])

    def test_remote_adler_retries_transient_transport_failure(self) -> None:
        transferer = self._make_transferer(skip_verified=True)
        remote_path = "dataset/part-020/a.txt"
        transient = subprocess.CompletedProcess(
            args=["ada", "--checksum", "/" + remote_path],
            returncode=1,
            stdout="ERROR: authentication failed. Please check your credentials.\n",
            stderr="curl: (6) Could not resolve host: dcacheview.grid.surfsara.nl\n",
        )
        success = subprocess.CompletedProcess(
            args=["ada", "--checksum", "/" + remote_path],
            returncode=0,
            stdout=f"/{remote_path}  ADLER32={_adler_hex(b'aaaa')}\n",
            stderr="",
        )

        with mock.patch.object(cli, "run_command", side_effect=[transient, success]) as run_mock, mock.patch.object(
            cli.time,
            "sleep",
        ) as sleep_mock:
            adler = transferer._remote_adler(remote_path)

        self.assertEqual(adler, _adler_hex(b"aaaa"))
        self.assertEqual(run_mock.call_count, 2)
        sleep_mock.assert_called_once()

    @unittest.skipUnless(SQUASHFS_TOOLS_AVAILABLE, "squashfs tools required")
    def test_download_bundle_extracts_requested_members(self) -> None:
        transferer = self._make_transferer(skip_verified=True)

        source_root = self.root / "source"
        source_root.mkdir(parents=True, exist_ok=True)
        entries = []
        for name, payload in (("a.txt", b"aaaa"), ("b.txt", b"bbbb")):
            source = source_root / name
            source.write_bytes(payload)
            entries.append({
                "source": source,
                "resolved_source": source,
                "rel": name,
                "remote_path": f"dataset/part-010/{name}",
                "size": len(payload),
            })

        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )
        job = plan.anchors[0].bundles[0]
        materialized = bundles.materialize_bundle_job(job)
        bundle_path = Path(materialized["resolved_source"])
        self.addCleanup(bundle_path.unlink, missing_ok=True)
        bundle_payload = bundle_path.read_bytes()

        download_root = self.root / "downloads"
        entry = {
            "remote_path": job.remote_path,
            "rel": f"bundle:{job.bundle_id[:12]}",
            "size": job.logical_total_bytes,
            "stage_size": len(bundle_payload),
            "bundle_format": job.bundle_object_xattrs["dcache_cp.bundle.format"],
            "bundle_members": [
                {
                    "rel": member.anchor_rel,
                    "remote_path": member.remote_path,
                    "local_path": download_root / member.anchor_rel,
                    "anchor_rel": member.anchor_rel,
                    "size": member.size,
                    "adler32": member.adler32,
                    "mtime_ns": member.mtime_ns,
                    "mode": member.mode,
                }
                for member in job.members
            ],
        }

        def fake_copy(_src: str, dst: str) -> None:
            Path(dst).write_bytes(bundle_payload)

        with mock.patch.object(transferer, "_rclone_copyto", side_effect=fake_copy), mock.patch.object(
            transferer,
            "_remote_adler",
            return_value=_adler_hex(bundle_payload),
        ):
            result = transferer.download_bundle(entry)

        self.assertFalse(result["skipped"])
        self.assertEqual(len(result["bundle_members"]), 2)
        self.assertEqual((download_root / "a.txt").read_bytes(), b"aaaa")
        self.assertEqual((download_root / "b.txt").read_bytes(), b"bbbb")

    def test_download_bundle_uses_sparse_read_for_requested_subset(self) -> None:
        transferer = self._make_transferer(skip_verified=True)
        download_root = self.root / "downloads-sparse"
        local_path = download_root / "a.txt"
        member = {
            "rel": "a.txt",
            "remote_path": "dataset/part-012/a.txt",
            "local_path": local_path,
            "anchor_rel": "a.txt",
            "size": 4,
            "adler32": _adler_hex(b"aaaa"),
            "mtime_ns": 0,
            "mode": 0o644,
        }
        staged_path = transferer._create_download_temp_path(local_path)
        staged_path.write_bytes(b"aaaa")
        self.addCleanup(staged_path.unlink, missing_ok=True)
        entry = {
            "remote_path": "dataset/part-012/.dcpacks/bundles/bundle-id.dcpbundle",
            "rel": "bundle:part-012",
            "size": 4,
            "stage_size": 1024,
            "bundle_member_total": 10,
            "bundle_format": "squashfs",
            "bundle_members": [member],
        }

        with mock.patch.object(cli._BundleMountManager, "available", return_value=True), mock.patch.object(
            transferer,
            "_extract_squashfs_bundle_members_via_mount",
            return_value=[(staged_path, local_path, member)],
        ) as sparse_mock, mock.patch.object(transferer, "_rclone_copyto") as copy_mock:
            result = transferer.download_bundle(entry)

        sparse_mock.assert_called_once()
        copy_mock.assert_not_called()
        self.assertTrue(result["sparse"])
        self.assertEqual(local_path.read_bytes(), b"aaaa")

    def test_extract_sparse_bundle_members_via_mount_falls_back_to_unsquashfs(self) -> None:
        transferer = self._make_transferer(skip_verified=True)
        local_path = self.root / "downloads-sparse-fallback" / "a.txt"
        member = {
            "rel": "a.txt",
            "remote_path": "dataset/part-014/a.txt",
            "local_path": local_path,
            "anchor_rel": "a.txt",
            "size": 4,
            "adler32": _adler_hex(b"aaaa"),
            "mtime_ns": 0,
            "mode": 0o644,
        }
        bundle_path = self.root / "mounted" / "bundle-id.dcpbundle"

        mount_manager = mock.Mock()
        mount_manager.bundle_local_path.return_value = bundle_path

        with mock.patch.object(cli.shutil, "which", side_effect=lambda name: None if name == "sqfscat" else shutil.which(name)), mock.patch.object(
            transferer,
            "_get_bundle_mount_manager",
            return_value=mount_manager,
        ), mock.patch.object(
            transferer,
            "_extract_squashfs_bundle_members",
            return_value=[(self.root / "temp-a", local_path, member)],
        ) as extract_mock:
            result = transferer._extract_squashfs_bundle_members_via_mount(
                "dataset/part-014/.dcpacks/bundles/bundle-id.dcpbundle",
                [member],
            )

        extract_mock.assert_called_once_with(bundle_path, [member])
        self.assertEqual(result, [(self.root / "temp-a", local_path, member)])

    @unittest.skipUnless(SQUASHFS_TOOLS_AVAILABLE, "squashfs tools required")
    def test_download_bundle_falls_back_to_full_copy_when_sparse_read_fails(self) -> None:
        transferer = self._make_transferer(skip_verified=True)

        source_root = self.root / "source-fallback"
        source_root.mkdir(parents=True, exist_ok=True)
        entries = []
        for name, payload in (("a.txt", b"aaaa"), ("b.txt", b"bbbb")):
            source = source_root / name
            source.write_bytes(payload)
            entries.append({
                "source": source,
                "resolved_source": source,
                "rel": name,
                "remote_path": f"dataset/part-013/{name}",
                "size": len(payload),
            })

        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )
        job = plan.anchors[0].bundles[0]
        materialized = bundles.materialize_bundle_job(job)
        bundle_path = Path(materialized["resolved_source"])
        self.addCleanup(bundle_path.unlink, missing_ok=True)
        bundle_payload = bundle_path.read_bytes()

        download_root = self.root / "downloads-fallback"
        target_member = job.members[0]
        entry = {
            "remote_path": job.remote_path,
            "rel": f"bundle:{job.bundle_id[:12]}",
            "size": target_member.size,
            "stage_size": len(bundle_payload),
            "bundle_member_total": len(job.members),
            "bundle_format": job.bundle_object_xattrs["dcache_cp.bundle.format"],
            "bundle_members": [{
                "rel": target_member.anchor_rel,
                "remote_path": target_member.remote_path,
                "local_path": download_root / target_member.anchor_rel,
                "anchor_rel": target_member.anchor_rel,
                "size": target_member.size,
                "adler32": target_member.adler32,
                "mtime_ns": target_member.mtime_ns,
                "mode": target_member.mode,
            }],
        }

        def fake_copy(_src: str, dst: str) -> None:
            Path(dst).write_bytes(bundle_payload)

        with mock.patch.object(cli._BundleMountManager, "available", return_value=True), mock.patch.object(
            transferer,
            "_extract_squashfs_bundle_members_via_mount",
            side_effect=RuntimeError("mount failed"),
        ) as sparse_mock, mock.patch.object(transferer, "_rclone_copyto", side_effect=fake_copy) as copy_mock, mock.patch.object(
            transferer,
            "_remote_adler",
            return_value=_adler_hex(bundle_payload),
        ):
            result = transferer.download_bundle(entry)

        sparse_mock.assert_called_once()
        copy_mock.assert_called_once()
        self.assertNotIn("sparse", result)
        self.assertEqual((download_root / target_member.anchor_rel).read_bytes(), b"aaaa")

    def test_extract_bundle_members_rejects_unsupported_format(self) -> None:
        transferer = self._make_transferer(skip_verified=True)
        with self.assertRaisesRegex(RuntimeError, "unsupported bundle format"):
            transferer._extract_bundle_members(Path("unused.dcpbundle"), [], bundle_format="tar")

    def test_extract_squashfs_bundle_members_uses_nonexistent_unsquashfs_dest(self) -> None:
        transferer = self._make_transferer(skip_verified=True)
        workspace_root = self.root / "unsquashfs-workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        bundle_path = self.root / "bundle.dcpbundle"
        bundle_path.write_bytes(b"placeholder")
        local_path = self.root / "downloads" / "a.txt"
        member = {
            "rel": "a.txt",
            "remote_path": "dataset/part-021/a.txt",
            "local_path": local_path,
            "anchor_rel": "a.txt",
            "size": 4,
            "adler32": _adler_hex(b"aaaa"),
            "mtime_ns": 0,
            "mode": 0o644,
        }

        def fake_run(cmd: list[str], check: bool, stdout, stderr, text: bool):
            self.assertFalse(check)
            self.assertTrue(text)
            dest = Path(cmd[cmd.index("-dest") + 1])
            self.assertFalse(dest.exists())
            dest.mkdir(parents=True, exist_ok=True)
            (dest / "a.txt").write_bytes(b"aaaa")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with mock.patch.object(cli.shutil, "which", side_effect=lambda name: "/usr/bin/unsquashfs" if name == "unsquashfs" else shutil.which(name)), mock.patch.object(
            cli,
            "dcache_mkdtemp",
            return_value=str(workspace_root),
        ), mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            staged = transferer._extract_squashfs_bundle_members(bundle_path, [member])

        self.assertEqual(len(staged), 1)
        staged_path, staged_local_path, staged_member = staged[0]
        self.assertEqual(staged_local_path, local_path)
        self.assertEqual(staged_member["rel"], "a.txt")
        self.assertEqual(staged_path.read_bytes(), b"aaaa")
        staged_path.unlink(missing_ok=True)
        self.assertFalse(workspace_root.exists())

    def test_filter_verified_bundle_download_skips_when_members_match(self) -> None:
        transferer = self._make_transferer(skip_verified=True)
        download_root = self.root / "downloads"
        download_root.mkdir(parents=True, exist_ok=True)
        (download_root / "a.txt").write_bytes(b"aaaa")
        (download_root / "b.txt").write_bytes(b"bbbb")
        entry = {
            "remote_path": "dataset/part-011/.dcpacks/bundles/bundle-id.dcpbundle",
            "rel": "bundle:part-011",
            "size": 8,
            "stage_size": 16,
            "bundle_members": [
                {
                    "rel": "a.txt",
                    "remote_path": "dataset/part-011/a.txt",
                    "local_path": download_root / "a.txt",
                    "anchor_rel": "a.txt",
                    "size": 4,
                    "adler32": _adler_hex(b"aaaa"),
                    "mtime_ns": 0,
                    "mode": 0o644,
                },
                {
                    "rel": "b.txt",
                    "remote_path": "dataset/part-011/b.txt",
                    "local_path": download_root / "b.txt",
                    "anchor_rel": "b.txt",
                    "size": 4,
                    "adler32": _adler_hex(b"bbbb"),
                    "mtime_ns": 0,
                    "mode": 0o644,
                },
            ],
        }

        remaining, skipped = cli._filter_verified_download_entries([entry], transferer)

        self.assertEqual(remaining, [])
        self.assertEqual(skipped, [entry])


if __name__ == "__main__":
    unittest.main()
