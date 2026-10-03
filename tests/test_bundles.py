from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dcache_cp import bundles
from dcache_cp import cli
from dcache_cp.xattrs import NamespaceXattrError


SQUASHFS_TOOLS_AVAILABLE = bool(shutil.which("mksquashfs") and shutil.which("unsquashfs"))


class ApiResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = cli.configparser.ConfigParser()
        self.parser.read_string(
            "[dcache]\n"
            "type = webdav\n"
            "url = https://webdav.grid.surfsara.nl/\n"
        )
        self.remote_cfg = self.parser["dcache"]

    def test_resolve_api_url_infers_from_webdav_remote_url(self) -> None:
        with mock.patch.object(cli, "_read_ada_default_api", return_value=None):
            self.assertEqual(
                cli.resolve_api_url(None, self.remote_cfg),
                "https://webdav.grid.surfsara.nl/api/v1",
            )

    def test_resolve_api_url_prefers_configured_api_over_inferred_url(self) -> None:
        self.remote_cfg["api"] = "https://configured.example/api/v2"

        with mock.patch.object(cli, "_read_ada_default_api", return_value="https://ada-default.example/api/v1"):
            self.assertEqual(
                cli.resolve_api_url(None, self.remote_cfg),
                "https://configured.example/api/v2",
            )

    def test_resolve_api_url_prefers_ada_default_over_inferred_url(self) -> None:
        with mock.patch.object(cli, "_read_ada_default_api", return_value="https://ada-default.example/api/v1"):
            self.assertEqual(
                cli.resolve_api_url(None, self.remote_cfg),
                "https://ada-default.example/api/v1",
            )

    def test_resolve_api_url_returns_none_when_remote_url_is_not_usable(self) -> None:
        self.remote_cfg["url"] = "not-a-url"

        with mock.patch.object(cli, "_read_ada_default_api", return_value=None):
            self.assertIsNone(cli.resolve_api_url(None, self.remote_cfg))

    def test_ada_tokenfile_cmd_does_not_append_api(self) -> None:
        cmd = cli._ada_tokenfile_cmd("ada", Path("/tmp/token.conf"), "https://example.invalid/api/v1")

        self.assertEqual(cmd, ["ada", "--tokenfile", "/tmp/token.conf"])


class CliBundleExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)
        self.source_dir = self.root / "src"
        self.source_dir.mkdir(parents=True, exist_ok=True)
        (self.source_dir / "a.txt").write_text("aaaa\n", encoding="utf-8")
        self.config_path = self.root / "dcache.conf"
        self.config_path.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")
        self.config = cli.configparser.ConfigParser()
        self.config.read_string("[dcache]\ntype = webdav\n")

    def test_literal_download_file_list_skips_remote_enumeration(self) -> None:
        file_list = self.root / "downloads.tsv"
        file_list.write_text(
            "dcache:/input/a.cram\t"
            f"{self.root / 'downloads' / 'a.cram'}\n",
            encoding="utf-8",
        )

        with mock.patch.object(
            cli, "_rclone_lsjson"
        ) as list_remote, mock.patch.object(
            cli, "resolve_config_for_prefix", return_value=self.config_path
        ), mock.patch.object(
            cli, "load_rclone_config", return_value=self.config
        ), mock.patch.object(
            cli, "resolve_remote_name", return_value="dcache"
        ), mock.patch.object(
            cli, "resolve_api_url", return_value="https://example.invalid/api/v1"
        ):
            result = cli.main(
                [
                    "--file-list",
                    str(file_list),
                    "--literal-file-list",
                    "--dry-run",
                ]
            )

        self.assertEqual(result, 0)
        list_remote.assert_not_called()

    def test_main_executes_bundle_uploads_when_all_uploads_are_bundled(self) -> None:
        member = bundles.BundleMember(
            rel="a.txt",
            anchor_rel="a.txt",
            source=self.source_dir / "a.txt",
            resolved_source=self.source_dir / "a.txt",
            remote_path="dataset/a.txt",
            size=5,
            mtime_ns=0,
            mode=0o644,
            adler32="00000001",
        )
        bundle_plan = bundles.BundleUploadPlan(
            plain_entries=(),
            anchors=(
                bundles.AnchorBundlePlan(
                    anchor_dir="dataset",
                    generation="gen-1",
                    bundles=(),
                    reused_members=(member,),
                    route_map=(),
                    deprecations=(),
                    commit_required=True,
                ),
            ),
        )

        class _FakeBundleUploadResolver:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def _require_client(self):
                return mock.Mock()

        class _FakeTransferer:
            def __init__(self, *args, **kwargs) -> None:
                self.progress = None

            def close(self) -> None:
                return None

        with mock.patch.object(cli, "resolve_config_for_prefix", return_value=self.config_path), mock.patch.object(
            cli, "load_rclone_config", return_value=self.config
        ), mock.patch.object(
            cli, "resolve_remote_name", return_value="dcache"
        ), mock.patch.object(
            cli, "resolve_api_url", return_value="https://example.invalid/api/v1"
        ), mock.patch.object(
            cli, "_build_incremental_bundle_upload_plan", return_value=bundle_plan
        ), mock.patch.object(
            cli, "_BundleUploadResolver", _FakeBundleUploadResolver
        ), mock.patch.object(
            cli, "Transferer", _FakeTransferer
        ), mock.patch.object(
            cli, "_execute_bundle_uploads",
            side_effect=lambda plan, transferer, client, progress, bar, **kwargs: progress.success(
                "a.txt", 5, attempts=0, skipped=True
            ),
        ) as execute_bundle_uploads:
            rc = cli.main([
                "--bundle-small-files",
                "--bundle-min-dir-total",
                "1",
                "--bundle-max-file-size",
                "1024",
                "--bundle-target-size",
                "1024",
                "--bundle-max-members",
                "100",
                "-R",
                str(self.source_dir),
                "dcache:/dataset/",
            ])

        self.assertEqual(rc, 0)
        execute_bundle_uploads.assert_called_once()


class BundlePlanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)

    def _write_entry(self, relative_path: str, payload: bytes) -> dict:
        source = self.root / relative_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(payload)
        return {
            "source": source,
            "resolved_source": source,
            "rel": relative_path,
            "remote_path": f"dataset/{relative_path}",
            "size": len(payload),
        }

    def test_plan_bundle_uploads_replaces_fully_eligible_directory(self) -> None:
        entries = [
            self._write_entry("part-000/a.txt", b"aaaa"),
            self._write_entry("part-000/b.txt", b"bbbb"),
            self._write_entry("part-000/c.txt", b"cccc"),
        ]

        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )

        self.assertEqual(plan.plain_entries, ())
        self.assertEqual(len(plan.anchors), 1)
        self.assertEqual(plan.bundle_count, 1)
        self.assertEqual(plan.bundled_file_count, 3)
        anchor = plan.anchors[0]
        self.assertEqual(anchor.anchor_dir, "dataset/part-000")
        bundle = anchor.bundles[0]
        self.assertEqual(bundle.logical_file_count, 3)
        self.assertTrue(bundle.remote_path.startswith("dataset/part-000/.dcpacks/bundles/"))
        self.assertTrue(bundle.remote_path.endswith(".dcpbundle"))

    def test_plan_bundle_uploads_leaves_mixed_directory_plain(self) -> None:
        entries = [
            self._write_entry("part-001/a.txt", b"small"),
            self._write_entry("part-001/b.txt", b"this-is-too-large"),
        ]

        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=8,
                min_dir_total=1,
                max_members=100,
            ),
        )

        self.assertEqual(plan.bundle_count, 0)
        self.assertEqual(plan.anchors, ())
        self.assertEqual(len(plan.plain_entries), 2)

    def test_bundle_options_rejects_non_squashfs_format(self) -> None:
        with self.assertRaisesRegex(ValueError, "bundle format must be squashfs"):
            bundles.BundleOptions(format="tar").validate()

    def test_anchor_xattrs_round_trip_routes(self) -> None:
        entries = [
            self._write_entry("part-002/a.txt", b"aaaa"),
            self._write_entry("part-002/b.txt", b"bbbb"),
        ]
        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )

        anchor = plan.anchors[0]
        bundle = anchor.bundles[0]
        xattrs = bundles.build_anchor_xattrs(anchor)
        routes = bundles.decode_anchor_routes(xattrs)

        self.assertEqual(xattrs["dcache_cp.bundle_anchor.active_generation"], anchor.generation)
        self.assertEqual(routes, {"a.txt": bundle.bundle_id, "b.txt": bundle.bundle_id})

    @unittest.skipUnless(SQUASHFS_TOOLS_AVAILABLE, "squashfs tools required")
    def test_materialize_bundle_job_writes_manifest_and_members(self) -> None:
        entries = [
            self._write_entry("part-003/a.txt", b"aaaa"),
            self._write_entry("part-003/b.txt", b"bbbb"),
        ]
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
        self.addCleanup(Path(materialized["resolved_source"]).unlink, missing_ok=True)

        self.assertEqual(materialized["remote_path"], job.remote_path)
        self.assertFalse(materialized["bundle_keep_temp"])
        self.assertEqual(job.bundle_object_xattrs["dcache_cp.bundle.format"], "squashfs")

        extract_parent = Path(tempfile.mkdtemp(prefix="bundle-test-unsquashfs-"))
        self.addCleanup(shutil.rmtree, extract_parent, True)
        extract_root = extract_parent / "extract"
        self.addCleanup(shutil.rmtree, extract_root, True)
        result = subprocess.run(
            ["unsquashfs", "-no-progress", "-dest", str(extract_root), str(materialized["resolved_source"])],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        names = sorted(
            path.relative_to(extract_root).as_posix()
            for path in extract_root.rglob("*")
            if path.is_file()
        )
        self.assertEqual(names, [bundles.MANIFEST_ENTRY, "a.txt", "b.txt"])
        manifest = json.loads((extract_root / bundles.MANIFEST_ENTRY).read_text(encoding="utf-8"))

        self.assertEqual(manifest["bundle_id"], job.bundle_id)
        self.assertEqual(manifest["anchor_dir"], "dataset/part-003")
        self.assertEqual([member["path"] for member in manifest["members"]], ["a.txt", "b.txt"])

    def test_plan_download_source_with_bundles_resolves_logical_members(self) -> None:
        entries = [
            self._write_entry("part-005/a.txt", b"aaaa"),
            self._write_entry("part-005/b.txt", b"bbbb"),
        ]
        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )

        anchor = plan.anchors[0]
        job = anchor.bundles[0]

        class _ListXattrClient:
            def list_xattrs(self, remote_path: str) -> dict[str, str]:
                if remote_path == anchor.anchor_dir:
                    return bundles.build_anchor_xattrs(anchor)
                if remote_path == job.remote_path:
                    return job.bundle_object_xattrs
                return {}

        listing = [{
            "Path": ".dcpacks/bundles/" + Path(job.remote_path).name,
            "Size": 123,
            "IsDir": False,
        }]
        config = self.root / "dcache.conf"
        config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")
        resolver = cli._BundleDownloadResolver(config, "dcache", "https://example.invalid", None)
        resolver.xattr_client = _ListXattrClient()

        with mock.patch.object(cli, "_rclone_lsjson", return_value=listing), mock.patch.object(
            cli, "_rclone_stat", return_value={"IsDir": True}
        ):
            plain_entries, bundle_entries = cli._plan_download_source_with_bundles(
                config,
                "dcache",
                "dataset/part-005",
                self.root / "downloads",
                True,
                resolver,
            )

        self.assertEqual(plain_entries, [])
        self.assertEqual(len(bundle_entries), 1)
        bundle_entry = bundle_entries[0]
        self.assertEqual(bundle_entry["remote_path"], job.remote_path)
        self.assertEqual(bundle_entry["stage_size"], 123)
        self.assertEqual(bundle_entry["bundle_id"], job.bundle_id)
        self.assertEqual([member["rel"] for member in bundle_entry["bundle_members"]], ["a.txt", "b.txt"])
        self.assertEqual(
            [str(member["local_path"]) for member in bundle_entry["bundle_members"]],
            [str(self.root / "downloads" / "a.txt"), str(self.root / "downloads" / "b.txt")],
        )

    def test_plan_download_source_with_bundles_resolves_direct_logical_member(self) -> None:
        entries = [
            self._write_entry("delete-case/bundle/del1.bin", b"aaaa"),
            self._write_entry("delete-case/bundle/del2.bin", b"bbbb"),
        ]
        for entry in entries:
            entry["bundle_root"] = "dataset/delete-case/bundle"

        plan = cli._build_incremental_bundle_upload_plan(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
            type(
                "_Resolver",
                (),
                {
                    "anchor_state": staticmethod(lambda _anchor_dir: (None, {})),
                    "bundle_details": staticmethod(lambda _anchor_dir, _bundle_id: None),
                    "resolve_existing_route": staticmethod(lambda _remote_path: None),
                    "preferred_anchor_for_directory": staticmethod(
                        lambda remote_dir: "dataset/delete-case/bundle"
                        if remote_dir == "dataset/delete-case/bundle"
                        else None
                    ),
                },
            )(),
        )

        anchor = plan.anchors[0]
        job = anchor.bundles[0]

        class _ListXattrClient:
            def list_xattrs(self, remote_path: str) -> dict[str, str]:
                if remote_path == anchor.anchor_dir:
                    return bundles.build_anchor_xattrs(anchor)
                if remote_path == job.remote_path:
                    return job.bundle_object_xattrs
                return {}

        config = self.root / "dcache.conf"
        config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")
        resolver = cli._BundleDownloadResolver(config, "dcache", "https://example.invalid", None)
        resolver.xattr_client = _ListXattrClient()

        def fake_lsjson(
            _config: Path,
            _remote: str,
            remote_path: str,
            *,
            recursive: bool = False,
            missing_ok: bool = False,
        ) -> list[dict]:
            self.assertFalse(recursive)
            self.assertFalse(missing_ok)
            if remote_path == "dataset/delete-case/bundle/del1.bin":
                raise subprocess.CalledProcessError(
                    3,
                    ["rclone", "lsjson", f"dcache:{remote_path}"],
                    output="[\n",
                    stderr=(
                        "2026/04/22 14:52:44 ERROR : error listing: directory not found\n"
                        "2026/04/22 14:52:44 NOTICE: Failed to lsjson with 2 errors: "
                        "last error was: error in ListJSON: directory not found"
                    ),
                )
            if remote_path == "dataset/delete-case/bundle/.dcpacks/bundles":
                return [{"Path": Path(job.remote_path).name, "Size": 123, "IsDir": False}]
            self.fail(f"unexpected lsjson path: {remote_path}")

        local_dest = self.root / "downloads" / "del1.bin"
        with mock.patch.object(cli, "_rclone_lsjson", side_effect=fake_lsjson), mock.patch.object(
            cli, "_rclone_stat", side_effect=FileNotFoundError("logical member has no physical file")
        ):
            plain_entries, bundle_entries = cli._plan_download_source_with_bundles(
                config,
                "dcache",
                "dataset/delete-case/bundle/del1.bin",
                local_dest,
                False,
                resolver,
            )

        self.assertEqual(plain_entries, [])
        self.assertEqual(len(bundle_entries), 1)
        bundle_entry = bundle_entries[0]
        self.assertEqual(bundle_entry["remote_path"], job.remote_path)
        self.assertEqual([member["rel"] for member in bundle_entry["bundle_members"]], ["del1.bin"])
        self.assertEqual(bundle_entry["bundle_members"][0]["local_path"], local_dest)

    def test_plan_file_list_downloads_with_bundles_handles_virtual_parent_directory(self) -> None:
        bundled_entries = [
            self._write_entry("promoted/child1/p1.bin", b"aaaa"),
            self._write_entry("promoted/child1/p2.bin", b"bbbb"),
        ]
        plain_entry = self._write_entry("local-anchor/plain-large.bin", b"plain-data")
        plan = bundles.plan_bundle_uploads(
            bundled_entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )

        anchor = plan.anchors[0]
        job = anchor.bundles[0]

        class _ListXattrClient:
            def list_xattrs(self, remote_path: str) -> dict[str, str]:
                if remote_path == anchor.anchor_dir:
                    return bundles.build_anchor_xattrs(anchor)
                if remote_path == job.remote_path:
                    return job.bundle_object_xattrs
                return {}

        config = self.root / "dcache.conf"
        config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")
        resolver = cli._BundleDownloadResolver(config, "dcache", "https://example.invalid", None)
        resolver.xattr_client = _ListXattrClient()

        requested = [
            {
                "remote_path": plain_entry["remote_path"],
                "local_path": self.root / "downloads" / "plain-large.bin",
                "rel": "plain-large.bin",
            },
            {
                "remote_path": bundled_entries[0]["remote_path"],
                "local_path": self.root / "downloads" / "p1.bin",
                "rel": "p1.bin",
                "bundle_size": 123,
            },
        ]

        def fake_lsjson(
            _config: Path,
            _remote: str,
            remote_path: str,
            *,
            recursive: bool = False,
            missing_ok: bool = False,
        ) -> list[dict]:
            self.assertFalse(recursive)
            self.assertFalse(missing_ok)
            if remote_path == "dataset/local-anchor":
                return [{"Path": "plain-large.bin", "Size": len(b"plain-data"), "IsDir": False}]
            if remote_path == "dataset/promoted/child1":
                raise subprocess.CalledProcessError(
                    3,
                    ["rclone", "lsjson", f"dcache:{remote_path}"],
                    output="[\n",
                    stderr=(
                        "2026/04/22 14:39:04 ERROR : error listing: directory not found\n"
                        "2026/04/22 14:39:04 NOTICE: Failed to lsjson with 2 errors: "
                        "last error was: error in ListJSON: directory not found"
                    ),
                )
            self.fail(f"unexpected lsjson path: {remote_path}")

        with mock.patch.object(cli, "_rclone_lsjson", side_effect=fake_lsjson), mock.patch.object(
            cli, "_rclone_stat", side_effect=FileNotFoundError("logical member has no physical file")
        ):
            plain_entries, bundle_entries = cli._plan_file_list_downloads_with_bundles(
                config,
                "dcache",
                requested,
                resolver,
            )

        self.assertEqual(len(plain_entries), 1)
        self.assertEqual(plain_entries[0]["remote_path"], plain_entry["remote_path"])
        self.assertEqual(plain_entries[0]["size"], len(b"plain-data"))

        self.assertEqual(len(bundle_entries), 1)
        bundle_entry = bundle_entries[0]
        self.assertEqual(bundle_entry["remote_path"], job.remote_path)
        self.assertEqual(bundle_entry["bundle_id"], job.bundle_id)
        self.assertEqual(bundle_entry["stage_size"], 123)
        self.assertEqual([member["rel"] for member in bundle_entry["bundle_members"]], ["p1.bin"])

    def test_resolve_logical_entry_skips_deleted_bundle_member(self) -> None:
        entries = [
            self._write_entry("part-005-deleted/a.txt", b"aaaa"),
            self._write_entry("part-005-deleted/b.txt", b"bbbb"),
        ]
        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )
        anchor = plan.anchors[0]
        job = anchor.bundles[0]
        deleted_xattrs = bundles.build_bundle_deleted_xattrs({
            "a.txt": bundles.BundleDeletedRecord(
                anchor_rel="a.txt",
                deleted_generation="gen-delete",
                deleted_at="2026-04-22T12:00:00Z",
            )
        })

        class _ListXattrClient:
            def list_xattrs(self, remote_path: str) -> dict[str, str]:
                if remote_path == anchor.anchor_dir:
                    return bundles.build_anchor_xattrs(anchor)
                if remote_path == job.remote_path:
                    xattrs = dict(job.bundle_object_xattrs)
                    xattrs.update(deleted_xattrs)
                    return xattrs
                return {}

        config = self.root / "dcache.conf"
        config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")
        resolver = cli._BundleDownloadResolver(config, "dcache", "https://example.invalid", None)
        resolver.xattr_client = _ListXattrClient()

        resolved = resolver.resolve_logical_entry({
            "remote_path": "dataset/part-005-deleted/a.txt",
            "local_path": self.root / "downloads" / "a.txt",
            "rel": "a.txt",
            "bundle_size": 123,
        })

        self.assertIsNone(resolved)

    def test_resolve_logical_entry_falls_back_from_missing_virtual_child_anchor_to_parent_anchor(self) -> None:
        entries = [
            self._write_entry("promoted/child1/p1.bin", b"aaaa"),
            self._write_entry("promoted/child1/p2.bin", b"bbbb"),
        ]
        for entry in entries:
            entry["bundle_root"] = "dataset/promoted"

        plan = cli._build_incremental_bundle_upload_plan(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
            type(
                "_Resolver",
                (),
                {
                    "anchor_state": staticmethod(lambda _anchor_dir: (None, {})),
                    "bundle_details": staticmethod(lambda _anchor_dir, _bundle_id: None),
                    "resolve_existing_route": staticmethod(lambda _remote_path: None),
                    "preferred_anchor_for_directory": staticmethod(
                        lambda remote_dir: "dataset/promoted" if remote_dir == "dataset/promoted/child1" else None
                    ),
                },
            )(),
        )

        anchor = plan.anchors[0]
        job = anchor.bundles[0]

        class _ListXattrClient:
            def list_xattrs(self, remote_path: str) -> dict[str, str]:
                if remote_path == "dataset/promoted/child1":
                    raise NamespaceXattrError(
                        "GET",
                        "https://example.invalid/namespace/dataset%2Fpromoted%2Fchild1",
                        404,
                        '{"detail":"No such file or directory","title":"Not Found","status":"404"}',
                    )
                if remote_path == anchor.anchor_dir:
                    return bundles.build_anchor_xattrs(anchor)
                if remote_path == job.remote_path:
                    return job.bundle_object_xattrs
                return {}

        config = self.root / "dcache.conf"
        config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")
        resolver = cli._BundleDownloadResolver(config, "dcache", "https://example.invalid", None)
        resolver.xattr_client = _ListXattrClient()

        resolved = resolver.resolve_logical_entry(
            {
                "remote_path": "dataset/promoted/child1/p1.bin",
                "local_path": self.root / "downloads" / "p1.bin",
                "rel": "p1.bin",
                "bundle_size": 123,
            }
        )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved["anchor_dir"], "dataset/promoted")
        self.assertEqual(resolved["anchor_rel"], "child1/p1.bin")
        self.assertEqual(resolved["bundle_remote_path"], job.remote_path)

    def test_bundle_deleted_xattrs_round_trip_with_member_keys(self) -> None:
        deleted = {
            "a.txt": bundles.BundleDeletedRecord(
                anchor_rel="a.txt",
                deleted_generation="gen-a",
                deleted_at="2026-04-22T12:00:00Z",
            ),
            "child/b.txt": bundles.BundleDeletedRecord(
                anchor_rel="child/b.txt",
                deleted_generation="gen-b",
                deleted_at="2026-04-22T12:01:00Z",
            ),
        }

        encoded = bundles.build_bundle_deleted_xattrs(deleted)
        decoded = bundles.decode_bundle_deleted(encoded)

        self.assertEqual(decoded, deleted)
        self.assertTrue(any(key.startswith("dcache_cp.bundle.deleted.member.") for key in encoded))

    def test_incremental_bundle_upload_plan_reuses_remote_members_and_marks_deprecations(self) -> None:
        unchanged = self._write_entry("part-006/a.txt", b"aaaa")
        changed = self._write_entry("part-006/b.txt", b"bbbb")
        old_unchanged = bundles.build_bundle_member(unchanged)
        old_changed = bundles.build_bundle_member(changed)

        time.sleep(0.01)
        changed["source"].write_bytes(b"bbbbb-new")
        changed["size"] = changed["source"].stat().st_size

        old_bundle_id = "oldbundle0001"
        old_generation = "gen-old"

        class _Resolver:
            def anchor_state(self, anchor_dir: str) -> tuple[str | None, dict[str, str]]:
                self_anchor = "dataset/part-006"
                if anchor_dir == self_anchor:
                    return old_generation, {"a.txt": old_bundle_id, "b.txt": old_bundle_id}
                return None, {}

            def bundle_details(self, anchor_dir: str, bundle_id: str) -> dict[str, object] | None:
                if anchor_dir != "dataset/part-006" or bundle_id != old_bundle_id:
                    return None
                return {
                    "members": {
                        "a.txt": bundles.BundleMemberMetadata(
                            anchor_rel=old_unchanged.anchor_rel,
                            adler32=old_unchanged.adler32,
                            size=old_unchanged.size,
                            mtime_ns=old_unchanged.mtime_ns,
                            mode=old_unchanged.mode,
                        ),
                        "b.txt": bundles.BundleMemberMetadata(
                            anchor_rel=old_changed.anchor_rel,
                            adler32=old_changed.adler32,
                            size=old_changed.size,
                            mtime_ns=old_changed.mtime_ns,
                            mode=old_changed.mode,
                        ),
                    }
                }

            def resolve_existing_route(self, remote_path: str) -> dict[str, str] | None:
                remote_path = str(remote_path).strip("/")
                if remote_path == unchanged["remote_path"]:
                    return {"anchor_dir": "dataset/part-006", "anchor_rel": "a.txt", "bundle_id": old_bundle_id}
                if remote_path == changed["remote_path"]:
                    return {"anchor_dir": "dataset/part-006", "anchor_rel": "b.txt", "bundle_id": old_bundle_id}
                return None

            def preferred_anchor_for_directory(self, _remote_dir: str) -> str | None:
                return None

        plan = cli._build_incremental_bundle_upload_plan(
            [unchanged, changed],
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
            _Resolver(),
        )

        self.assertEqual(plan.plain_entries, ())
        self.assertEqual(plan.bundle_count, 1)
        self.assertEqual(plan.bundled_file_count, 2)
        self.assertEqual(plan.reused_file_count, 1)

        anchor = plan.anchors[0]
        self.assertEqual(anchor.anchor_dir, "dataset/part-006")
        self.assertTrue(anchor.commit_required)
        self.assertEqual([member.anchor_rel for member in anchor.reused_members], ["a.txt"])
        self.assertEqual(len(anchor.bundles), 1)
        self.assertNotEqual(anchor.generation, old_generation)

        new_bundle = anchor.bundles[0]
        route_map = dict(anchor.route_map)
        self.assertEqual(route_map["a.txt"], old_bundle_id)
        self.assertEqual(route_map["b.txt"], new_bundle.bundle_id)
        self.assertEqual(len(anchor.deprecations), 1)
        self.assertEqual(anchor.deprecations[0].bundle_id, old_bundle_id)
        self.assertEqual(anchor.deprecations[0].remote_path, bundles.bundle_object_remote_path(anchor.anchor_dir, old_bundle_id))
        self.assertEqual(anchor.deprecations[0].members[0].anchor_rel, "b.txt")
        self.assertEqual(anchor.deprecations[0].members[0].replacement_bundle_id, new_bundle.bundle_id)

    def test_plan_bundle_uploads_promotes_nested_files_only_within_transfer_root(self) -> None:
        entries = [
            self._write_entry("tree/sub/a.txt", b"aaaa"),
            self._write_entry("tree/sub/b.txt", b"bbbb"),
        ]
        for entry in entries:
            entry["bundle_root"] = "dataset/tree"

        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=10_000,
                max_members=100,
            ),
        )

        self.assertEqual(plan.plain_entries, ())
        self.assertEqual(plan.bundle_count, 1)
        anchor = plan.anchors[0]
        self.assertEqual(anchor.anchor_dir, "dataset/tree")
        self.assertEqual(
            [member.anchor_rel for member in anchor.bundles[0].members],
            ["sub/a.txt", "sub/b.txt"],
        )
        self.assertEqual(
            bundles.decode_anchor_routes(bundles.build_anchor_xattrs(anchor)),
            {
                "sub/a.txt": anchor.bundles[0].bundle_id,
                "sub/b.txt": anchor.bundles[0].bundle_id,
            },
        )

    def test_incremental_bundle_upload_plan_keeps_multiple_source_roots_separate(self) -> None:
        entries = [
            self._write_entry("archive/dir1/a.txt", b"aaaa"),
            self._write_entry("archive/dir1/b.txt", b"bbbb"),
            self._write_entry("archive/dir2/c.txt", b"cccc"),
            self._write_entry("archive/dir2/d.txt", b"dddd"),
        ]
        for entry in entries:
            remote_path = str(entry["remote_path"])
            if "/dir1/" in remote_path:
                entry["bundle_root"] = "dataset/archive/dir1"
            else:
                entry["bundle_root"] = "dataset/archive/dir2"

        class _Resolver:
            def anchor_state(self, _anchor_dir: str) -> tuple[str | None, dict[str, str]]:
                return None, {}

            def bundle_details(self, _anchor_dir: str, _bundle_id: str) -> dict[str, object] | None:
                return None

            def resolve_existing_route(self, _remote_path: str) -> dict[str, str] | None:
                return None

            def preferred_anchor_for_directory(self, remote_dir: str) -> str | None:
                if remote_dir in {"dataset/archive/dir1", "dataset/archive/dir2"}:
                    return "dataset/archive"
                return None

        plan = cli._build_incremental_bundle_upload_plan(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
            _Resolver(),
        )

        self.assertEqual(plan.plain_entries, ())
        self.assertEqual([anchor.anchor_dir for anchor in plan.anchors], ["dataset/archive/dir1", "dataset/archive/dir2"])
        self.assertEqual(plan.bundle_count, 2)

    def test_incremental_bundle_upload_plan_reuses_existing_parent_anchor_above_transfer_root(self) -> None:
        unchanged = self._write_entry("parent/child/a.txt", b"aaaa")
        new_entry = self._write_entry("parent/child/b.txt", b"bbbb")
        unchanged["bundle_root"] = "dataset/parent/child"
        new_entry["bundle_root"] = "dataset/parent/child"

        parent_anchor_dir = "dataset/parent"
        old_bundle_id = "parentbundle0001"
        old_member = bundles.build_bundle_member(unchanged, anchor_dir=parent_anchor_dir)

        class _Resolver:
            def anchor_state(self, anchor_dir: str) -> tuple[str | None, dict[str, str]]:
                if anchor_dir == "dataset/parent/child":
                    return None, {}
                if anchor_dir == parent_anchor_dir:
                    return "gen-parent", {"child/a.txt": old_bundle_id}
                return None, {}

            def bundle_details(self, anchor_dir: str, bundle_id: str) -> dict[str, object] | None:
                if anchor_dir != parent_anchor_dir or bundle_id != old_bundle_id:
                    return None
                return {
                    "members": {
                        "child/a.txt": bundles.BundleMemberMetadata(
                            anchor_rel=old_member.anchor_rel,
                            adler32=old_member.adler32,
                            size=old_member.size,
                            mtime_ns=old_member.mtime_ns,
                            mode=old_member.mode,
                        )
                    }
                }

            def resolve_existing_route(self, remote_path: str) -> dict[str, str] | None:
                if remote_path == unchanged["remote_path"]:
                    return {
                        "anchor_dir": parent_anchor_dir,
                        "anchor_rel": "child/a.txt",
                        "bundle_id": old_bundle_id,
                    }
                return None

            def preferred_anchor_for_directory(self, remote_dir: str) -> str | None:
                if remote_dir == "dataset/parent/child":
                    return parent_anchor_dir
                return None

        plan = cli._build_incremental_bundle_upload_plan(
            [unchanged, new_entry],
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=10_000,
                max_members=100,
            ),
            _Resolver(),
        )

        self.assertEqual(plan.plain_entries, ())
        self.assertEqual(plan.bundle_count, 1)
        self.assertEqual(plan.reused_file_count, 1)
        anchor = plan.anchors[0]
        self.assertEqual(anchor.anchor_dir, parent_anchor_dir)
        self.assertEqual([member.anchor_rel for member in anchor.reused_members], ["child/a.txt"])
        self.assertEqual(dict(anchor.route_map)["child/a.txt"], old_bundle_id)
        self.assertEqual(dict(anchor.route_map)["child/b.txt"], anchor.bundles[0].bundle_id)


class _FakeTransferer:
    def __init__(self) -> None:
        self.remote = "dcache"
        self.uploaded: list[str] = []
        self.deleted: list[str] = []

    def upload(self, entry: dict) -> dict:
        self.uploaded.append(entry["remote_path"])
        return {"attempt": 2, "remote_path": entry["remote_path"]}

    def _rclone_deletefile(self, target: str) -> None:
        self.deleted.append(target)


class _FakeXattrClient:
    def __init__(self, *, initial: dict[str, dict[str, str]] | None = None, fail_on: str | None = None) -> None:
        self.store = {path: dict(attrs) for path, attrs in (initial or {}).items()}
        self.fail_on = fail_on
        self.calls: list[str] = []

    def list_xattrs(self, remote_path: str) -> dict[str, str]:
        if remote_path not in self.store:
            raise NamespaceXattrError("GET", f"https://example.invalid/{remote_path}", 404, "not found")
        return dict(self.store[remote_path])

    def set_xattrs(self, remote_path: str, _attrs: dict[str, str]) -> None:
        self.calls.append(remote_path)
        if self.fail_on == remote_path:
            raise RuntimeError(f"xattr failure for {remote_path}")
        current = dict(self.store.get(remote_path, {}))
        current.update(_attrs)
        self.store[remote_path] = current


class _RaceyRouteXattrClient(_FakeXattrClient):
    def __init__(self, *, anchor_dir: str, stale_route_map: dict[str, str], initial: dict[str, dict[str, str]]) -> None:
        super().__init__(initial=initial)
        self.anchor_dir = anchor_dir
        self._stale_anchor_xattrs = bundles.build_anchor_xattrs_for_routes(stale_route_map, "gen-stale")
        self._overwrite_once = True

    def set_xattrs(self, remote_path: str, _attrs: dict[str, str]) -> None:
        super().set_xattrs(remote_path, _attrs)
        if (
            self._overwrite_once
            and remote_path == self.anchor_dir
            and "dcache_cp.bundle_anchor.active_generation" in _attrs
        ):
            self._overwrite_once = False
            current = dict(self.store.get(remote_path, {}))
            current.update(self._stale_anchor_xattrs)
            current["dcache_cp.bundle_anchor.active_generation"] = self._stale_anchor_xattrs[
                "dcache_cp.bundle_anchor.active_generation"
            ]
            self.store[remote_path] = current


class _FakeBar:
    def __init__(self) -> None:
        self.finished = 0
        self.updated: list[str] = []

    def finish(self) -> None:
        self.finished += 1

    def update(self, status: str | None = None) -> None:
        self.updated.append(status or "")


@unittest.skipUnless(SQUASHFS_TOOLS_AVAILABLE, "squashfs tools required")
class BundleExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)

    def _write_entry(self, relative_path: str, payload: bytes) -> dict:
        source = self.root / relative_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(payload)
        return {
            "source": source,
            "resolved_source": source,
            "rel": relative_path,
            "remote_path": f"dataset/{relative_path}",
            "size": len(payload),
        }

    def _build_plan(self) -> bundles.BundleUploadPlan:
        entries = [
            self._write_entry("part-004/a.txt", b"aaaa"),
            self._write_entry("part-004/b.txt", b"bbbb"),
        ]
        return bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        )

    def test_execute_bundle_uploads_marks_logical_success_after_anchor_commit(self) -> None:
        plan = self._build_plan()
        transferer = _FakeTransferer()
        xattrs = _FakeXattrClient()
        progress = cli.Progress(total_files=2, total_bytes=8)
        bar = _FakeBar()

        cli._execute_bundle_uploads(plan, transferer, xattrs, progress, bar, delete_source=False, keep_temp=False)

        bundle_remote = plan.anchors[0].bundles[0].remote_path
        self.assertEqual(transferer.uploaded, [bundle_remote])
        self.assertEqual(xattrs.calls, [bundle_remote, plan.anchors[0].anchor_dir, plan.anchors[0].anchor_dir])
        self.assertEqual(progress.validated_files, 2)
        self.assertEqual(progress.validated_bytes, 8)
        self.assertEqual(progress.failed, [])
        self.assertEqual(progress.total_retries, 1)
        self.assertEqual(transferer.deleted, [])
        self.assertEqual(bar.finished, 1)

    def test_execute_bundle_uploads_preserves_uploaded_bundle_on_anchor_failure(self) -> None:
        plan = self._build_plan()
        transferer = _FakeTransferer()
        xattrs = _FakeXattrClient(fail_on=plan.anchors[0].anchor_dir)
        progress = cli.Progress(total_files=2, total_bytes=8)
        bar = _FakeBar()

        with mock.patch.object(cli.LOG, "error"):
            cli._execute_bundle_uploads(plan, transferer, xattrs, progress, bar, delete_source=False, keep_temp=False)

        bundle_remote = plan.anchors[0].bundles[0].remote_path
        self.assertEqual(transferer.uploaded, [bundle_remote])
        self.assertEqual(transferer.deleted, [])
        self.assertEqual(progress.validated_files, 0)
        self.assertEqual(progress.validated_bytes, 0)
        self.assertEqual(len(progress.failed), 2)
        self.assertIn("xattr failure", progress.failed[0][1])
        self.assertEqual(bar.finished, 1)

    def test_execute_bundle_uploads_reuses_remote_bundle_and_deletes_sources_on_move(self) -> None:
        plan = self._build_plan()
        job = plan.anchors[0].bundles[0]
        transferer = _FakeTransferer()
        xattrs = _FakeXattrClient(initial={job.remote_path: job.bundle_object_xattrs})
        progress = cli.Progress(total_files=2, total_bytes=8)
        bar = _FakeBar()

        cli._execute_bundle_uploads(plan, transferer, xattrs, progress, bar, delete_source=True, keep_temp=False)

        self.assertEqual(transferer.uploaded, [])
        self.assertEqual(transferer.deleted, [])
        self.assertEqual(progress.validated_files, 2)
        self.assertEqual(progress.validated_bytes, 8)
        self.assertEqual(progress.skipped_files, 2)
        self.assertEqual(progress.skipped_bytes, 8)
        self.assertFalse(any(member.source.exists() for member in job.members))

    def test_execute_bundle_uploads_writes_deprecations_for_superseded_members(self) -> None:
        unchanged = self._write_entry("part-007/a.txt", b"aaaa")
        changed = self._write_entry("part-007/b.txt", b"bbbb")
        old_bundle = bundles.plan_bundle_uploads(
            [unchanged, changed],
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
        ).anchors[0].bundles[0]

        time.sleep(0.01)
        changed["source"].write_bytes(b"bbbb-new")
        changed["size"] = changed["source"].stat().st_size

        class _Resolver:
            def anchor_state(self, anchor_dir: str) -> tuple[str | None, dict[str, str]]:
                if anchor_dir == old_bundle.anchor_dir:
                    return old_bundle.generation, {
                        member.anchor_rel: old_bundle.bundle_id
                        for member in old_bundle.members
                    }
                return None, {}

            def bundle_details(self, anchor_dir: str, bundle_id: str) -> dict[str, object] | None:
                if anchor_dir != old_bundle.anchor_dir or bundle_id != old_bundle.bundle_id:
                    return None
                return {
                    "members": {
                        member.anchor_rel: bundles.BundleMemberMetadata(
                            anchor_rel=member.anchor_rel,
                            adler32=member.adler32,
                            size=member.size,
                            mtime_ns=member.mtime_ns,
                            mode=member.mode,
                        )
                        for member in old_bundle.members
                    }
                }

            def resolve_existing_route(self, remote_path: str) -> dict[str, str] | None:
                remote_path = str(remote_path).strip("/")
                member = next((member for member in old_bundle.members if member.remote_path == remote_path), None)
                if member is None:
                    return None
                return {
                    "anchor_dir": old_bundle.anchor_dir,
                    "anchor_rel": member.anchor_rel,
                    "bundle_id": old_bundle.bundle_id,
                }

            def preferred_anchor_for_directory(self, _remote_dir: str) -> str | None:
                return None

        plan = cli._build_incremental_bundle_upload_plan(
            [unchanged, changed],
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=1,
                max_members=100,
            ),
            _Resolver(),
        )

        transferer = _FakeTransferer()
        xattrs = _FakeXattrClient(initial={old_bundle.remote_path: old_bundle.bundle_object_xattrs})
        progress = cli.Progress(total_files=2, total_bytes=changed["size"] + unchanged["size"])
        bar = _FakeBar()

        cli._execute_bundle_uploads(plan, transferer, xattrs, progress, bar, delete_source=False, keep_temp=False)

        deprecated = bundles.decode_bundle_deprecated(xattrs.store[old_bundle.remote_path])
        self.assertIn("b.txt", deprecated)
        self.assertEqual(deprecated["b.txt"].replacement_bundle_id, plan.anchors[0].bundles[0].bundle_id)
        self.assertEqual(deprecated["b.txt"].replacement_generation, plan.anchors[0].generation)

    def test_commit_bundle_download_move_marks_members_deleted_without_deleting_partial_bundle(self) -> None:
        plan = self._build_plan()
        anchor = plan.anchors[0]
        job = anchor.bundles[0]
        member = job.members[0]
        transferer = _FakeTransferer()
        xattrs = _FakeXattrClient(initial={
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: job.bundle_object_xattrs,
        })
        entry = {
            "remote_path": job.remote_path,
            "bundle_anchor_dir": anchor.anchor_dir,
            "bundle_id": job.bundle_id,
        }
        result = {
            "remote_path": job.remote_path,
            "bundle_members": [{
                "rel": member.rel,
                "remote_path": member.remote_path,
                "local_path": self.root / "downloads" / member.anchor_rel,
                "anchor_rel": member.anchor_rel,
                "size": member.size,
                "adler32": member.adler32,
                "mtime_ns": member.mtime_ns,
                "mode": member.mode,
            }],
        }

        cli._commit_bundle_download_move(entry, result, xattrs, transferer)

        deleted = bundles.decode_bundle_deleted(xattrs.store[job.remote_path])
        routes = bundles.decode_anchor_routes(xattrs.store[anchor.anchor_dir])
        self.assertIn(member.anchor_rel, deleted)
        self.assertEqual(routes, {job.members[1].anchor_rel: job.bundle_id})
        self.assertEqual(transferer.deleted, [])

    def test_commit_bundle_download_move_refuses_anchor_mismatch(self) -> None:
        plan = self._build_plan()
        anchor = plan.anchors[0]
        job = anchor.bundles[0]
        transferer = _FakeTransferer()
        tampered_bundle_xattrs = dict(job.bundle_object_xattrs)
        tampered_bundle_xattrs["dcache_cp.bundle.anchor_dir"] = "dataset/other-anchor"
        xattrs = _FakeXattrClient(initial={
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: tampered_bundle_xattrs,
        })
        entry = {
            "remote_path": job.remote_path,
            "bundle_anchor_dir": anchor.anchor_dir,
            "bundle_id": job.bundle_id,
        }
        result = {
            "remote_path": job.remote_path,
            "bundle_members": [{
                "rel": job.members[0].rel,
                "remote_path": job.members[0].remote_path,
                "local_path": self.root / "downloads" / job.members[0].anchor_rel,
                "anchor_rel": job.members[0].anchor_rel,
                "size": job.members[0].size,
                "adler32": job.members[0].adler32,
                "mtime_ns": job.members[0].mtime_ns,
                "mode": job.members[0].mode,
            }],
        }

        with self.assertRaisesRegex(RuntimeError, "belongs to anchor"):
            cli._commit_bundle_download_move(entry, result, xattrs, transferer)

    def test_commit_bundle_download_move_deletes_bundle_only_when_all_members_retired(self) -> None:
        plan = self._build_plan()
        anchor = plan.anchors[0]
        job = anchor.bundles[0]
        transferer = _FakeTransferer()
        xattrs = _FakeXattrClient(initial={
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: job.bundle_object_xattrs,
        })
        entry = {
            "remote_path": job.remote_path,
            "bundle_anchor_dir": anchor.anchor_dir,
            "bundle_id": job.bundle_id,
        }
        result = {
            "remote_path": job.remote_path,
            "bundle_members": [
                {
                    "rel": member.rel,
                    "remote_path": member.remote_path,
                    "local_path": self.root / "downloads" / member.anchor_rel,
                    "anchor_rel": member.anchor_rel,
                    "size": member.size,
                    "adler32": member.adler32,
                    "mtime_ns": member.mtime_ns,
                    "mode": member.mode,
                }
                for member in job.members
            ],
        }

        with mock.patch.object(cli.LOG, "info"):
            cli._commit_bundle_download_move(entry, result, xattrs, transferer)

        routes = bundles.decode_anchor_routes(xattrs.store[anchor.anchor_dir])
        deleted = bundles.decode_bundle_deleted(xattrs.store[job.remote_path])
        self.assertEqual(routes, {})
        self.assertEqual(set(deleted), {member.anchor_rel for member in job.members})
        self.assertEqual(transferer.deleted, [f"dcache:{job.remote_path}"])

    def test_commit_bundle_download_move_retries_after_stale_route_publish(self) -> None:
        plan = self._build_plan()
        anchor = plan.anchors[0]
        job = anchor.bundles[0]
        member = job.members[0]
        transferer = _FakeTransferer()
        stale_route_map = {
            bundle_member.anchor_rel: job.bundle_id
            for bundle_member in job.members
        }
        xattrs = _RaceyRouteXattrClient(
            anchor_dir=anchor.anchor_dir,
            stale_route_map=stale_route_map,
            initial={
                anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
                job.remote_path: job.bundle_object_xattrs,
            },
        )
        entry = {
            "remote_path": job.remote_path,
            "bundle_anchor_dir": anchor.anchor_dir,
            "bundle_id": job.bundle_id,
        }
        result = {
            "remote_path": job.remote_path,
            "bundle_members": [{
                "rel": member.rel,
                "remote_path": member.remote_path,
                "local_path": self.root / "downloads" / member.anchor_rel,
                "anchor_rel": member.anchor_rel,
                "size": member.size,
                "adler32": member.adler32,
                "mtime_ns": member.mtime_ns,
                "mode": member.mode,
            }],
        }

        cli._commit_bundle_download_move(entry, result, xattrs, transferer)

        deleted = bundles.decode_bundle_deleted(xattrs.store[job.remote_path])
        routes = bundles.decode_anchor_routes(xattrs.store[anchor.anchor_dir])
        self.assertIn(member.anchor_rel, deleted)
        self.assertNotIn(member.anchor_rel, routes)
        self.assertEqual(routes, {job.members[1].anchor_rel: job.bundle_id})


if __name__ == "__main__":
    unittest.main()
