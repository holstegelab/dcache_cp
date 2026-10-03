from __future__ import annotations

import configparser
import io
import posixpath
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dcache_cp import bundles
from dcache_cp import ls


class _FakeXattrClient:
    def __init__(self, store: dict[str, dict[str, str]]) -> None:
        self.store = {path: dict(attrs) for path, attrs in store.items()}

    def list_xattrs(self, remote_path: str) -> dict[str, str]:
        return dict(self.store.get(remote_path, {}))


class BundleLsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)
        self.config = self.root / "dcache.conf"
        self.config.write_text("[dcache]\ntype = webdav\n", encoding="utf-8")

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

    def _make_parent_anchor_bundle(self):
        entries = [
            self._write_entry("root/child/a.txt", b"aaaa"),
            self._write_entry("root/child/b.txt", b"bbbb"),
        ]
        for entry in entries:
            entry["bundle_root"] = "dataset/root"
        plan = bundles.plan_bundle_uploads(
            entries,
            bundles.BundleOptions(
                target_size=1024,
                max_file_size=1024,
                min_dir_total=10_000,
                max_members=100,
            ),
        )
        anchor = plan.anchors[0]
        job = anchor.bundles[0]
        return anchor, job

    def test_bundle_groups_show_inherited_parent_bundle_members(self) -> None:
        anchor, job = self._make_parent_anchor_bundle()
        resolver = ls._BundleLsResolver(self.config, "https://example.invalid", None)
        resolver._client = _FakeXattrClient({
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: job.bundle_object_xattrs,
        })

        groups = resolver.bundle_groups_for_directory("dataset/root/child")

        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].display_path, posixpath.relpath(job.remote_path, "dataset/root/child"))
        self.assertEqual([member.name for member in groups[0].members], ["a.txt", "b.txt"])

    def test_bundle_groups_hide_deleted_members(self) -> None:
        anchor, job = self._make_parent_anchor_bundle()
        deleted_xattrs = bundles.build_bundle_deleted_xattrs({
            "child/a.txt": bundles.BundleDeletedRecord(
                anchor_rel="child/a.txt",
                deleted_generation="gen-delete",
                deleted_at="2026-04-22T12:00:00Z",
            )
        })
        resolver = ls._BundleLsResolver(self.config, "https://example.invalid", None)
        bundle_xattrs = dict(job.bundle_object_xattrs)
        bundle_xattrs.update(deleted_xattrs)
        resolver._client = _FakeXattrClient({
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: bundle_xattrs,
        })

        groups = resolver.bundle_groups_for_directory("dataset/root/child")

        self.assertEqual(len(groups), 1)
        self.assertEqual([member.name for member in groups[0].members], ["b.txt"])

    def test_bundle_groups_can_reveal_deleted_members(self) -> None:
        anchor, job = self._make_parent_anchor_bundle()
        deleted_xattrs = bundles.build_bundle_deleted_xattrs({
            "child/a.txt": bundles.BundleDeletedRecord(
                anchor_rel="child/a.txt",
                deleted_generation="gen-delete",
                deleted_at="2026-04-22T12:00:00Z",
            )
        })
        resolver = ls._BundleLsResolver(self.config, "https://example.invalid", None)
        bundle_xattrs = dict(job.bundle_object_xattrs)
        bundle_xattrs.update(deleted_xattrs)
        resolver._client = _FakeXattrClient({
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: bundle_xattrs,
        })

        groups = resolver.bundle_groups_for_directory("dataset/root/child", include_deleted=True)

        self.assertEqual(len(groups), 1)
        self.assertEqual([member.name for member in groups[0].members], ["b.txt", "a.txt"])
        self.assertEqual(groups[0].total_member_count, 2)
        self.assertEqual(groups[0].deleted_count, 1)

    def test_logical_member_for_path_resolves_bundled_file(self) -> None:
        anchor, job = self._make_parent_anchor_bundle()
        resolver = ls._BundleLsResolver(self.config, "https://example.invalid", None)
        resolver._client = _FakeXattrClient({
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: job.bundle_object_xattrs,
        })

        resolved = resolver.logical_member_for_path("dataset/root/child/a.txt")

        self.assertIsNotNone(resolved)
        group, member = resolved
        self.assertEqual(group.display_path, posixpath.relpath(job.remote_path, "dataset/root/child"))
        self.assertEqual(member.name, "a.txt")
        self.assertEqual(member.anchor_rel, "child/a.txt")

    def test_logical_member_for_path_hides_deleted_member_by_default(self) -> None:
        anchor, job = self._make_parent_anchor_bundle()
        deleted_xattrs = bundles.build_bundle_deleted_xattrs({
            "child/a.txt": bundles.BundleDeletedRecord(
                anchor_rel="child/a.txt",
                deleted_generation="gen-delete",
                deleted_at="2026-04-22T12:00:00Z",
            )
        })
        resolver = ls._BundleLsResolver(self.config, "https://example.invalid", None)
        bundle_xattrs = dict(job.bundle_object_xattrs)
        bundle_xattrs.update(deleted_xattrs)
        resolver._client = _FakeXattrClient({
            anchor.anchor_dir: bundles.build_anchor_xattrs(anchor),
            job.remote_path: bundle_xattrs,
        })

        hidden = resolver.logical_member_for_path("dataset/root/child/a.txt")
        shown = resolver.logical_member_for_path("dataset/root/child/a.txt", include_deleted=True)

        self.assertIsNone(hidden)
        self.assertIsNotNone(shown)
        _, member = shown
        self.assertTrue(member.is_deleted)

    def test_bundle_status_summary_excludes_deprecated_from_active_count(self) -> None:
        group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            total_member_count=5,
            deleted_count=1,
            deprecated_count=1,
        )

        summary = ls._bundle_status_summary(group)

        self.assertIn("3/5 active", summary)
        self.assertIn("1 deleted", summary)
        self.assertIn("1 deprecated", summary)

    def test_render_long_inlines_bundle_members_with_bundle_column(self) -> None:
        group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            locality="ONLINE",
            members=[
                ls._BundleMemberView(
                    name="a.txt",
                    logical_remote_path="dataset/root/child/a.txt",
                    anchor_rel="child/a.txt",
                    size=4,
                    mode=0o644,
                    mtime_ns=1_713_690_000_000_000_000,
                    adler32="00ab12cd",
                )
            ],
        )
        rows = ls._merge_rows([], [group], human=False)

        output = io.StringIO()
        ls._C.init(output)
        with redirect_stdout(output):
            ls._render_long(rows, show_pin=False, show_locality=True, show_checksum=False)

        rendered = output.getvalue()
        self.assertIn("a.txt", rendered)
        self.assertIn("bundle=1", rendered)
        self.assertIn("ONLINE", rendered)
        self.assertNotIn(".dcpacks", rendered)

    def test_render_long_bundle_path_flag_shows_full_bundle_path(self) -> None:
        group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            members=[
                ls._BundleMemberView(
                    name="a.txt",
                    logical_remote_path="dataset/root/child/a.txt",
                    anchor_rel="child/a.txt",
                    size=4,
                    mode=0o644,
                    mtime_ns=1_713_690_000_000_000_000,
                    adler32="00ab12cd",
                )
            ],
        )
        rows = ls._merge_rows([], [group], human=False)

        output = io.StringIO()
        ls._C.init(output)
        with redirect_stdout(output):
            ls._render_long(rows, show_pin=False, show_locality=False, show_checksum=False, show_bundle_path=True)

        rendered = output.getvalue()
        self.assertIn("bundle=dataset/root/.dcpacks/bundles/bundle-id.dcpbundle", rendered)

    def test_render_bundle_legend_lists_numbered_bundles(self) -> None:
        group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            locality="NEARLINE",
        )

        output = io.StringIO()
        ls._C.init(output)
        with redirect_stdout(output):
            ls._render_bundle_legend([group])

        rendered = output.getvalue()
        self.assertIn("bundles:", rendered)
        self.assertIn("1", rendered)
        self.assertIn("bundle bundle-id", rendered)
        self.assertIn("NEARLINE", rendered)

    def _make_fake_resolver(self, member: ls._BundleMemberView) -> mock.Mock:
        fake_resolver = mock.Mock()
        fake_group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            members=[member],
        )
        fake_resolver.logical_member_for_path.return_value = None
        fake_resolver.bundle_groups_for_directory.return_value = [fake_group]
        return fake_resolver

    def test_main_falls_back_to_bundle_groups_for_bundle_only_directory(self) -> None:
        parser = configparser.ConfigParser()
        parser.read_string("[dcache]\ntype = webdav\n")
        fake_resolver = self._make_fake_resolver(ls._BundleMemberView(
            name="a.txt",
            logical_remote_path="dataset/root/child/a.txt",
            anchor_rel="child/a.txt",
            size=4,
            mode=0o644,
            mtime_ns=0,
            adler32="00ab12cd",
        ))

        output = io.StringIO()
        with mock.patch.object(sys, "argv", ["dcache_ls", "-l", "dcache:/dataset/root/child"]), mock.patch.object(
            ls, "resolve_config_for_prefix", return_value=self.config
        ), mock.patch.object(
            ls, "load_rclone_config", return_value=parser
        ), mock.patch.object(
            ls, "resolve_remote_name", return_value="dcache"
        ), mock.patch.object(
            ls, "resolve_api_url", return_value="https://example.invalid/api/v1"
        ), mock.patch.object(
            ls, "_BundleLsResolver", return_value=fake_resolver
        ), mock.patch.object(
            ls,
            "_list_path",
            side_effect=RuntimeError("Error while getting information about '/dataset/root/child': No such file or directory"),
        ), redirect_stdout(output):
            rc = ls.main()

        self.assertEqual(rc, 0)
        fake_resolver.bundle_groups_for_directory.assert_called_once_with("dataset/root/child", include_deleted=False)
        rendered = output.getvalue()
        self.assertIn("a.txt", rendered)
        self.assertIn("bundle=1", rendered)

    def test_main_falls_back_to_bundle_groups_when_ada_stat_is_non_json(self) -> None:
        parser = configparser.ConfigParser()
        parser.read_string("[dcache]\ntype = webdav\n")
        fake_resolver = self._make_fake_resolver(ls._BundleMemberView(
            name="a.txt",
            logical_remote_path="dataset/root/child/a.txt",
            anchor_rel="child/a.txt",
            size=4,
            mode=0o644,
            mtime_ns=0,
            adler32="00ab12cd",
        ))

        output = io.StringIO()
        with mock.patch.object(sys, "argv", ["dcache_ls", "-l", "dcache:/dataset/root/child"]), mock.patch.object(
            ls, "resolve_config_for_prefix", return_value=self.config
        ), mock.patch.object(
            ls, "load_rclone_config", return_value=parser
        ), mock.patch.object(
            ls, "resolve_remote_name", return_value="dcache"
        ), mock.patch.object(
            ls, "resolve_api_url", return_value="https://example.invalid/api/v1"
        ), mock.patch.object(
            ls, "_BundleLsResolver", return_value=fake_resolver
        ), mock.patch.object(
            ls,
            "_list_path",
            side_effect=RuntimeError("ada --stat returned non-JSON output for /dataset/root/child.\nstdout: ''\nstderr: ''\nUpgrade ada or set --ada to point to the bundled version."),
        ), redirect_stdout(output):
            rc = ls.main()

        self.assertEqual(rc, 0)
        fake_resolver.bundle_groups_for_directory.assert_called_once_with("dataset/root/child", include_deleted=False)
        rendered = output.getvalue()
        self.assertIn("a.txt", rendered)

    def test_main_no_bundles_treats_virtual_bundle_directory_as_empty_physical_listing(self) -> None:
        parser = configparser.ConfigParser()
        parser.read_string("[dcache]\ntype = webdav\n")
        fake_resolver = mock.Mock()
        fake_group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            members=[
                ls._BundleMemberView(
                    name="a.txt",
                    logical_remote_path="dataset/root/child/a.txt",
                    anchor_rel="child/a.txt",
                    size=4,
                    mode=0o644,
                    mtime_ns=0,
                    adler32="00ab12cd",
                )
            ],
        )
        fake_resolver.logical_member_for_path.return_value = None
        fake_resolver.bundle_groups_for_directory.return_value = [fake_group]

        output = io.StringIO()
        with mock.patch.object(sys, "argv", ["dcache_ls", "-l", "--no-bundles", "dcache:/dataset/root/child"]), mock.patch.object(
            ls, "resolve_config_for_prefix", return_value=self.config
        ), mock.patch.object(
            ls, "load_rclone_config", return_value=parser
        ), mock.patch.object(
            ls, "resolve_remote_name", return_value="dcache"
        ), mock.patch.object(
            ls, "resolve_api_url", return_value="https://example.invalid/api/v1"
        ), mock.patch.object(
            ls, "_BundleLsResolver", return_value=fake_resolver
        ), mock.patch.object(
            ls,
            "_list_path",
            side_effect=RuntimeError("ada --stat returned non-JSON output for /dataset/root/child.\nstdout: ''\nstderr: 'curl: (22) The requested URL returned error: 404'\nUpgrade ada or set --ada to point to the bundled version."),
        ), redirect_stdout(output):
            rc = ls.main()

        self.assertEqual(rc, 0)
        self.assertEqual(output.getvalue(), "")
        fake_resolver.bundle_groups_for_directory.assert_called_once_with("dataset/root/child", include_deleted=False)

    def test_main_show_deleted_bundles_reveals_deleted_members(self) -> None:
        parser = configparser.ConfigParser()
        parser.read_string("[dcache]\ntype = webdav\n")
        fake_resolver = mock.Mock()
        fake_group = ls._BundleGroupView(
            bundle_remote_path="dataset/root/.dcpacks/bundles/bundle-id.dcpbundle",
            display_path="../.dcpacks/bundles/bundle-id.dcpbundle",
            members=[
                ls._BundleMemberView(
                    name="a.txt",
                    logical_remote_path="dataset/root/child/a.txt",
                    anchor_rel="child/a.txt",
                    size=4,
                    mode=0o644,
                    mtime_ns=0,
                    adler32="00ab12cd",
                    is_deleted=True,
                )
            ],
        )
        fake_resolver.logical_member_for_path.return_value = None
        fake_resolver.bundle_groups_for_directory.return_value = [fake_group]

        with mock.patch.object(sys, "argv", ["dcache_ls", "-l", "--show-deleted-bundles", "dcache:/dataset/root/child"]), mock.patch.object(
            ls, "resolve_config_for_prefix", return_value=self.config
        ), mock.patch.object(
            ls, "load_rclone_config", return_value=parser
        ), mock.patch.object(
            ls, "resolve_remote_name", return_value="dcache"
        ), mock.patch.object(
            ls, "resolve_api_url", return_value="https://example.invalid/api/v1"
        ), mock.patch.object(
            ls, "_BundleLsResolver", return_value=fake_resolver
        ), mock.patch.object(
            ls,
            "_list_path",
            side_effect=RuntimeError("Error while getting information about '/dataset/root/child': No such file or directory"),
        ), redirect_stdout(io.StringIO()):
            rc = ls.main()

        self.assertEqual(rc, 0)
        fake_resolver.bundle_groups_for_directory.assert_called_once_with("dataset/root/child", include_deleted=True)


if __name__ == "__main__":
    unittest.main()
