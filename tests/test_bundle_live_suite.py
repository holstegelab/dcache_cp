from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "dcache_bundle_live_suite.py"
SPEC = importlib.util.spec_from_file_location("dcache_bundle_live_suite", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
live_suite = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = live_suite
SPEC.loader.exec_module(live_suite)


class SparseToolDetectionTests(unittest.TestCase):
    def test_detect_sparse_tooling_reports_available_when_unsquashfs_and_unmount_helper_exist(self) -> None:
        def fake_which(name: str) -> str | None:
            mapping = {
                "unsquashfs": "/usr/bin/unsquashfs",
                "fusermount3": "/usr/bin/fusermount3",
            }
            return mapping.get(name)

        with mock.patch.object(live_suite.shutil, "which", side_effect=fake_which):
            result = live_suite._detect_sparse_tooling()

        self.assertTrue(result["available"])
        self.assertEqual(result["missing"], [])

    def test_detect_sparse_tooling_still_reports_available_when_sqfscat_is_missing(self) -> None:
        def fake_which(name: str) -> str | None:
            mapping = {
                "unsquashfs": "/usr/bin/unsquashfs",
                "fusermount3": "/usr/bin/fusermount3",
            }
            return mapping.get(name)

        with mock.patch.object(live_suite.shutil, "which", side_effect=fake_which):
            result = live_suite._detect_sparse_tooling()

        self.assertTrue(result["available"])
        self.assertEqual(result["missing"], [])

    def test_detect_sparse_tooling_reports_missing_unmount_helper(self) -> None:
        def fake_which(name: str) -> str | None:
            mapping = {
                "sqfscat": "/usr/bin/sqfscat",
                "unsquashfs": "/usr/bin/unsquashfs",
            }
            return mapping.get(name)

        with mock.patch.object(live_suite.shutil, "which", side_effect=fake_which):
            result = live_suite._detect_sparse_tooling()

        self.assertFalse(result["available"])
        self.assertEqual(result["missing"], ["fusermount3|fusermount|umount"])

    def test_detect_sparse_tooling_reports_missing_unsquashfs(self) -> None:
        def fake_which(name: str) -> str | None:
            mapping = {
                "sqfscat": "/usr/bin/sqfscat",
                "fusermount3": "/usr/bin/fusermount3",
            }
            return mapping.get(name)

        with mock.patch.object(live_suite.shutil, "which", side_effect=fake_which):
            result = live_suite._detect_sparse_tooling()

        self.assertFalse(result["available"])
        self.assertEqual(result["missing"], ["unsquashfs"])


class ParentAnchorDatasetTests(unittest.TestCase):
    def test_parent_anchor_rerun_preserves_metadata_for_reused_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            initial_root = root / "initial"
            rerun_root = root / "rerun"

            live_suite._write_parent_anchor_initial(initial_root)
            live_suite._write_parent_anchor_rerun(rerun_root, initial_root / "promoted" / "child1")

            for name in ("p1.bin", "p2.bin"):
                source = initial_root / "promoted" / "child1" / name
                copied = rerun_root / name
                self.assertEqual(source.read_bytes(), copied.read_bytes())
                self.assertEqual(source.stat().st_mtime_ns, copied.stat().st_mtime_ns)
                self.assertEqual(source.stat().st_mode & 0o777, copied.stat().st_mode & 0o777)


class LiveSuiteOutputTests(unittest.TestCase):
    def test_delete_case_move_rerun_accepts_already_moved_resume(self) -> None:
        self.assertTrue(
            live_suite._delete_case_move_rerun_resumed_cleanly(
                "skip del2.bin (already moved)\nskip del3.bin (already moved)\nno files to process\n"
            )
        )


if __name__ == "__main__":
    unittest.main()
