import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dcache_cp import cli


@unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
class RclonePlanningTests(unittest.TestCase):
    """Exercise real rclone metadata without network access or archive writes."""

    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory()
        self.addCleanup(self.workspace.cleanup)
        self.root = Path(self.workspace.name)
        self.remote = self.root / "remote"
        (self.remote / "run42/nested").mkdir(parents=True)
        self.sample = self.remote / "run42/sample.txt"
        self.sample.write_bytes(b"sample\n")
        (self.remote / "run42/nested/other.txt").write_bytes(b"other\n")
        self.config = self.root / "rclone.conf"
        self.config.write_text(f"[dcache]\ntype = alias\nremote = {self.remote}\n")

    def test_real_rclone_single_file_and_directory_metadata(self):
        target = self.root / "renamed.txt"
        plan = cli.plan_download(self.config, "dcache", "run42/sample.txt", target, False)
        self.assertEqual(plan[0]["remote_path"], "run42/sample.txt")
        self.assertEqual(plan[0]["local_path"], target)
        self.assertEqual(plan[0]["size"], 7)
        directory = self.root / "downloads"
        plan = cli.plan_download(self.config, "dcache", "run42", directory, True)
        self.assertEqual(
            {e["remote_path"]: e["local_path"] for e in plan},
            {
                "run42/sample.txt": directory / "sample.txt",
                "run42/nested/other.txt": directory / "nested/other.txt",
            },
        )
        self.assertFalse(target.exists())
        self.assertFalse(directory.exists())

    def test_real_rclone_move_dry_run_leaves_matching_source_and_destination(self):
        destination = self.root / "copy.txt"
        destination.write_bytes(self.sample.read_bytes())
        with patch.object(cli, "_default_ada", return_value="ada"), \
                patch.object(cli, "adler32_local", side_effect=AssertionError("dry run must not hash files")):
            status = cli.main([
                "--config", str(self.config), "--dry-run",
                "dcache:/run42/sample.txt", str(destination),
            ], prog="dcache_mv", delete_source=True)
        self.assertEqual(status, 0)
        self.assertEqual(self.sample.read_bytes(), b"sample\n")
        self.assertEqual(destination.read_bytes(), b"sample\n")


if __name__ == "__main__":
    unittest.main()
