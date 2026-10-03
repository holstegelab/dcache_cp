import shutil
import tempfile
import threading
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from dcache_cp import cli


class LiteralFileListTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        self.remote = self.root / "remote"
        self.remote.mkdir()
        self.source = self.remote / "input" / "source.txt"
        self.source.parent.mkdir()
        self.source.write_bytes(b"NEW_DATA")
        self.target = self.root / "downloads" / "renamed.txt"
        self.config = self.root / "rclone.conf"
        self.config.write_text(f"[dcache]\ntype=alias\nremote={self.remote}\n")
        self.manifest = self.root / "downloads.tsv"
        self.manifest.write_text(f"dcache:/input/source.txt\t{self.target}\n")
        cache = cli._ChecksumCache.__new__(cli._ChecksumCache)
        cache._lock, cache._data, cache._dirty = threading.Lock(), {}, set()
        cache._load = lambda path: {}
        for mocked in (
            patch.object(cli, "_checksum_cache", cache),
            patch.object(cli, "resolve_pool_for_config", return_value=None),
        ):
            mocked.start()
            self.addCleanup(mocked.stop)

    def arguments(self, *extra):
        return [
            "--config", str(self.config), "--file-list", str(self.manifest),
            "--literal-file-list", "--ada", "unused-ada",
            "--max-retries", "0", "--retry-wait", "0", *extra,
        ]

    def remote_checksum(self, path):
        data = (self.remote / path).read_bytes()
        return format(zlib.adler32(data) & 0xffffffff, "08x")

    def test_literal_dry_run_skips_remote_calls_and_preserves_destination(self):
        self.target.parent.mkdir()
        self.target.write_bytes(b"EXISTING")
        with patch.object(cli, "run_command") as command, \
                patch.object(cli, "Transferer") as transfer, \
                patch.object(cli, "StageManager") as stage:
            self.assertEqual(cli.main(self.arguments("--dry-run")), 0)
        command.assert_not_called()
        transfer.assert_not_called()
        stage.assert_not_called()
        self.assertEqual(self.target.read_bytes(), b"EXISTING")

    def test_normal_file_list_still_lists_parent_directory(self):
        arguments = self.arguments("--dry-run")
        arguments.remove("--literal-file-list")
        listing = [{"Path": "source.txt", "Size": 8, "IsDir": False}]
        with patch.object(cli, "_rclone_lsjson", return_value=listing) as enumerate_remote:
            self.assertEqual(cli.main(arguments), 0)
        enumerate_remote.assert_called_once_with(self.config, "dcache", "input", recursive=False, missing_ok=False)

    def test_invalid_modes_fail_before_resolving_config(self):
        upload = self.root / "upload.tsv"
        upload.write_text(f"{self.source}\tdcache:/output.txt\n")
        cases = [
            (["--literal-file-list", "dcache:/input/source.txt", str(self.target)], False),
            (["--literal-file-list", "--file-list", str(upload)], False),
            (self.arguments("--dry-run"), True),
        ]
        for arguments, moving in cases:
            with self.subTest(arguments=arguments, moving=moving), \
                    patch.object(cli, "resolve_config_for_prefix") as config:
                self.assertEqual(cli.main(arguments, delete_source=moving), 1)
                config.assert_not_called()

    def test_literal_plan_still_rejects_duplicate_destinations(self):
        with self.manifest.open("a") as handle:
            handle.write(f"dcache:/input/other.txt\t{self.target}\n")
        with patch.object(cli, "run_command") as command:
            with self.assertRaises(ValueError):
                cli.main(self.arguments("--dry-run"))
        command.assert_not_called()

    @unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
    def test_literal_download_verifies_and_installs_exact_target_without_listing(self):
        with patch.object(cli, "_rclone_lsjson") as enumerate_remote, \
                patch.object(cli, "_rclone_stat") as stat_remote, \
                patch.object(cli.Transferer, "_remote_adler", self.remote_checksum):
            self.assertEqual(cli.main(self.arguments("--no-stage")), 0)
        enumerate_remote.assert_not_called()
        stat_remote.assert_not_called()
        self.assertEqual(self.target.read_bytes(), b"NEW_DATA")
        self.assertEqual(self.source.read_bytes(), b"NEW_DATA")
        self.assertEqual(list(self.root.rglob(".dcache-cp-*.part")), [])

    @unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
    def test_literal_checksum_mismatch_keeps_existing_destination(self):
        self.target.parent.mkdir()
        self.target.write_bytes(b"EXISTING")
        with patch.object(cli.Transferer, "_remote_adler", return_value="00000001"):
            self.assertEqual(cli.main(self.arguments("--no-stage", "--no-skip-verified")), 1)
        self.assertEqual(self.target.read_bytes(), b"EXISTING")
        self.assertEqual(self.source.read_bytes(), b"NEW_DATA")
        self.assertEqual(list(self.root.rglob(".dcache-cp-*.part")), [])

    @unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
    def test_literal_missing_source_fails_and_keeps_existing_destination(self):
        self.source.unlink()
        self.target.parent.mkdir()
        self.target.write_bytes(b"EXISTING")
        with patch.object(cli.Transferer, "_remote_adler") as checksum:
            self.assertEqual(cli.main(self.arguments("--no-stage", "--no-skip-verified")), 1)
        checksum.assert_not_called()
        self.assertEqual(self.target.read_bytes(), b"EXISTING")
        self.assertEqual(list(self.root.rglob(".dcache-cp-*.part")), [])

    @unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
    def test_literal_resume_skips_verified_destination(self):
        self.target.parent.mkdir()
        self.target.write_bytes(b"NEW_DATA")
        with patch.object(cli.Transferer, "_remote_adler", self.remote_checksum), \
                patch.object(cli.Transferer, "_rclone_copyto") as copy, \
                patch.object(cli, "StageManager") as stage:
            self.assertEqual(cli.main(self.arguments()), 0)
        copy.assert_not_called()
        stage.assert_not_called()
        self.assertEqual(self.source.read_bytes(), b"NEW_DATA")


if __name__ == "__main__":
    unittest.main()
