import json
import subprocess
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from dcache_cp import cli


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory()
        self.addCleanup(self.workspace.cleanup)
        self.root = Path(self.workspace.name)
        self.config = self.root / "rclone.conf"
        self.config.write_text("[dcache]\ntype = webdav\nurl = https://example.invalid/\n")
        self.entries = {
            "run42": [{"Path": "sample.txt", "Size": 7, "IsDir": False}],
            "run42/sample.txt": [{"Path": "sample.txt", "Size": 7, "IsDir": False}],
            "run42/other.txt": [{"Path": "other.txt", "Size": 5, "IsDir": False}],
        }
        self.directories = {"run42"}
        self.checksum = format(zlib.adler32(b"sample\n") & 0xffffffff, "08x")
        cache = patch.object(cli, "_checksum_cache")
        self.cache = cache.start()
        self.cache.get.return_value = None
        self.addCleanup(cache.stop)
        command = patch.object(cli, "run_command", side_effect=self.remote_command)
        self.run_command = command.start()
        self.addCleanup(command.stop)
        ada = patch.object(cli, "_default_ada", return_value="ada")
        ada.start()
        self.addCleanup(ada.stop)

    def remote_command(self, command, check=True):
        # Model rclone's different lsjson shapes for --stat and listings.
        if "lsjson" not in command:
            self.fail(f"unexpected remote command: {command}")
        target = command[command.index("lsjson") + 1]
        remote_path = target.partition(":")[2]
        if remote_path not in self.entries:
            raise subprocess.CalledProcessError(3, command, stderr="not found")
        if "--stat" in command:
            payload = {
                "Path": Path(remote_path).name,
                "IsDir": remote_path in self.directories,
                "Size": -1 if remote_path in self.directories else self.entries[remote_path][0]["Size"],
            }
        else:
            payload = self.entries[remote_path]
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")

    def test_single_file_keeps_remote_path_and_exact_destination(self):
        target = self.root / "renamed.txt"
        plan = cli.plan_download(self.config, "dcache", "/run42/sample.txt", target, False)
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["remote_path"], "run42/sample.txt")
        self.assertEqual(plan[0]["local_path"], target)
        self.assertEqual(plan[0]["size"], 7)

    def test_single_file_uses_existing_directory(self):
        target = self.root / "downloads"
        target.mkdir()
        plan = cli.plan_download(self.config, "dcache", "run42/sample.txt", target, False)
        self.assertEqual(plan[0]["remote_path"], "run42/sample.txt")
        self.assertEqual(plan[0]["local_path"], target / "sample.txt")

    def test_single_file_uses_new_directory_with_trailing_slash(self):
        target = self.root / "downloads"
        plan = cli.plan_download(self.config, "dcache", "run42/sample.txt", str(target) + "/", False)
        self.assertEqual(plan[0]["local_path"], target / "sample.txt")
        self.assertFalse(target.exists())

    def test_recursive_directory_keeps_relative_paths(self):
        self.entries["run42"].extend([
            {"Path": "nested", "Size": -1, "IsDir": True},
            {"Path": "nested/other.txt", "Size": 5, "IsDir": False},
        ])
        target = self.root / "downloads"
        plan = cli.plan_download(self.config, "dcache", "run42/", target, True)
        self.assertEqual([e["remote_path"] for e in plan], ["run42/sample.txt", "run42/nested/other.txt"])
        self.assertEqual([e["local_path"] for e in plan], [target / "sample.txt", target / "nested/other.txt"])

    def test_directory_named_like_its_child_is_not_treated_as_a_file(self):
        self.directories.add("run42/sample.txt")
        target = self.root / "downloads"
        plan = cli.plan_download(self.config, "dcache", "run42/sample.txt", target, True)
        self.assertEqual(plan[0]["remote_path"], "run42/sample.txt/sample.txt")
        self.assertEqual(plan[0]["local_path"], target / "sample.txt")

    def test_missing_source_propagates_listing_error(self):
        with self.assertRaises(subprocess.CalledProcessError):
            cli.plan_download(self.config, "dcache", "run42/missing.txt", self.root / "output", False)

    def test_multiple_file_sources_use_new_destination_directory(self):
        plans = []
        original = cli.plan_download

        def capture(*args, **kwargs):
            plan = original(*args, **kwargs)
            plans.extend(plan)
            return plan

        target = self.root / "downloads"
        with patch.object(cli, "plan_download", side_effect=capture):
            status = cli.main([
                "--config", str(self.config), "--dry-run",
                "dcache:/run42/sample.txt", "dcache:/run42/other.txt", str(target),
            ])
        self.assertEqual(status, 0)
        self.assertEqual([e["remote_path"] for e in plans], ["run42/sample.txt", "run42/other.txt"])
        self.assertEqual([e["local_path"] for e in plans], [target / "sample.txt", target / "other.txt"])
        self.assertFalse(target.exists())

    def matching_local_copy(self):
        target = self.root / "downloads"
        target.mkdir()
        (target / "sample.txt").write_bytes(b"sample\n")
        return target

    def assert_download_dry_run_is_read_only(self, paths, *, delete_source):
        before = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        with patch.object(cli.Transferer, "_remote_adler", return_value=self.checksum) as checksum, \
                patch.object(cli.Transferer, "_rclone_deletefile") as delete, \
                patch.object(cli, "_execute_simple") as transfer, \
                patch.object(cli, "StageManager") as stage:
            status = cli.main(
                ["--config", str(self.config), "--dry-run", *paths],
                prog="dcache_mv" if delete_source else "dcache_cp",
                delete_source=delete_source,
            )
        self.assertEqual(status, 0)
        delete.assert_not_called()
        checksum.assert_not_called()
        transfer.assert_not_called()
        stage.assert_not_called()
        self.cache.put.assert_not_called()
        after = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(after, before)

    def test_move_download_dry_run_preserves_matching_remote_source(self):
        target = self.matching_local_copy()
        self.assert_download_dry_run_is_read_only(["-R", "dcache:/run42/", str(target)], delete_source=True)
        self.assertEqual((target / "sample.txt").read_bytes(), b"sample\n")

    def test_move_file_list_dry_run_preserves_matching_remote_source(self):
        target = self.matching_local_copy()
        transfers = self.root / "transfers.tsv"
        transfers.write_text(f"dcache:/run42/sample.txt\t{target / 'sample.txt'}\n")
        self.assert_download_dry_run_is_read_only(["--file-list", str(transfers)], delete_source=True)

    def test_copy_download_dry_run_does_not_verify_or_modify_files(self):
        target = self.matching_local_copy()
        self.assert_download_dry_run_is_read_only(["-R", "dcache:/run42/", str(target)], delete_source=False)

    def test_upload_move_dry_run_preserves_local_source(self):
        source = self.root / "sample.txt"
        source.write_bytes(b"sample\n")
        self.assert_download_dry_run_is_read_only([str(source), "dcache:/run42/sample.txt"], delete_source=True)
        self.assertEqual(source.read_bytes(), b"sample\n")

    def test_real_move_still_deletes_verified_remote_source(self):
        target = self.matching_local_copy()
        with patch.object(cli.Transferer, "_remote_adler", return_value=self.checksum), \
                patch.object(cli.Transferer, "_rclone_deletefile") as delete:
            status = cli.main([
                "--config", str(self.config), "--no-stage", "-R", "dcache:/run42/", str(target),
            ], prog="dcache_mv", delete_source=True)
        self.assertEqual(status, 0)
        delete.assert_called_once_with("dcache:run42/sample.txt")


if __name__ == "__main__":
    unittest.main()
