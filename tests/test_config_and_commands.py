import configparser
import io
import json
import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from dcache_cp import cli, ls


class ConfigAndCommandTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        self.config = self.root / "rclone.conf"
        self.config.write_text("[first]\ntype=webdav\nurl=https://example.invalid/\nbearer_token=FAKE_FIRST\n"
                               "[second]\ntype=webdav\nurl=https://example.invalid/project%20data/\nbearer_token=FAKE_SECOND\n")

    def test_encoded_url_is_read_without_interpolation(self):
        cfg = cli.load_rclone_config(self.config)
        stage = cli.StageManager("ada", self.config, None, cfg["second"])
        self.assertEqual(stage.webdav_url, "https://example.invalid/project%20data/")

    def test_ada_receives_only_selected_remote_in_private_temporary_file(self):
        cfg = cli.load_rclone_config(self.config)
        captured = []
        def command(args, **kwargs):
            filename = Path(args[args.index("--tokenfile") + 1])
            selected = cli.load_rclone_config(filename)
            self.assertEqual(selected.sections(), ["dcache"])
            self.assertEqual(selected["dcache"]["bearer_token"], "FAKE_SECOND")
            self.assertEqual(selected["dcache"]["url"], cfg["second"]["url"])
            self.assertEqual(filename.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("FAKE_FIRST", filename.read_text())
            self.assertFalse(any("FAKE_SECOND" in arg for arg in args))
            captured.append(filename)
            return subprocess.CompletedProcess(args, 0, "{}", "")
        with patch.object(cli, "run_command", side_effect=command):
            cli.run_ada("ada", self.config, None, ["--stat", "/file"], remote="second")
        self.assertFalse(captured[0].exists())

    def test_ada_token_command_is_supported_without_logging_credential(self):
        command = shlex.join([sys.executable, "-c", 'print("FAKE_COMMAND_TOKEN")'])
        self.config.write_text(f"[dcache]\ntype=webdav\nurl=https://example.invalid/\nbearer_token_command={command}\n")
        with self.assertLogs("dcache_cp", level="DEBUG") as logs:
            with cli._ada_tokenfile(self.config) as (filename, token):
                self.assertEqual(token, "FAKE_COMMAND_TOKEN")
                self.assertIn("bearer_token", filename.read_text())
                cli.LOG.debug("test token resolution completed")
        self.assertNotIn("FAKE_COMMAND_TOKEN", "\n".join(logs.output))

    def test_fallback_keeps_credential_out_of_argv(self):
        cfg = cli.load_rclone_config(self.config)
        manager = cli.StageManager("ada", self.config, None, cfg["second"])
        captured = []
        def curl(args, **kwargs):
            self.assertFalse(any("FAKE_SECOND" in arg for arg in args))
            filename = Path(args[args.index("--header") + 1].removeprefix("@"))
            self.assertEqual(filename.stat().st_mode & 0o777, 0o600)
            self.assertEqual(filename.read_text(), "Authorization: Bearer FAKE_SECOND\n")
            captured.append(filename)
            return subprocess.CompletedProcess(args, 0, "HTTP/2 206\n206", "")
        with patch.object(cli, "run_command", side_effect=curl):
            manager.prime_via_webdav_range({"remote_path": "file"})
        self.assertFalse(captured[0].exists())

    def test_run_command_redacts_supplied_secrets_in_logs_and_errors(self):
        command = [sys.executable, "-c", 'import sys; print("FAKE_SECRET"); sys.exit(1)']
        with self.assertLogs("dcache_cp", level="DEBUG") as logs:
            with self.assertRaises(subprocess.CalledProcessError) as error:
                cli.run_command(command, secrets=("FAKE_SECRET",), quiet=True)
            cli.LOG.debug("private command finished")
        self.assertNotIn("FAKE_SECRET", error.exception.stdout)
        self.assertNotIn("FAKE_SECRET", "\n".join(logs.output))

    def test_bulk_api_uses_selected_credentials_in_a_private_header_file(self):
        cfg = cli.load_rclone_config(self.config)
        manager = cli.StageManager("ada", self.config, None, cfg["second"])
        captured = []
        def curl(args, **kwargs):
            self.assertFalse(any("FAKE_SECOND" in arg for arg in args))
            header = Path(args[args.index("--header") + 1].removeprefix("@"))
            body = Path(args[args.index("--data-binary") + 1].removeprefix("@"))
            self.assertEqual(header.stat().st_mode & 0o777, 0o600)
            self.assertIn("Authorization: Bearer FAKE_SECOND\n", header.read_text())
            self.assertNotIn("FAKE_FIRST", header.read_text())
            self.assertEqual(json.loads(body.read_text()), {"target": ['/a "quoted" file']})
            captured.extend([header, body])
            return subprocess.CompletedProcess(args, 0, "{}", "")
        with patch.object(cli, "run_command", side_effect=curl):
            manager._api_request("https://example.invalid/api/v1/bulk-requests", method="POST",
                                 data={"target": ['/a "quoted" file']})
        self.assertTrue(all(not path.exists() for path in captured))

    def test_command_deadline_terminates_blocking_process(self):
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            cli.run_command([sys.executable, "-c", "import time; time.sleep(20)"], timeout=0.05)
        self.assertLess(time.monotonic() - start, 2)

    def test_active_command_responds_to_cancellation(self):
        cancel = threading.Event()
        errors = []
        def run():
            try:
                cli.run_command([sys.executable, "-c", "import time; time.sleep(20)"], timeout=None, cancel_event=cancel)
            except cli.TransferCancelled as exc:
                errors.append(exc)
        thread = threading.Thread(target=run)
        thread.start()
        time.sleep(0.05)
        cancel.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)

    def fake_ada(self):
        ada = self.root / "ada"
        ada.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(20)\nprint('ADLER32=00000001')\n")
        ada.chmod(0o700)
        return str(ada)

    def test_checksum_deadline_bounds_actual_ada_call(self):
        transfer = cli.Transferer(self.config, "second", self.fake_ada(), None, 0, 0, "1s", checksum_timeout=0.05)
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            transfer._remote_adler("file")
        self.assertLess(time.monotonic() - start, 2)

    def test_stage_deadline_bounds_actual_ada_call(self):
        config = cli.load_rclone_config(self.config)
        stage = cli.StageManager(self.fake_ada(), self.config, None, config["second"])
        stage.deadline = time.monotonic() + 0.05
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            stage._stat_json("file")
        self.assertLess(time.monotonic() - start, 2)

    def test_mixed_positional_prefixes_rejected_before_configuration(self):
        with patch.object(cli, "resolve_config_for_prefix") as config:
            status = cli.main(["--dry-run", "first:/a", "second:/b", str(self.root)])
        self.assertEqual(status, 1)
        config.assert_not_called()

    def test_colliding_upload_destinations_rejected_before_move(self):
        first, second = self.root / "a/same", self.root / "b/same"
        first.parent.mkdir()
        second.parent.mkdir()
        first.write_bytes(b"FIRST")
        second.write_bytes(b"SECOND")
        with patch.object(cli, "Transferer") as transfer:
            with self.assertRaisesRegex(ValueError, "same destination"):
                cli.main(["--config", str(self.config), "--remote", "second", str(first), str(second), "dcache:/output/"], delete_source=True)
        transfer.assert_not_called()
        self.assertEqual(first.read_bytes(), b"FIRST")
        self.assertEqual(second.read_bytes(), b"SECOND")

    def test_colliding_download_destinations_rejected(self):
        files = [dict(remote_path="a", local_path=self.root / "same"), dict(remote_path="b", local_path=self.root / "same")]
        with self.assertRaisesRegex(ValueError, "same destination"):
            cli._validate_transfer_plan(files, "download", True)

    def test_overlapping_destination_paths_rejected(self):
        files = [dict(remote_path="a", local_path=self.root / "file"), dict(remote_path="b", local_path=self.root / "file/child")]
        with self.assertRaisesRegex(ValueError, "overlapping"):
            cli._validate_transfer_plan(files, "download")

    def test_overlap_is_found_with_an_intervening_sibling_name(self):
        files = [dict(remote_path=name, local_path=self.root / name)
                 for name in ("file", "file-extra", "file/child")]
        with self.assertRaisesRegex(ValueError, "overlapping"):
            cli._validate_transfer_plan(files, "download")

    def test_quoted_tsv_uses_the_parsed_prefix(self):
        manifest = self.root / "input.tsv"
        manifest.write_text(f'"dcache:/file"\t{self.root / "output"}\n')
        with patch.object(cli, "resolve_config_for_prefix", return_value=self.config) as resolve, \
                patch.object(cli, "_fill_file_list_download_sizes", side_effect=lambda config, remote, files, **kw: files):
            status = cli.main(["--remote", "second", "--file-list", str(manifest), "--dry-run"])
        self.assertEqual(status, 0)
        self.assertEqual(resolve.call_args.args[0], "dcache")

    def test_extra_tsv_columns_rejected(self):
        manifest = self.root / "input.tsv"
        manifest.write_text(f"dcache:/file\t{self.root / 'output'}\textra\n")
        with self.assertRaisesRegex(ValueError, "expected 2"):
            cli.load_file_list(manifest)

    def test_help_does_not_resolve_or_download_ada(self):
        with patch.object(cli, "_default_ada") as default, patch.object(ls, "_default_ada") as ls_default:
            for parser in (cli.build_parser(), ls.build_parser()):
                with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as exit_code:
                    parser.parse_args(["--help"])
                self.assertEqual(exit_code.exception.code, 0)
        default.assert_not_called()
        ls_default.assert_not_called()

    def test_explicit_ada_does_not_resolve_default(self):
        source = self.root / "source"
        source.write_bytes(b"DATA")
        def completed(files, worker, workers, progress, bar):
            for entry in files:
                progress.success(entry["rel"], entry["size"])
        with patch.object(cli, "_default_ada") as default, patch.object(cli, "_execute_simple", side_effect=completed):
            status = cli.main(["--config", str(self.config), "--remote", "second", "--ada", "chosen-ada", str(source), "dcache:/output"])
        self.assertEqual(status, 0)
        default.assert_not_called()

    def test_incomplete_execution_returns_failure(self):
        source = self.root / "source"
        source.write_bytes(b"DATA")
        with patch.object(cli, "_execute_simple"), patch.object(cli, "print_summary"):
            status = cli.main(["--config", str(self.config), "--remote", "second", "--ada", "ada", str(source), "dcache:/output"])
        self.assertEqual(status, 1)

    def test_listing_single_child_checksums_child_path(self):
        payload = {"fileType": "DIR", "children": [{"fileType": "REGULAR", "fileName": "child", "size": 4}]}
        with patch.object(ls, "_ada_stat", return_value=payload), patch.object(ls, "_ada_checksum", return_value="00000001") as checksum:
            rows = ls._list_path("ada", self.config, None, "directory", human=False, show_pin=False, show_checksum=True, remote="second")
        self.assertEqual(rows[0].checksum, "00000001")
        self.assertEqual(checksum.call_args.args[3:], ("directory/child", "second"))

    def test_listing_passes_selected_remote_to_ada(self):
        with patch.object(ls, "run_ada", return_value=subprocess.CompletedProcess([], 0, "{}", "")) as run:
            ls._ada_stat("ada", self.config, None, "file", "second")
        self.assertEqual(run.call_args.kwargs["remote"], "second")

    def test_listing_human_readable_example(self):
        args = ls.build_parser().parse_args(["-lH", "dcache:/directory"])
        self.assertTrue(args.long)
        self.assertTrue(args.human_readable)
