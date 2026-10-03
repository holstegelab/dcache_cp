import json
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dcache_cp import cli


class FakeStage:
    def __init__(self, *, fallback=False, initial_failure=False, online=True):
        self.cancel_event = threading.Event()
        self.deadline = None
        self.fallback = fallback
        self.initial_failure = initial_failure
        self.initial_online = online
        self.available = set()
        self.active = set()
        self.peak = 0
        self.stage_calls = []
        self.unstage_calls = []
        self.fallback_calls = []
        self.failures = {}
        self.poll_exception = None
        self.fallback_exception = None
        self.unstage_exception = None

    def cancel(self):
        self.cancel_event.set()

    def can_prime_via_webdav_range(self):
        return self.fallback

    def stage(self, paths, lifetime):
        self.stage_calls.append(list(paths))
        if self.initial_failure:
            raise subprocess.CalledProcessError(1, ["ada", "--stage"], stderr="not permitted to stage")
        self.active.update(paths)
        self.peak = max(self.peak, len(self.active))
        if self.initial_online:
            self.available.update(paths)
        return ["request-id"]

    def unstage(self, paths):
        self.unstage_calls.append(list(paths))
        if self.unstage_exception:
            raise self.unstage_exception
        self.active.difference_update(paths)

    def poll_stage_request_errors(self, requests, pending):
        return dict(self.failures)

    def poll_online_statuses(self, paths):
        if self.poll_exception:
            raise self.poll_exception
        return [p for p in paths if p in self.available], {}

    def prime_via_webdav_range(self, entry):
        path = "/" + entry["remote_path"].strip("/")
        self.fallback_calls.append(path)
        if self.fallback_exception:
            raise self.fallback_exception
        self.available.add(path)
        return dict(remote_path=path, timed_out=True)


class FakeTransfer:
    delete_source = False

    def download(self, entry):
        entry["local_path"].write_bytes(b"CONTENT")
        return dict(rel=entry["rel"], size=entry["size"], skipped=False, attempt=1)


class StagingSafetyTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)

    def entries(self, count=1):
        return [dict(remote_path=f"source-{i}", local_path=self.root / f"output-{i}", rel=f"source-{i}", size=10)
                for i in range(count)]

    def execute(self, entries, stage, *, max_files=2, max_bytes=20, destage=True, timeout=2, transfer=None):
        progress = cli.Progress(len(entries), sum(e["size"] for e in entries))
        cli._execute_pipeline_download(entries, transfer or FakeTransfer(), stage, 2, progress, Mock(),
                                       max_files, max_bytes, "7D", 0, timeout, destage)
        return progress

    def test_repeated_source_creates_every_requested_output(self):
        entries = self.entries(2)
        entries[1]["remote_path"] = entries[0]["remote_path"]
        stage = FakeStage()
        progress = self.execute(entries, stage)
        self.assertEqual(stage.stage_calls, [["/source-0"]])
        self.assertEqual(progress.validated_files, 2)
        self.assertFalse(progress.failed)
        for entry in entries:
            self.assertEqual(entry["local_path"].read_bytes(), b"CONTENT")

    def test_duplicate_source_move_rejected_before_staging(self):
        entries = self.entries(2)
        entries[1]["remote_path"] = entries[0]["remote_path"]
        stage, transfer = FakeStage(), FakeTransfer()
        transfer.delete_source = True
        with self.assertRaisesRegex(ValueError, "move source appears more than once"):
            self.execute(entries, stage, transfer=transfer)
        self.assertEqual(stage.stage_calls, [])

    def test_batch_limits_bound_aggregate_pins(self):
        stage = FakeStage()
        progress = self.execute(self.entries(4), stage)
        self.assertEqual(progress.validated_files, 4)
        self.assertEqual(len(stage.stage_calls), 2)
        self.assertEqual(stage.peak, 2)
        self.assertEqual(stage.active, set())

    def bundle_entry(self):
        members = [dict(remote_path=f"logical-{i}", local_path=self.root / f"output-{i}",
                        rel=f"logical-{i}", size=2) for i in range(2)]
        return dict(remote_path=".dcpacks/bundles/example.dcpbundle", rel="bundle:example",
                    size=4, stage_size=40, bundle_members=members)

    def test_bundle_download_stages_physical_object_and_commits_every_member(self):
        entry = self.bundle_entry()
        stage, committed = FakeStage(), []
        progress = cli.Progress(2, 4)

        def download_bundle(bundle):
            for member in bundle["bundle_members"]:
                member["local_path"].write_bytes(b"OK")
            return dict(remote_path=bundle["remote_path"], bundle_members=bundle["bundle_members"],
                        skipped=False, attempt=1)

        cli._execute_pipeline_download(
            [entry], FakeTransfer(), stage, 1, progress, Mock(), 1, 40, "7D", 0, 2, True,
            worker_fn=download_bundle, result_handler=lambda entry, result: committed.append(result),
        )
        self.assertEqual(stage.stage_calls, [["/.dcpacks/bundles/example.dcpbundle"]])
        self.assertEqual(stage.unstage_calls, stage.stage_calls)
        self.assertEqual(stage.active, set())
        self.assertEqual(progress.validated_files, 2)
        self.assertFalse(progress.failed)
        self.assertEqual(len(committed), 1)
        for member in entry["bundle_members"]:
            self.assertEqual(member["local_path"].read_bytes(), b"OK")

    def test_bundle_batch_limit_uses_physical_bytes(self):
        stage = FakeStage()
        with self.assertRaisesRegex(ValueError, "exceeds --stage-batch-bytes"):
            self.execute([self.bundle_entry()], stage, max_bytes=10)
        self.assertEqual(stage.stage_calls, [])

    def test_no_destage_limit_includes_plain_and_bundle_physical_bytes(self):
        entry = self.bundle_entry()
        files = [entry, dict(remote_path="plain", local_path=self.root / "plain", rel="plain", size=10)]
        with self.assertRaisesRegex(ValueError, "--no-destage"):
            cli._validate_retained_stage_limits(files, 2, 45)
        cli._validate_retained_stage_limits(files, 2, 50)

    def test_plain_destination_cannot_overlap_bundle_member_destination(self):
        entry = self.bundle_entry()
        plain = dict(remote_path="plain", local_path=entry["bundle_members"][0]["local_path"])
        with self.assertRaisesRegex(ValueError, "same destination"):
            cli._validate_transfer_plan([entry, plain], "download")

    def test_initial_stage_failure_uses_fallback(self):
        stage = FakeStage(fallback=True, initial_failure=True, online=False)
        progress = self.execute(self.entries(), stage)
        self.assertEqual(stage.fallback_calls, ["/source-0"])
        self.assertEqual(progress.validated_files, 1)
        self.assertFalse(progress.failed)

    def test_request_failure_uses_fallback(self):
        stage = FakeStage(fallback=True, online=False)
        stage.failures = {"/source-0": "stage request failed"}
        progress = self.execute(self.entries(), stage)
        self.assertEqual(stage.fallback_calls, ["/source-0"])
        self.assertEqual(progress.validated_files, 1)

    def test_failed_fallback_reports_failure_for_every_output(self):
        entries = self.entries(2)
        entries[1]["remote_path"] = entries[0]["remote_path"]
        stage = FakeStage(fallback=True, initial_failure=True, online=False)
        stage.fallback_exception = RuntimeError("fallback failed")
        progress = self.execute(entries, stage)
        self.assertEqual(len(progress.failed), 2)
        self.assertEqual(progress.validated_files, 0)
        self.assertFalse(any(e["local_path"].exists() for e in entries))

    def test_no_destage_keeps_pins_on_success(self):
        stage = FakeStage()
        self.execute(self.entries(2), stage, destage=False)
        self.assertEqual(stage.active, {"/source-0", "/source-1"})
        self.assertEqual(stage.unstage_calls, [])

    def test_no_destage_keeps_pins_on_error(self):
        stage = FakeStage()
        stage.poll_exception = RuntimeError("poll failed")
        with self.assertRaises(RuntimeError):
            self.execute(self.entries(), stage, destage=False)
        self.assertEqual(stage.unstage_calls, [])
        self.assertEqual(stage.active, {"/source-0"})

    def test_no_destage_rejects_plan_exceeding_retained_pin_limits(self):
        stage = FakeStage()
        with self.assertRaisesRegex(ValueError, "--no-destage"):
            self.execute(self.entries(3), stage, destage=False)
        self.assertEqual(stage.stage_calls, [])

    def test_oversized_file_rejected_before_staging(self):
        stage = FakeStage()
        with self.assertRaisesRegex(ValueError, "exceeds --stage-batch-bytes"):
            self.execute(self.entries(), stage, max_bytes=9)
        self.assertEqual(stage.stage_calls, [])

    def test_pin_release_failure_prevents_next_batch(self):
        stage = FakeStage()
        stage.unstage_exception = RuntimeError("release failed")
        with self.assertRaisesRegex(RuntimeError, "refusing to stage more"):
            self.execute(self.entries(4), stage)
        self.assertEqual(len(stage.stage_calls), 1)
        self.assertEqual(stage.active, {"/source-0", "/source-1"})

    def test_staging_timeout_records_failure(self):
        stage = FakeStage(online=False)
        start = time.monotonic()
        progress = self.execute(self.entries(), stage, timeout=0.05)
        self.assertLess(time.monotonic() - start, 1)
        self.assertEqual(len(progress.failed), 1)
        self.assertEqual(progress.validated_files, 0)
        self.assertEqual(stage.active, set())

    def test_cancelled_pool_does_not_run_queued_work(self):
        started, release = threading.Event(), threading.Event()
        ran = []
        def worker(entry):
            ran.append(entry["id"])
            if entry["id"] == 1:
                started.set()
                release.wait(2)
            return entry
        pool = cli._DaemonWorkerPool(worker, 1, name_prefix="cancel-test")
        try:
            pool.submit({"id": 1})
            self.assertTrue(started.wait(2))
            pool.submit({"id": 2})
            pool.submit({"id": 3})
            pool.stop()
        finally:
            release.set()
            pool.join(2)
        self.assertEqual(ran, [1])


class BulkCompletionTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        config = self.root / "rclone.conf"
        config.write_text("[dcache]\ntype=webdav\nurl=https://example.invalid/\nbearer_token=FAKE\n")
        self.stage = cli.StageManager("ada", config, None, cli.load_rclone_config(config)["dcache"])
        self.pin = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        self.unpin = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        self.url = "https://example.invalid/api/v1/bulk-requests/"
        self.stage._run_ada = Mock(return_value=subprocess.CompletedProcess([], 0, "request-url: " + self.url + self.pin, ""))
        self.stage._stat_json = Mock(return_value=({"children": [{"fileName": "source", "fileLocality": "ONLINE"}]}, None))

    def status(self, request, state, target_state=None, **extra):
        return dict(uid=request, status=state, nextId=-1,
                    targets=[dict(target="/source", state=target_state or state)], **extra)

    def test_online_file_is_not_ready_until_its_pin_completes(self):
        self.stage.stage(["/source"])
        with patch.object(self.stage, "_api_request", side_effect=[
            json.dumps(self.status(self.pin, "QUEUED", "CREATED")),
            json.dumps(self.status(self.pin, "STARTED", "COMPLETED")),
        ]):
            self.assertEqual(self.stage.poll_stage_request_errors([self.pin], {"/source"}), {})
            self.assertEqual(self.stage.poll_online_statuses(["/source"])[0], [])
            self.stage.poll_stage_request_errors([self.pin], {"/source"})
            self.assertEqual(self.stage.poll_online_statuses(["/source"])[0], ["/source"])

    def test_uuid_in_filename_is_not_treated_as_an_owned_request(self):
        self.stage._run_ada.return_value.stdout = '["/' + self.unpin + '.bam"]\nrequest-url: ' + self.url + self.pin
        self.assertEqual(self.stage.stage(["/" + self.unpin + ".bam"]), [self.pin])
        self.assertEqual(set(self.stage._pin_requests), {self.pin})

    def test_wait_online_polls_the_pending_pin(self):
        self.stage.stage(["/source"])
        with patch.object(self.stage, "_api_request", side_effect=[
            json.dumps(self.status(self.pin, "QUEUED", "CREATED")),
            json.dumps(self.status(self.pin, "COMPLETED")),
        ]):
            self.assertEqual(self.stage.wait_online(["/source"], poll_interval=0, timeout=2), ["/source"])

    def test_wait_one_online_polls_the_pending_pin(self):
        self.stage.stage(["/source"])
        with patch.object(self.stage, "_api_request", side_effect=[
            json.dumps(self.status(self.pin, "QUEUED", "CREATED")),
            json.dumps(self.status(self.pin, "COMPLETED")),
        ]):
            self.assertEqual(self.stage.wait_one_online(["/source"], set(), poll_interval=0, timeout=2), "/source")

    def test_unpin_is_scoped_and_must_complete_before_cleanup_returns(self):
        self.stage.stage(["/source"])
        posts = []
        unpin_polls = []
        def api(url, method="GET", data=None, **kwargs):
            if method == "POST":
                self.assertEqual(data["activity"], "UNPIN")
                self.assertEqual(data["arguments"], {"id": self.pin})
                self.assertEqual(data["target"], ["/source"])
                posts.append(data)
                return "request-url: " + self.url + self.unpin
            if url.endswith(self.pin):
                return json.dumps(self.status(self.pin, "COMPLETED"))
            unpin_polls.append(url)
            return json.dumps(self.status(self.unpin, "QUEUED" if len(unpin_polls) == 1 else "COMPLETED"))
        self.stage._pending_pins.clear()
        with patch.object(self.stage, "_api_request", side_effect=api), patch.object(cli.time, "sleep"):
            self.stage.unstage(["/source"])
        self.assertEqual(len(posts), 1)
        self.assertEqual(len(unpin_polls), 2)
        self.assertEqual(self.stage._pin_requests, {})
        self.assertEqual(self.stage._run_ada.call_count, 1)  # No unscoped ADA unstage.

    def test_fallback_does_not_release_another_jobs_pins(self):
        with patch.object(self.stage, "_api_request") as api:
            self.stage.unstage(["/source"])
        api.assert_not_called()
        self.stage._run_ada.assert_not_called()

    def test_unconfirmed_pin_is_cancelled_before_unpin(self):
        self.stage.stage(["/source"])
        methods = []
        def api(url, method="GET", data=None, **kwargs):
            methods.append(method)
            if method == "PATCH":
                self.assertEqual(data, {"action": "cancel"})
                return "{}"
            if method == "POST":
                return "request-url: " + self.url + self.unpin
            return json.dumps(self.status(self.pin, "CANCELLED") if url.endswith(self.pin)
                              else self.status(self.unpin, "COMPLETED"))
        with patch.object(self.stage, "_api_request", side_effect=api):
            self.stage.unstage(["/source"])
        self.assertEqual(methods, ["PATCH", "GET", "POST", "GET"])

    def test_failed_unpin_is_not_reported_as_released(self):
        self.stage.stage(["/source"])
        self.stage._pending_pins.clear()
        def api(url, method="GET", **kwargs):
            if method == "POST":
                return "request-url: " + self.url + self.unpin
            return json.dumps(self.status(self.pin, "COMPLETED") if url.endswith(self.pin)
                              else self.status(self.unpin, "COMPLETED", "FAILED"))
        with patch.object(self.stage, "_api_request", side_effect=api), self.assertRaisesRegex(RuntimeError, "failed"):
            self.stage.unstage(["/source"])
        self.assertIn(self.pin, self.stage._pin_requests)

    def test_missing_unpin_target_is_not_reported_as_released(self):
        self.stage._request_urls[self.unpin] = self.url + self.unpin
        payload = self.status(self.unpin, "COMPLETED")
        payload["targets"] = []
        with patch.object(self.stage, "_api_request", return_value=json.dumps(payload)):
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                self.stage._wait_request_terminal(self.unpin, expected_paths={"/source"})

    def test_wrong_request_response_does_not_confirm_a_pin(self):
        self.stage.stage(["/source"])
        with patch.object(self.stage, "_api_request", return_value=json.dumps(self.status(self.unpin, "COMPLETED"))):
            self.assertEqual(self.stage.poll_stage_request_errors([self.pin], {"/source"}), {})
            self.assertEqual(self.stage.poll_online_statuses(["/source"])[0], [])

    def test_later_target_page_cannot_hide_a_failed_pin(self):
        self.stage.stage(["/source"])
        first = self.status(self.pin, "COMPLETED")
        first["targets"] = []
        first["nextId"] = 42
        second = self.status(self.pin, "COMPLETED", "FAILED")
        second["targets"][0]["errorMessage"] = "no space"
        with patch.object(self.stage, "_api_request", side_effect=[json.dumps(first), json.dumps(second)]) as api:
            errors = self.stage.poll_stage_request_errors([self.pin], {"/source"})
        self.assertIn("no space", errors["/source"])
        self.assertEqual(api.call_args_list[1].args, (self.url + self.pin + "?offset=42",))

    def test_request_wait_has_a_deadline(self):
        self.stage._request_urls[self.unpin] = self.url + self.unpin
        self.stage.deadline = time.monotonic() + 0.02
        with patch.object(self.stage, "_api_request", return_value=json.dumps(self.status(self.unpin, "QUEUED"))):
            with self.assertRaises(TimeoutError):
                self.stage._wait_request_terminal(self.unpin)
