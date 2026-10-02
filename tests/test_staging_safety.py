import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

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
