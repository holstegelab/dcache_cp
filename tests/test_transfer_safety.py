import os
import shutil
import subprocess
import tempfile
import threading
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from dcache_cp import cli


def checksum(data):
    return format(zlib.adler32(data) & 0xffffffff, "08x")


@unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
class TransferSafetyTests(unittest.TestCase):
    """Exercise copying, promotion, and cleanup with real rclone and local files."""

    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        self.remote = self.root / "remote"
        self.remote.mkdir()
        self.config = self.root / "rclone.conf"
        self.config.write_text(f"[dcache]\ntype=alias\nremote={self.remote}\n")
        # Keep all test checksum state in memory, away from the user's cache.
        cache = cli._ChecksumCache.__new__(cli._ChecksumCache)
        cache._lock, cache._data, cache._dirty = threading.Lock(), {}, set()
        cache._load = lambda path: {}
        patched = patch.object(cli, "_checksum_cache", cache)
        patched.start()
        self.addCleanup(patched.stop)

    def transfer(self, **options):
        t = cli.Transferer(self.config, "dcache", "unused-ada", None, 0, 0, "10s", **options)
        t._remote_adler = lambda path: checksum((self.remote / path).read_bytes())
        return t

    def upload(self, source, remote="output.txt"):
        return cli.plan_upload(source, remote, False)[0]

    def download(self, destination, remote="source.txt"):
        return dict(remote_path=remote, local_path=destination, rel=remote, size=4)

    def assert_no_temporary_copies(self):
        self.assertEqual(list(self.root.rglob(".dcache-cp-*.part")), [])

    def test_upload_checksum_api_failure_preserves_existing_destination(self):
        source, destination = self.root / "source", self.remote / "output.txt"
        source.write_bytes(b"GOOD")
        destination.write_bytes(b"GOOD")
        t = self.transfer(skip_verified=False)
        t._remote_adler = lambda path: (_ for _ in ()).throw(RuntimeError("temporary API failure"))
        with self.assertRaises(RuntimeError):
            t.upload(self.upload(source))
        self.assertEqual(source.read_bytes(), b"GOOD")
        self.assertEqual(destination.read_bytes(), b"GOOD")
        self.assert_no_temporary_copies()

    def test_download_checksum_api_failure_preserves_existing_destination(self):
        (self.remote / "source.txt").write_bytes(b"GOOD")
        destination = self.root / "output"
        destination.write_bytes(b"GOOD")
        t = self.transfer(skip_verified=False)
        t._remote_adler = lambda path: (_ for _ in ()).throw(RuntimeError("temporary API failure"))
        with self.assertRaises(RuntimeError):
            t.download(self.download(destination))
        self.assertEqual(destination.read_bytes(), b"GOOD")
        self.assertEqual((self.remote / "source.txt").read_bytes(), b"GOOD")
        self.assert_no_temporary_copies()

    def test_upload_mismatch_preserves_old_destination_and_source(self):
        source = self.root / "source"
        source.write_bytes(b"NEW_DATA")
        (self.remote / "output.txt").write_bytes(b"OLD_DATA")
        t = self.transfer(skip_verified=False, delete_source=True)
        t._remote_adler = lambda path: checksum(b"WRONG")
        with self.assertRaises(RuntimeError):
            t.upload(self.upload(source))
        self.assertEqual(source.read_bytes(), b"NEW_DATA")
        self.assertEqual((self.remote / "output.txt").read_bytes(), b"OLD_DATA")
        self.assert_no_temporary_copies()

    def test_download_mismatch_preserves_old_destination_and_source(self):
        (self.remote / "source.txt").write_bytes(b"NEW_DATA")
        destination = self.root / "output"
        destination.write_bytes(b"OLD_DATA")
        t = self.transfer(skip_verified=False, delete_source=True)
        t._remote_adler = lambda path: checksum(b"WRONG")
        with self.assertRaises(RuntimeError):
            t.download(self.download(destination))
        self.assertEqual(destination.read_bytes(), b"OLD_DATA")
        self.assertTrue((self.remote / "source.txt").exists())
        self.assert_no_temporary_copies()

    def test_upload_move_promotes_verified_copy_and_deletes_source(self):
        source = self.root / "source"
        source.write_bytes(b"NEW")
        (self.remote / "output.txt").write_bytes(b"OLD")
        self.transfer(delete_source=True).upload(self.upload(source))
        self.assertFalse(source.exists())
        self.assertEqual((self.remote / "output.txt").read_bytes(), b"NEW")
        self.assert_no_temporary_copies()

    def test_download_move_promotes_verified_copy_and_deletes_source(self):
        source, destination = self.remote / "source.txt", self.root / "output"
        source.write_bytes(b"NEW")
        destination.write_bytes(b"OLD")
        self.transfer(delete_source=True).download(self.download(destination))
        self.assertFalse(source.exists())
        self.assertEqual(destination.read_bytes(), b"NEW")
        self.assert_no_temporary_copies()

    def test_changed_source_is_retained_and_existing_destination_preserved(self):
        source = self.root / "source"
        source.write_bytes(b"OLD_SOURCE")
        destination = self.remote / "output.txt"
        destination.write_bytes(b"EXISTING_COPY")
        t = self.transfer(skip_verified=False, delete_source=True)
        def remote_hash(path):
            value = checksum((self.remote / path).read_bytes())
            source.write_bytes(b"NEW_SOURCE_WRITTEN_DURING_COPY")
            return value
        t._remote_adler = remote_hash
        with self.assertRaises(cli.SourceChangedError):
            t.upload(self.upload(source))
        self.assertEqual(source.read_bytes(), b"NEW_SOURCE_WRITTEN_DURING_COPY")
        self.assertEqual(destination.read_bytes(), b"EXISTING_COPY")
        self.assert_no_temporary_copies()

    def test_changed_local_destination_is_retained(self):
        source, destination = self.remote / "source.txt", self.root / "output"
        source.write_bytes(b"REMOTE")
        destination.write_bytes(b"OLD")
        t = self.transfer(skip_verified=False, delete_source=True)
        copy = t._rclone_copyto
        def changed_copy(src, dst):
            copy(src, dst)
            destination.write_bytes(b"NEW_LOCAL_CONTENT")
        t._rclone_copyto = changed_copy
        with self.assertRaises(cli.SourceChangedError):
            t.download(self.download(destination))
        self.assertEqual(destination.read_bytes(), b"NEW_LOCAL_CONTENT")
        self.assertTrue(source.exists())
        self.assert_no_temporary_copies()

    def test_cache_invalidates_rewrite_with_preserved_size_and_mtime(self):
        source = self.root / "source"
        source.write_bytes(b"AAAA")
        old_stat = source.stat()
        original = cli.adler32_local(source)
        source.write_bytes(b"BBBB")
        os.utime(source, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
        self.assertNotEqual(cli.adler32_local(source), original)

    def test_move_uses_fresh_content_after_preserved_mtime_rewrite(self):
        source = self.root / "source"
        source.write_bytes(b"AAAA")
        old_stat = source.stat()
        cli.adler32_local(source)
        (self.remote / "output.txt").write_bytes(b"AAAA")
        source.write_bytes(b"BBBB")
        os.utime(source, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
        self.transfer(delete_source=True).upload(self.upload(source))
        self.assertFalse(source.exists())
        self.assertEqual((self.remote / "output.txt").read_bytes(), b"BBBB")

    def test_symlink_move_deletes_link_and_keeps_target(self):
        target, link = self.root / "target", self.root / "link"
        target.write_bytes(b"CONTENT")
        link.symlink_to(target)
        self.transfer(delete_source=True).upload(self.upload(link))
        self.assertFalse(link.is_symlink())
        self.assertEqual(target.read_bytes(), b"CONTENT")
        self.assertEqual((self.remote / "output.txt").read_bytes(), b"CONTENT")

    def test_tsv_symlink_move_deletes_link_and_keeps_target(self):
        target, link = self.root / "target", self.root / "link"
        target.write_bytes(b"CONTENT")
        link.symlink_to(target)
        manifest = self.root / "input.tsv"
        manifest.write_text(f"{link}\tdcache:output.txt\n")
        _, entries = cli.load_file_list(manifest)
        self.transfer(delete_source=True).upload(entries[0])
        self.assertFalse(link.is_symlink())
        self.assertEqual(target.read_bytes(), b"CONTENT")

    def test_move_through_directory_symlink_is_rejected(self):
        link = self.root / "link"
        link.symlink_to(self.remote, target_is_directory=True)
        with self.assertRaises(ValueError):
            cli.plan_upload(link, "output/", True, move=True)

    def test_force_copy_replaces_same_size_same_mtime_contents(self):
        source, destination = self.remote / "source.txt", self.root / "output"
        source.write_bytes(b"GOOD")
        destination.write_bytes(b"BAD!")
        os.utime(destination, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns))
        self.transfer(skip_verified=False)._rclone_copyto("dcache:source.txt", str(destination))
        self.assertEqual(destination.read_bytes(), b"GOOD")

    def test_promotion_failure_retains_verified_temporary_copy_and_source(self):
        source = self.root / "source"
        source.write_bytes(b"NEW")
        (self.remote / "output.txt").write_bytes(b"OLD")
        t = self.transfer(skip_verified=False, delete_source=True)
        with patch.object(t, "_rclone_moveto", side_effect=RuntimeError("MOVE failed")):
            with self.assertRaisesRegex(RuntimeError, "source retained"):
                t.upload(self.upload(source))
        self.assertEqual(source.read_bytes(), b"NEW")
        self.assertEqual((self.remote / "output.txt").read_bytes(), b"OLD")
        temporary = list(self.remote.glob(".dcache-cp-*.part"))
        self.assertEqual(len(temporary), 1)
        self.assertEqual(temporary[0].read_bytes(), b"NEW")

    def test_remote_change_before_delete_preserves_remote_source(self):
        source, destination = self.remote / "source.txt", self.root / "output"
        source.write_bytes(b"OLD")
        t = self.transfer(skip_verified=False, delete_source=True)
        calls = []
        def remote_hash(path):
            calls.append(path)
            if len(calls) == 2:
                source.write_bytes(b"NEW")
            return checksum(source.read_bytes())
        t._remote_adler = remote_hash
        with self.assertRaises(cli.SourceChangedError):
            t.download(self.download(destination))
        self.assertEqual(source.read_bytes(), b"NEW")
        self.assertEqual(destination.read_bytes(), b"OLD")

    def test_retry_uses_new_temporary_copy_without_removing_destination(self):
        source = self.root / "source"
        source.write_bytes(b"NEW")
        (self.remote / "output.txt").write_bytes(b"OLD")
        t = self.transfer(skip_verified=False)
        t.max_retries = 1
        actual = t._remote_adler
        seen = []
        def remote_hash(path):
            seen.append(path)
            return checksum(b"WRONG") if len(seen) == 1 else actual(path)
        t._remote_adler = remote_hash
        result = t.upload(self.upload(source))
        self.assertEqual(result["attempt"], 2)
        self.assertNotEqual(seen[0], seen[1])
        self.assertEqual((self.remote / "output.txt").read_bytes(), b"NEW")
        self.assert_no_temporary_copies()

    def test_download_retries_copy_failure_before_temporary_file_exists(self):
        (self.remote / "source.txt").write_bytes(b"GOOD")
        destination = self.root / "output"
        destination.write_bytes(b"OLD")
        t = self.transfer(skip_verified=False)
        t.max_retries = 1
        copy = t._rclone_copyto
        calls = []
        def flaky_copy(src, dst):
            calls.append(dst)
            if len(calls) == 1:
                self.assertEqual(destination.read_bytes(), b"OLD")
                raise subprocess.CalledProcessError(1, ["rclone"])
            copy(src, dst)
        t._rclone_copyto = flaky_copy
        result = t.download(self.download(destination))
        self.assertEqual(result["attempt"], 2)
        self.assertEqual(destination.read_bytes(), b"GOOD")
        self.assert_no_temporary_copies()
