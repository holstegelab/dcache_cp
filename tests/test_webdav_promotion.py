import shutil
import tempfile
import threading
import unittest
import urllib.parse
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

from dcache_cp import cli


@unittest.skipUnless(shutil.which("rclone"), "rclone is not installed")
class WebdavPromotionTests(unittest.TestCase):
    """Exercise actual rclone WebDAV requests against a local synthetic server."""

    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        cache = patch.object(cli, "_checksum_cache")
        self.cache = cache.start()
        self.cache.get.return_value = None
        self.addCleanup(cache.stop)
        self.files = {"/output": b"OLD"}
        self.requests = []
        self.reject_move = False
        test = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def reply(self, code, body=b"", content_type="text/plain"):
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Type", content_type)
                self.end_headers()
                self.wfile.write(body)

            def path_name(self):
                return urllib.parse.unquote(urllib.parse.urlparse(self.path).path)

            def do_PROPFIND(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                path = self.path_name()
                test.requests.append(("PROPFIND", path))
                if path != "/" and path not in test.files:
                    self.reply(404)
                    return
                resource = "<d:collection/>" if path == "/" else ""
                body = (f'<d:multistatus xmlns:d="DAV:"><d:response><d:href>{escape(path)}</d:href>'
                        f'<d:propstat><d:prop><d:resourcetype>{resource}</d:resourcetype>'
                        f'<d:getcontentlength>{len(test.files.get(path, b""))}</d:getcontentlength>'
                        '<d:getlastmodified>Fri, 02 Oct 2026 09:00:00 GMT</d:getlastmodified>'
                        '</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>'
                        '</d:response></d:multistatus>').encode()
                self.reply(207, body, "application/xml")

            def do_MKCOL(self):
                self.reply(405)

            def do_PUT(self):
                path = self.path_name()
                test.requests.append(("PUT", path))
                test.files[path] = self.rfile.read(int(self.headers["Content-Length"]))
                self.reply(201)

            def do_MOVE(self):
                path = self.path_name()
                test.requests.append(("MOVE", path))
                if test.reject_move:
                    self.reply(403)
                    return
                destination = urllib.parse.unquote(urllib.parse.urlparse(self.headers["Destination"]).path)
                test.assertEqual(self.headers["Overwrite"], "T")
                test.files[destination] = test.files.pop(path)
                self.reply(201)

            def do_DELETE(self):
                path = self.path_name()
                test.requests.append(("DELETE", path))
                test.files.pop(path, None)
                self.reply(204)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.config = self.root / "rclone.conf"
        self.config.write_text(f"[dcache]\ntype=webdav\nvendor=other\nurl=http://127.0.0.1:{self.server.server_port}/\n")
        self.source = self.root / "source"
        self.source.write_bytes(b"NEW")
        self.transfer = cli.Transferer(self.config, "dcache", "unused-ada", None, 0, 0, "5s",
                                       skip_verified=False, delete_source=True)
        self.transfer._remote_adler = lambda path: format(zlib.adler32(self.files["/" + path]) & 0xffffffff, "08x")

    def test_successful_server_move_replaces_destination_without_predeletion(self):
        result = self.transfer.upload(cli.plan_upload(self.source, "output", False)[0])
        self.assertEqual(result["attempt"], 1)
        self.assertEqual(self.files, {"/output": b"NEW"})
        self.assertFalse(self.source.exists())
        self.assertNotIn(("DELETE", "/output"), self.requests)
        self.assertEqual(len([r for r in self.requests if r[0] == "MOVE"]), 1)

    def test_failed_server_move_preserves_existing_destination_and_verified_copy(self):
        self.reject_move = True
        with self.assertRaisesRegex(RuntimeError, "source retained"):
            self.transfer.upload(cli.plan_upload(self.source, "output", False)[0])
        self.assertEqual(self.source.read_bytes(), b"NEW")
        self.assertEqual(self.files["/output"], b"OLD")
        temporary = [p for p in self.files if p.startswith("/.dcache-cp-")]
        self.assertEqual(len(temporary), 1)
        self.assertEqual(self.files[temporary[0]], b"NEW")
        self.assertNotIn(("DELETE", "/output"), self.requests)
