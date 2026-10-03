from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dcache_cp import cli


class PlanDownloadSingleFileTests(unittest.TestCase):
    """Regression tests for single-file download enumeration.

    ``rclone lsjson`` on a *file* returns one non-dir entry whose ``Path`` is the
    file's own basename.  The remote path must then be used verbatim, not joined
    with the basename again (which produced ``<file>/<file>`` and a 404).
    """

    def setUp(self) -> None:
        self.config = Path("/nonexistent/dcache.conf")

    def test_single_file_source_is_not_double_joined(self) -> None:
        listing = [{
            "Path": "all_combined_v5.tsv",
            "Name": "all_combined_v5.tsv",
            "Size": 21754443,
            "IsDir": False,
        }]
        with mock.patch.object(cli, "_rclone_lsjson", return_value=listing), \
                mock.patch.object(cli, "_rclone_stat", return_value=listing[0]) as is_file:
            out = cli.plan_download(
                self.config,
                "dcache",
                "releases/release1/all_combined_v5.tsv",
                "/local/ades_release1/",
                recursive=False,
            )

        is_file.assert_called_once()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["remote_path"], "releases/release1/all_combined_v5.tsv")
        self.assertEqual(out[0]["local_path"], Path("/local/ades_release1/all_combined_v5.tsv"))
        self.assertEqual(out[0]["size"], 21754443)

    def test_directory_source_still_joins_children(self) -> None:
        listing = [
            {"Path": "a.tsv", "Size": 10, "IsDir": False},
            {"Path": "b.tsv", "Size": 20, "IsDir": False},
        ]
        # Stat identifies a directory before listing its children.
        with mock.patch.object(cli, "_rclone_lsjson", return_value=listing), \
                mock.patch.object(cli, "_rclone_stat", return_value={"IsDir": True}) as is_file:
            out = cli.plan_download(
                self.config,
                "dcache",
                "releases/release1",
                Path("/local/dest"),
                recursive=False,
            )

        is_file.assert_called_once()
        self.assertEqual(
            sorted(e["remote_path"] for e in out),
            ["releases/release1/a.tsv", "releases/release1/b.tsv"],
        )

    def test_directory_with_single_equally_named_child_still_joins(self) -> None:
        # Directory ``foo`` containing a single file ``foo``: lsjson Path == basename,
        # but stat reports a directory, so the basename heuristic must defer to it.
        listing = [{"Path": "foo", "Size": 5, "IsDir": False}]
        with mock.patch.object(cli, "_rclone_lsjson", return_value=listing), \
                mock.patch.object(cli, "_rclone_stat", return_value={"IsDir": True}) as is_file:
            out = cli.plan_download(
                self.config,
                "dcache",
                "dataset/foo",
                Path("/local/dest"),
                recursive=False,
            )

        is_file.assert_called_once()
        self.assertEqual(out[0]["remote_path"], "dataset/foo/foo")


if __name__ == "__main__":
    unittest.main()
