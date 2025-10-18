"""Tests for manifest input/output validation rules in the CLI."""

import os
import unittest
from unittest.mock import patch

from dedupe_copy.bin import dedupecopy_cli
from dedupe_copy.test import utils


class TestManifestRules(unittest.TestCase):
    """Test manifest input/output validation rules."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = utils.make_temp_dir("manifest_rules")
        self.manifest_dir = utils.make_temp_dir("manifest_dir")
        # Create a dummy file for operations
        utils.make_file_tree(self.temp_dir, file_spec=1, file_size=10)

    def tearDown(self):
        """Tear down test environment."""
        utils.remove_dir(self.temp_dir)
        utils.remove_dir(self.manifest_dir)

    def run_cli_with_args(self, args):
        """Helper to run the CLI with a given set of arguments."""
        with patch("sys.argv", ["dedupecopy_cli.py"] + args):
            dedupecopy_cli.run_cli()

    def test_delete_requires_manifest_out(self):
        """Test that --delete requires -m/--manifest-dump-path."""
        args = ["--no-walk", "--delete", "-i", os.path.join(self.manifest_dir, "in.db")]
        with self.assertRaises(SystemExit) as cm:
            self.run_cli_with_args(args)
        self.assertEqual(cm.exception.code, 2)

    def test_delete_on_copy_requires_manifest_out(self):
        """Test that --delete-on-copy requires -m/--manifest-dump-path."""
        copy_to = os.path.join(self.temp_dir, "copy_to")
        args = ["-p", self.temp_dir, "-c", copy_to, "--delete-on-copy"]
        with self.assertRaises(SystemExit) as cm:
            self.run_cli_with_args(args)
        self.assertEqual(cm.exception.code, 2)

    def test_input_and_output_manifest_cannot_be_same(self):
        """Test that -i and -m cannot point to the same file."""
        manifest_path = os.path.join(self.manifest_dir, "manifest.db")
        args = ["--no-walk", "-i", manifest_path, "-m", manifest_path, "--delete"]
        with self.assertRaises(SystemExit) as cm:
            self.run_cli_with_args(args)
        self.assertEqual(cm.exception.code, 2)

    def test_reporting_does_not_require_manifest_out(self):
        """Test that a simple reporting operation does not require -m."""
        report_path = os.path.join(self.temp_dir, "report.csv")
        manifest_in_path = os.path.join(self.manifest_dir, "in.db")

        # First, create a manifest
        self.run_cli_with_args(["-p", self.temp_dir, "-m", manifest_in_path])

        # Now, run a reporting command without -m
        try:
            self.run_cli_with_args(
                ["--no-walk", "-i", manifest_in_path, "-r", report_path]
            )
        except SystemExit as e:
            self.fail(f"Reporting command failed unexpectedly with SystemExit: {e}")

        self.assertTrue(os.path.exists(report_path))


if __name__ == "__main__":
    unittest.main()
