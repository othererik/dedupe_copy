"""Tests for the CLI argument validation."""
import unittest
from unittest.mock import patch

from dedupe_copy.bin import dedupecopy_cli


class TestCliValidation(unittest.TestCase):
    """Tests for the CLI argument validation."""

    @patch("dedupe_copy.bin.dedupecopy_cli.run_dupe_copy")
    @patch("dedupe_copy.bin.dedupecopy_cli.argparse.ArgumentParser.error")
    def test_compare_and_output_manifest_cannot_be_same(
        self, mock_error, mock_run_dupe_copy
    ):  # pylint: disable=unused-argument
        """Verify CLI exits if --compare and -m paths are the same."""
        # This test is expected to fail before the fix
        test_args = [
            "dedupecopy",
            "--no-walk",
            "--compare",
            "some/path/manifest.db",
            "-m",
            "some/path/manifest.db",
        ]

        # Mock the parser.error to avoid exiting the test runner
        mock_error.side_effect = SystemExit

        with patch("sys.argv", test_args):
            with self.assertRaises(SystemExit):
                dedupecopy_cli.run_cli()

        mock_error.assert_called_once_with(
            "--compare and -m/--manifest-dump-path cannot be the same file."
        )

    @patch("dedupe_copy.bin.dedupecopy_cli.run_dupe_copy")
    @patch("dedupe_copy.bin.dedupecopy_cli.argparse.ArgumentParser.error")
    def test_input_and_output_manifest_cannot_be_same(
        self, mock_error, mock_run_dupe_copy
    ):  # pylint: disable=unused-argument
        """Verify CLI exits if -i and -m paths are the same."""
        test_args = [
            "dedupecopy",
            "--no-walk",
            "-i",
            "some/path/manifest.db",
            "-m",
            "some/path/manifest.db",
        ]
        mock_error.side_effect = SystemExit

        with patch("sys.argv", test_args):
            with self.assertRaises(SystemExit):
                dedupecopy_cli.run_cli()

        mock_error.assert_called_once_with(
            "-i/--manifest-read-path and -m/--manifest-dump-path cannot be the same file."
        )
