"""Tests for dedupe_copy.__main__. - Just picking up coverage, not super useful."""

import runpy
import unittest
from unittest.mock import patch


class TestMain(unittest.TestCase):
    """Test __main__"""

    def test_main(self):
        """Test that __main__ calls run_cli"""
        with patch("dedupe_copy.bin.dedupecopy_cli.run_cli") as mock_run_cli:
            runpy.run_module("dedupe_copy.__main__", run_name="__main__")
            self.assertTrue(mock_run_cli.called)
