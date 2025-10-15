"""Tests for the web service functionality."""

import unittest
from unittest.mock import patch, MagicMock
import multiprocessing
import queue
import threading
import time

from dedupe_copy.bin import dedupecopy_cli
from dedupe_copy.threads import WebProgressThread


class TestWebService(unittest.TestCase):
    """Tests for the web service."""

    @patch("dedupe_copy.bin.dedupecopy_cli.run_dupe_copy")
    @patch("multiprocessing.Process")
    @patch("dedupe_copy.web_service.run_web_service")
    def test_web_ui_flag(self, mock_run_web_service, mock_process, mock_run_dupe_copy):
        """Test that the --web-ui flag starts the web service."""
        # We need to simulate running the CLI with the --web-ui flag
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "--no-walk",
                "--manifest-read-path",
                "dummy.db",
                "--web-ui",
            ],
        ):
            dedupecopy_cli.run_cli()

        # Check that a multiprocessing.Process was created and started
        mock_process.assert_called_once()
        mock_process.return_value.start.assert_called_once()

        # Check that run_dupe_copy was called with a progress_manager
        mock_run_dupe_copy.assert_called_once()
        args, kwargs = mock_run_dupe_copy.call_args
        self.assertIn("progress_manager", kwargs)
        self.assertIsNotNone(kwargs["progress_manager"])

    def test_web_progress_thread(self):
        """Test that the WebProgressThread updates the progress manager."""
        progress_manager = multiprocessing.Manager().dict()
        work_queue = queue.Queue()
        result_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()
        walk_queue = queue.Queue()
        stop_event = threading.Event()
        save_event = threading.Event()

        thread = WebProgressThread(
            work_queue,
            result_queue,
            progress_queue,
            walk_queue=walk_queue,
            stop_event=stop_event,
            save_event=save_event,
            progress_manager=progress_manager,
        )

        thread.do_log_file("test.txt")
        self.assertEqual(progress_manager["file_count"], 1)

        thread.do_log_dir("/tmp")
        self.assertEqual(progress_manager["directory_count"], 1)

        thread.do_log_accepted("test.txt")
        self.assertEqual(progress_manager["accepted_count"], 1)
        self.assertEqual(progress_manager["last_accepted"], "test.txt")

    # The Flask app test is a bit more involved and might require a test client.
    # For now, I'll focus on the CLI flag and the progress thread.
    # I can add a test for the Flask app later if needed.

if __name__ == "__main__":
    unittest.main()
