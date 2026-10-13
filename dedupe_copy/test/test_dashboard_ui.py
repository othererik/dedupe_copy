"""Tests for the dashboard UI, file logging, and CLI flags."""
# pylint: disable=protected-access

import logging
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from rich.console import Console, Group

from dedupe_copy.bin.dedupecopy_cli import _create_parser, _handle_arguments
from dedupe_copy.logging_config import setup_logging
from dedupe_copy.ui import ConsoleUI, DashboardLogHandler


class TestDashboardUI(unittest.TestCase):
    """Unit tests for ConsoleUI in dashboard mode."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test_run.log")
        self.console = Console(record=True, width=100)

    def tearDown(self) -> None:
        # Reset logging handlers
        logger = logging.getLogger("dedupe_copy")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        root = logging.getLogger()
        for handler in root.handlers[:]:
            handler.close()
            root.removeHandler(handler)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_dashboard_initialization(self) -> None:
        """Verify ConsoleUI in dashboard mode initializes live renderable and log file."""
        ui = ConsoleUI(
            console=self.console,
            dashboard=True,
            log_file=self.log_file,
        )
        try:
            self.assertTrue(ui.dashboard)
            self.assertEqual(ui.log_file, self.log_file)
            self.assertIsNotNone(ui.live)

            # Check DashboardLogHandler is attached
            dedupe_logger = logging.getLogger("dedupe_copy")
            has_dash_handler = any(
                isinstance(h, DashboardLogHandler) for h in dedupe_logger.handlers
            )
            self.assertTrue(has_dash_handler)

            # Check FileHandler is attached
            has_file_handler = any(
                isinstance(h, logging.FileHandler) for h in dedupe_logger.handlers
            )
            self.assertTrue(has_file_handler)
        finally:
            ui.stop()

    def test_dashboard_log_ring_buffer(self) -> None:
        """Verify log messages are captured in the ring buffer up to maxlen."""
        ui = ConsoleUI(console=self.console, dashboard=True)
        try:
            logger = logging.getLogger("dedupe_copy")
            for i in range(10):
                logger.info("Message %d", i)

            # Buffer holds last 5 messages
            self.assertEqual(len(ui._recent_logs), 5)
            last_msg = ui._recent_logs[-1]
            self.assertEqual(last_msg[1], "INFO")
            self.assertIn("Message 9", last_msg[2])
        finally:
            ui.stop()

    def test_dashboard_renderable_components(self) -> None:
        """Verify that _build_dashboard_renderable creates a valid Group."""
        ui = ConsoleUI(
            console=self.console,
            dashboard=True,
            log_file=self.log_file,
        )
        try:
            ui.update_stats(
                discovered=1000,
                directories=25,
                accepted=850,
                ignored=150,
                errors=2,
                copied=500,
                deleted=10,
                rate=125.5,
                work_queue=50,
                walk_queue=5,
                result_queue=10,
                progress_queue=0,
                elapsed=12.0,
            )
            ui.add_task("Walking filesystem...", total=1000)
            ui.add_log_record("INFO", "Discovered test batch")
            ui.add_log_record("ERROR", "Sample test error")

            renderable = ui._build_dashboard_renderable()
            self.assertIsInstance(renderable, Group)
            self.assertEqual(len(renderable.renderables), 4)

            # Render to console recording
            self.console.print(renderable)
            output = self.console.export_text()
            self.assertIn("DedupeCopy", output)
            self.assertIn("Files Discovered", output)
            self.assertIn("1,000", output)
            self.assertIn("Files Accepted", output)
            self.assertIn("Recent Activity", output)
            self.assertIn("Sample test error", output)
        finally:
            ui.stop()

    def test_dashboard_verify_workflow(self) -> None:
        """Verify dashboard renders verify-specific metrics and omits walk queues."""
        ui = ConsoleUI(console=self.console, dashboard=True)
        try:
            ui.update_stats(
                workflow="verify",
                total_to_verify=50,
                verified=48,
                errors=2,
                remaining=0,
                hashes=45,
                rate=250.0,
                elapsed=0.2,
            )
            renderable = ui._build_dashboard_renderable()
            self.console.print(renderable)
            output = self.console.export_text()

            # Verify panel titles and verify-specific metrics
            self.assertIn("Work Left / Manifest", output)
            self.assertIn("Files Awaiting Verification", output)
            self.assertIn("Total Files in Manifest", output)
            self.assertIn("Manifest Hashes", output)
            self.assertIn("Files Verified", output)
            self.assertIn("48", output)
            self.assertIn("Errors Encountered", output)
            self.assertIn("2", output)
            self.assertIn("250.0 files/sec", output)

            # Irrelevant walk queues and metrics should be omitted
            self.assertNotIn("Directories to Walk", output)
            self.assertNotIn("Files Awaiting Hash", output)
            self.assertNotIn("Files Discovered", output)
            self.assertNotIn("Directories Scanned", output)
        finally:
            ui.stop()

    def test_dashboard_no_walk_copy_workflow(self) -> None:
        """Verify dashboard in no-walk copy mode shows copy work and omits walk metrics."""
        ui = ConsoleUI(console=self.console, dashboard=True)
        try:
            ui.update_stats(
                copied=10,
                total_to_copy=25,
                remaining_copy=15,
                rate=12.0,
                elapsed=1.0,
            )
            renderable = ui._build_dashboard_renderable()
            self.console.print(renderable)
            output = self.console.export_text()

            self.assertIn("Files Awaiting Copy", output)
            self.assertIn("15", output)
            self.assertIn("Files Copied", output)
            self.assertIn("10", output)
            self.assertIn("12.0 files/sec", output)

            # Walk metrics omitted when no walk was performed
            self.assertNotIn("Files Discovered", output)
            self.assertNotIn("Directories Scanned", output)
        finally:
            ui.stop()

    def test_dashboard_start_and_stop(self) -> None:
        """Verify dashboard start and stop methods invoke Live methods without error."""
        ui = ConsoleUI(console=self.console, dashboard=True)
        self.assertIsNotNone(ui.live)
        with patch.object(ui.live, "start") as mock_start, patch.object(
            ui.live, "stop"
        ) as mock_stop:
            ui.start()
            mock_start.assert_called_once()
            ui.stop()
            mock_stop.assert_called_once()

    def test_setup_logging_with_file(self) -> None:
        """Verify setup_logging creates and writes messages to log_file."""
        setup_logging(verbosity="normal", use_colors=False, log_file=self.log_file)
        logger = logging.getLogger("dedupe_copy")
        logger.info("Log line 1")
        logger.warning("Log line 2")

        # Flush handlers
        for h in logger.handlers:
            h.flush()

        self.assertTrue(os.path.exists(self.log_file))
        with open(self.log_file, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("Log line 1", content)
        self.assertIn("Log line 2", content)


class TestCLIDashboardFlags(unittest.TestCase):
    """Unit tests for CLI parsing of dashboard and log file options."""

    def test_cli_dashboard_flag_parsing(self) -> None:
        """Test --dashboard and --no-dashboard parsing."""
        parser = _create_parser()

        # Explicit --dashboard
        args = parser.parse_args(["-p", "/tmp", "--dashboard"])
        self.assertTrue(args.dashboard)

        # Explicit --no-dashboard
        args = parser.parse_args(["-p", "/tmp", "--no-dashboard"])
        self.assertFalse(args.dashboard)

        # Alias --stream-ui
        args = parser.parse_args(["-p", "/tmp", "--stream-ui"])
        self.assertFalse(args.dashboard)

    def test_cli_log_file_options(self) -> None:
        """Test --log-file and --no-log-file parsing and default resolution."""
        parser = _create_parser()

        # Explicit log file
        args = parser.parse_args(["-p", "/tmp", "--log-file", "my_log.txt"])
        self.assertEqual(args.log_file, "my_log.txt")
        processed = _handle_arguments(args)
        self.assertEqual(processed["log_file"], "my_log.txt")

        # Explicit --no-log-file
        args = parser.parse_args(["-p", "/tmp", "--dashboard", "--no-log-file"])
        self.assertTrue(args.no_log_file)
        processed = _handle_arguments(args)
        self.assertIsNone(processed["log_file"])

        # Default dashboard log file
        args = parser.parse_args(["-p", "/tmp", "--dashboard"])
        processed = _handle_arguments(args)
        self.assertEqual(processed["log_file"], "dedupe_copy.log")

    def test_cli_demo_flag(self) -> None:
        """Test --demo CLI flag parsing."""
        parser = _create_parser()
        args = parser.parse_args(["--demo"])
        self.assertTrue(args.demo)


class TestDashboardEndToEnd(unittest.TestCase):
    """End-to-end test of run_dupe_copy with dashboard and log file."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.src_dir = os.path.join(self.temp_dir, "src")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        self.log_file = os.path.join(self.temp_dir, "run.log")
        os.makedirs(self.src_dir)
        for i in range(5):
            with open(
                os.path.join(self.src_dir, f"file_{i}.txt"), "w", encoding="utf-8"
            ) as f:
                f.write(f"content {i}")

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_run_dupe_copy_with_dashboard_and_log_file(self) -> None:
        """Run run_dupe_copy with dashboard=True and verify files copied and log written."""
        from dedupe_copy.core import run_dupe_copy  # pylint: disable=import-outside-toplevel

        ret = run_dupe_copy(
            read_from_path=self.src_dir,
            copy_to_path=self.dest_dir,
            use_ui=True,
            dashboard=True,
            log_file=self.log_file,
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(self.dest_dir))
        self.assertTrue(os.path.exists(self.log_file))
        with open(self.log_file, "r", encoding="utf-8") as f:
            log_content = f.read()
        self.assertIn("Final Summary", log_content)


if __name__ == "__main__":
    unittest.main()
