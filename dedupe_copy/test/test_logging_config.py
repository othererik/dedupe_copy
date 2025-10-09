"""Unit tests for logging configuration in dedupe_copy."""

import unittest
import logging
from unittest.mock import patch

from dedupe_copy.logging_config import (
    setup_logging,
    ColoredFormatter,
    get_logger,
    HAS_COLORAMA,
)


class TestLoggingConfig(unittest.TestCase):
    """Tests for logging configuration in dedupe_copy.logging_config"""

    def setUp(self):
        # Reset logging configuration before each test
        logging.getLogger("dedupe_copy").handlers = []

    @patch("dedupe_copy.logging_config.sys.stdout.isatty", return_value=True)
    @patch("dedupe_copy.logging_config.colorama_init")
    def test_setup_logging_with_colors(
        self, mock_colorama_init, mock_isatty
    ):  # pylint: disable=W0613
        """Test that setup_logging uses ColoredFormatter when colors are enabled."""
        if not HAS_COLORAMA:
            self.skipTest("colorama is not installed")

        setup_logging(verbosity="normal", use_colors=True)
        logger = logging.getLogger("dedupe_copy")
        self.assertEqual(len(logger.handlers), 1)
        handler = logger.handlers[0]
        self.assertIsInstance(handler.formatter, ColoredFormatter)
        mock_colorama_init.assert_called_once()

    @patch("dedupe_copy.logging_config.sys.stdout.isatty", return_value=False)
    def test_setup_logging_without_tty(self, mock_isatty):  # pylint: disable=W0613
        """Test that setup_logging does not use ColoredFormatter when not in a TTY."""
        setup_logging(verbosity="normal", use_colors=True)
        logger = logging.getLogger("dedupe_copy")
        self.assertEqual(len(logger.handlers), 1)
        handler = logger.handlers[0]
        self.assertIsInstance(handler.formatter, logging.Formatter)
        self.assertNotIsInstance(handler.formatter, ColoredFormatter)

    def test_setup_logging_no_colors(self):
        """Test that setup_logging uses the standard Formatter when colors are disabled."""
        setup_logging(verbosity="normal", use_colors=False)
        logger = logging.getLogger("dedupe_copy")
        self.assertEqual(len(logger.handlers), 1)
        handler = logger.handlers[0]
        self.assertIsInstance(handler.formatter, logging.Formatter)
        self.assertNotIsInstance(handler.formatter, ColoredFormatter)

    def test_verbosity_levels(self):
        """Test that verbosity levels are correctly mapped to logging levels."""
        level_map = {
            "quiet": logging.WARNING,
            "normal": logging.INFO,
            "verbose": logging.INFO,
            "debug": logging.DEBUG,
        }
        for verbosity, level in level_map.items():
            with self.subTest(verbosity=verbosity):
                setup_logging(verbosity=verbosity)
                logger = logging.getLogger("dedupe_copy")
                self.assertEqual(logger.level, level)

    def test_get_logger(self):
        """Test that get_logger returns a logger with the correct name."""
        logger_name = "my_test_logger"
        logger = get_logger(logger_name)
        self.assertEqual(logger.name, logger_name)


class TestColoredFormatter(unittest.TestCase):
    """Tests for the ColoredFormatter class."""

    def setUp(self):
        self.record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )

    @patch("dedupe_copy.logging_config.sys.stdout.isatty", return_value=True)
    def test_colored_formatter_applies_colors(
        self, mock_isatty
    ):  # pylint: disable=W0613
        """Test that ColoredFormatter applies colors to the log level name."""
        if not HAS_COLORAMA:
            self.skipTest("colorama is not installed")

        formatter = ColoredFormatter("%(levelname)s: %(message)s")

        level_colors = {
            logging.DEBUG: "\x1b[36m",  # Fore.CYAN
            logging.INFO: "",  # No color for INFO
            logging.WARNING: "\x1b[33m",  # Fore.YELLOW
            logging.ERROR: "\x1b[31m",  # Fore.RED
            logging.CRITICAL: "\x1b[31m\x1b[1m",  # Fore.RED + Style.BRIGHT
        }

        for level, color_code in level_colors.items():
            with self.subTest(level=logging.getLevelName(level)):
                self.record.levelno = level
                self.record.levelname = logging.getLevelName(level)
                formatted_message = formatter.format(self.record)

                # Check that the levelname is colored correctly
                self.assertIn(color_code, formatted_message)
                self.assertIn(logging.getLevelName(level), formatted_message)
                # Check for reset code
                if color_code:
                    self.assertIn("\x1b[0m", formatted_message)

    @patch("dedupe_copy.logging_config.sys.stdout.isatty", return_value=False)
    def test_colored_formatter_no_tty(self, mock_isatty):  # pylint: disable=W0613
        """Test that ColoredFormatter does not add color when not in a TTY."""
        formatter = ColoredFormatter("%(levelname)s: %(message)s")
        self.record.levelno = logging.WARNING
        self.record.levelname = "WARNING"
        formatted_message = formatter.format(self.record)
        self.assertEqual(formatted_message, "WARNING: Test message")


if __name__ == "__main__":
    unittest.main()
