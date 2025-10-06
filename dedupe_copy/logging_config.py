"""Logging configuration utilities for dedupe_copy

Provides centralized logging setup with verbosity levels and color support.
"""

import logging
import sys

# Try to import colorama for colored output
try:
    from colorama import Fore, Style, init as colorama_init

    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color support for terminal output"""

    # Color mappings for different log levels
    COLORS = {
        logging.DEBUG: Fore.CYAN if HAS_COLORAMA else "",
        logging.INFO: "",
        logging.WARNING: Fore.YELLOW if HAS_COLORAMA else "",
        logging.ERROR: Fore.RED if HAS_COLORAMA else "",
        logging.CRITICAL: Fore.RED + Style.BRIGHT if HAS_COLORAMA else "",
    }

    RESET = Style.RESET_ALL if HAS_COLORAMA else ""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with color if available"""
        # Add color to the level name
        if HAS_COLORAMA and sys.stdout.isatty():
            levelname = record.levelname
            color = self.COLORS.get(record.levelno, "")
            record.levelname = f"{color}{levelname}{self.RESET}"

        # Format the message
        result = super().format(record)

        return result


def setup_logging(verbosity: str = "normal", use_colors: bool = True) -> None:
    """Setup logging configuration for dedupe_copy

    Args:
        verbosity: One of 'quiet', 'normal', 'verbose', 'debug'
        use_colors: Whether to use colored output (only if colorama available)
    """
    # Initialize colorama if available
    if HAS_COLORAMA and use_colors:
        colorama_init(autoreset=True)

    # Map verbosity to log level
    level_map = {
        "quiet": logging.WARNING,
        "normal": logging.INFO,
        "verbose": logging.INFO,
        "debug": logging.DEBUG,
    }

    log_level = level_map.get(verbosity, logging.INFO)

    # Configure root logger for dedupe_copy module
    logger = logging.getLogger("dedupe_copy")
    logger.setLevel(log_level)

    # Remove any existing handlers
    logger.handlers = []

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Choose formatter based on verbosity and color support
    if verbosity == "debug":
        # Debug mode: show detailed information
        fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    elif verbosity == "quiet":
        # Quiet mode: minimal output
        fmt = "%(levelname)s: %(message)s"
    else:
        # Normal/verbose mode: just the message
        fmt = "%(message)s"

    formatter: logging.Formatter | ColoredFormatter
    if use_colors and HAS_COLORAMA and sys.stdout.isatty():
        formatter = ColoredFormatter(fmt)
    else:
        formatter = logging.Formatter(fmt)

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Don't propagate to root logger to avoid duplicate messages
    logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the dedupe_copy package

    Args:
        name: Module name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
