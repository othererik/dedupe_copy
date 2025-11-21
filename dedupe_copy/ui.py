"""User Interface module using rich."""

import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TaskID,
)
from rich.theme import Theme

# Custom theme
custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red bold",
        "success": "green",
    }
)


class ConsoleUI:
    """Manages the rich console and progress display."""

    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console(theme=custom_theme)
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
            transient=True,  # Clear progress bar when done
        )
        self.tasks: dict[str, TaskID] = {}
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging to work with rich."""
        # Remove existing handlers to avoid duplicate logs
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Add RichHandler
        handler = RichHandler(console=self.console, rich_tracebacks=True, markup=True)
        handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    def start(self):
        """Start the progress display."""
        self.progress.start()

    def stop(self):
        """Stop the progress display."""
        self.progress.stop()

    def add_task(
        self, name: str, total: Optional[float] = None, description: str = ""
    ) -> TaskID:
        """Add a new task to the progress bar."""
        task_id = self.progress.add_task(description or name, total=total)
        self.tasks[name] = task_id
        return task_id

    def update_task(
        self, name: str, advance: float = 1, description: Optional[str] = None
    ):
        """Update a task's progress."""
        if name in self.tasks:
            self.progress.update(
                self.tasks[name], advance=advance, description=description
            )

    def log(self, message: str, level: str = "info"):
        """Log a message to the console."""
        if level == "info":
            self.console.print(f"[info]{message}[/info]")
        elif level == "warning":
            self.console.print(f"[warning]{message}[/warning]")
        elif level == "error":
            self.console.print(f"[error]{message}[/error]")
        elif level == "success":
            self.console.print(f"[success]{message}[/success]")
        else:
            self.console.print(message)
