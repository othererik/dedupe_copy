"""User Interface module using rich."""

import collections
import datetime
import logging
import os
import threading
from typing import Any, Dict, Optional, Tuple

from rich.console import Console, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text
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


class DashboardLogHandler(logging.Handler):
    """Logging handler that captures recent log messages into ConsoleUI's ring buffer."""

    def __init__(self, ui: "ConsoleUI") -> None:
        super().__init__()
        self.ui = ui

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.ui.add_log_record(record.levelname, msg)
        except Exception:  # pylint: disable=broad-except
            self.handleError(record)


class ConsoleUI:
    """Manages the rich console and progress display."""

    def __init__(
        self,
        console: Optional[Console] = None,
        dashboard: bool = False,
        log_file: Optional[str] = None,
    ) -> None:
        self.console = console or Console(theme=custom_theme)
        self.dashboard = dashboard
        self.log_file = log_file
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
            transient=not self.dashboard,
        )
        self.tasks: dict[str, TaskID] = {}
        self._lock = threading.Lock()
        self._recent_logs: collections.deque[Tuple[str, str, str]] = (
            collections.deque(maxlen=5)
        )
        self._installed_handlers: list[logging.Handler] = []
        self._stats: Dict[str, Any] = {
            "discovered": 0,
            "directories": 0,
            "accepted": 0,
            "ignored": 0,
            "errors": 0,
            "copied": 0,
            "deleted": 0,
            "rate": 0.0,
            "work_queue": 0,
            "walk_queue": 0,
            "result_queue": 0,
            "progress_queue": 0,
            "elapsed": 0.0,
        }
        self.live: Optional[Live] = None
        if self.dashboard:
            self.live = Live(
                get_renderable=self._build_dashboard_renderable,
                console=self.console,
                refresh_per_second=4,
                auto_refresh=True,
                transient=False,
                redirect_stdout=True,
                redirect_stderr=True,
            )
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging to work with rich."""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        dedupe_logger = logging.getLogger("dedupe_copy")
        configured_level = (
            dedupe_logger.level
            if dedupe_logger.level != logging.NOTSET
            else logging.INFO
        )

        existing_file_handlers = [
            h for h in dedupe_logger.handlers if isinstance(h, logging.FileHandler)
        ]
        for handler in dedupe_logger.handlers[:]:
            dedupe_logger.removeHandler(handler)
        dedupe_logger.propagate = False

        for fh in existing_file_handlers:
            dedupe_logger.addHandler(fh)

        if self.log_file and not any(
            isinstance(h, logging.FileHandler)
            and getattr(h, "baseFilename", None)
            == os.path.abspath(self.log_file)
            for h in dedupe_logger.handlers
        ):
            file_h = logging.FileHandler(
                self.log_file, mode="a", encoding="utf-8"
            )
            file_h.setLevel(
                logging.DEBUG if configured_level == logging.DEBUG else logging.INFO
            )
            file_h.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            dedupe_logger.addHandler(file_h)
            self._installed_handlers.append(file_h)

        if self.dashboard:
            dash_handler = DashboardLogHandler(self)
            dash_handler.setFormatter(logging.Formatter("%(message)s"))
            dash_handler.setLevel(configured_level)
            root_logger.addHandler(dash_handler)
            root_logger.setLevel(configured_level)
            dedupe_logger.addHandler(dash_handler)
            self._installed_handlers.append(dash_handler)
        else:
            handler = RichHandler(
                console=self.console, rich_tracebacks=True, markup=True
            )
            handler.setFormatter(
                logging.Formatter("%(message)s", datefmt="[%X]")
            )
            handler.setLevel(configured_level)
            root_logger.addHandler(handler)
            root_logger.setLevel(configured_level)
            dedupe_logger.addHandler(handler)
            self._installed_handlers.append(handler)

    def _render_header(self, stats: Dict[str, Any]) -> Panel:
        """Render the dashboard header panel."""
        elapsed_sec = stats.get("elapsed", 0.0)
        elapsed_str = str(datetime.timedelta(seconds=int(elapsed_sec)))
        header_table = Table.grid(expand=True)
        header_table.add_column(justify="left")
        header_table.add_column(justify="right")
        log_info = f" [dim](log: {self.log_file})[/dim]" if self.log_file else ""
        header_table.add_row(
            Text.from_markup(f"[bold cyan]DedupeCopy[/bold cyan]{log_info}"),
            Text.from_markup(f"[bold]Elapsed:[/bold] [yellow]{elapsed_str}[/yellow]"),
        )
        return Panel(header_table, style="blue")

    def _build_queue_panel(
        self, stats: Dict[str, Any], is_verify: bool, has_walk: bool
    ) -> Panel:
        """Construct the work left / queues panel."""
        queue_table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
        queue_table.add_column("Key", style="bold")
        queue_table.add_column("Value", justify="right")

        if is_verify:
            queue_table.add_row(
                "Files Awaiting Verification", f"{stats.get('remaining', 0):,}"
            )
            queue_table.add_row(
                "Total Files in Manifest", f"{stats.get('total_to_verify', 0):,}"
            )
            if stats.get("hashes", 0) > 0:
                queue_table.add_row("Manifest Hashes", f"{stats.get('hashes', 0):,}")
            return Panel(
                queue_table,
                title="[bold]Work Left / Manifest[/bold]",
                border_style="magenta",
            )

        no_transfers = (
            stats.get("copied", 0) == 0
            and stats.get("deleted", 0) == 0
            and stats.get("total_to_copy", 0) == 0
            and stats.get("total_to_delete", 0) == 0
        )
        if has_walk or no_transfers:
            queue_table.add_row("Directories to Walk", f"{stats.get('walk_queue', 0):,}")
            queue_table.add_row("Files Awaiting Hash", f"{stats.get('work_queue', 0):,}")
            queue_table.add_row("Results to Process", f"{stats.get('result_queue', 0):,}")
        if stats.get("remaining_copy", 0) > 0 or stats.get("total_to_copy", 0) > 0:
            queue_table.add_row(
                "Files Awaiting Copy", f"{stats.get('remaining_copy', 0):,}"
            )
        if stats.get("remaining_delete", 0) > 0 or stats.get("total_to_delete", 0) > 0:
            queue_table.add_row(
                "Files Awaiting Deletion", f"{stats.get('remaining_delete', 0):,}"
            )
        if has_walk or stats.get("progress_queue", 0) > 0:
            queue_table.add_row("Progress Messages", f"{stats.get('progress_queue', 0):,}")
        if stats.get("manifest_hashes", 0) > 0 and not has_walk:
            queue_table.add_row(
                "Manifest Hashes", f"{stats.get('manifest_hashes', 0):,}"
            )

        return Panel(
            queue_table,
            title="[bold]Work Left / Queues[/bold]",
            border_style="magenta",
        )

    def _build_stat_panel(
        self, stats: Dict[str, Any], is_verify: bool, has_walk: bool
    ) -> Panel:
        """Construct the statistics and throughput panel."""
        stat_table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
        stat_table.add_column("Metric", style="bold")
        stat_table.add_column("Count", justify="right")

        if is_verify:
            stat_table.add_row(
                "Files Verified", f"[green]{stats.get('verified', 0):,}[/green]"
            )
            if stats.get("errors", 0) > 0:
                stat_table.add_row(
                    "Errors Encountered",
                    f"[bold red]{stats.get('errors', 0):,}[/bold red]",
                )
        else:
            no_transfers = (
                stats.get("copied", 0) == 0
                and stats.get("deleted", 0) == 0
                and stats.get("total_to_copy", 0) == 0
                and stats.get("total_to_delete", 0) == 0
            )
            if has_walk or no_transfers:
                stat_table.add_row("Files Discovered", f"{stats.get('discovered', 0):,}")
                stat_table.add_row("Directories Scanned", f"{stats.get('directories', 0):,}")
                stat_table.add_row(
                    "Files Accepted", f"[green]{stats.get('accepted', 0):,}[/green]"
                )
                stat_table.add_row(
                    "Files Ignored", f"[yellow]{stats.get('ignored', 0):,}[/yellow]"
                )
            if stats.get("copied", 0) > 0 or stats.get("total_to_copy", 0) > 0:
                stat_table.add_row(
                    "Files Copied", f"[cyan]{stats.get('copied', 0):,}[/cyan]"
                )
            if stats.get("deleted", 0) > 0 or stats.get("total_to_delete", 0) > 0:
                stat_table.add_row(
                    "Files Deleted", f"[red]{stats.get('deleted', 0):,}[/red]"
                )
            if stats.get("collisions", 0) > 0 and not has_walk:
                stat_table.add_row(
                    "Collisions Found",
                    f"[yellow]{stats.get('collisions', 0):,}[/yellow]",
                )
            if stats.get("errors", 0) > 0:
                stat_table.add_row(
                    "Errors Encountered",
                    f"[bold red]{stats.get('errors', 0):,}[/bold red]",
                )

        rate = stats.get("rate", 0.0)
        stat_table.add_row("Processing Rate", f"{rate:.1f} files/sec")
        return Panel(
            stat_table,
            title="[bold]Statistics & Throughput[/bold]",
            border_style="green",
        )

    def _render_queues_and_stats(self, stats: Dict[str, Any]) -> Table:
        """Render the middle two-column table for queues and stats."""
        mid_table = Table.grid(expand=True)
        mid_table.add_column(ratio=1)
        mid_table.add_column(ratio=1)

        is_verify = stats.get("workflow") == "verify"
        has_walk = (
            stats.get("discovered", 0) > 0
            or stats.get("directories", 0) > 0
            or stats.get("accepted", 0) > 0
            or stats.get("ignored", 0) > 0
            or stats.get("walk_queue", 0) > 0
            or stats.get("work_queue", 0) > 0
            or stats.get("result_queue", 0) > 0
        )

        queue_panel = self._build_queue_panel(stats, is_verify, has_walk)
        stat_panel = self._build_stat_panel(stats, is_verify, has_walk)

        mid_table.add_row(queue_panel, stat_panel)
        return mid_table

    def _render_activity(self, logs: list[Tuple[str, str, str]]) -> Panel:
        """Render recent activity logs panel."""
        log_table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
        log_table.add_column("Time", style="dim", width=9)
        log_table.add_column("Level", width=9)
        log_table.add_column("Message", ratio=1, overflow="ellipsis", no_wrap=True)
        if logs:
            for time_str, level, msg in logs:
                style = (
                    "green"
                    if level == "INFO"
                    else (
                        "yellow"
                        if level == "WARNING"
                        else (
                            "bold red"
                            if level in ("ERROR", "CRITICAL")
                            else "dim"
                        )
                    )
                )
                clean_msg = msg.replace("\n", " ").strip()
                log_table.add_row(
                    Text(time_str, style="dim"),
                    Text(level, style=style),
                    Text(clean_msg, overflow="ellipsis", no_wrap=True),
                )
        else:
            log_table.add_row(
                "",
                Text("INFO", style="dim"),
                Text("Starting operations...", style="dim"),
            )

        act_title = "[bold]Recent Activity[/bold]"
        if self.log_file:
            act_title += f" [dim](full log in {self.log_file})[/dim]"
        return Panel(log_table, title=act_title, border_style="dim")

    def _build_dashboard_renderable(self) -> Group:
        """Constructs the composite renderable representing the dashboard."""
        with self._lock:
            stats = dict(self._stats)
            logs = list(self._recent_logs)

        header_panel = self._render_header(stats)
        progress_panel = Panel(
            self.progress, title="[bold]Progress[/bold]", border_style="cyan"
        )
        mid_table = self._render_queues_and_stats(stats)
        act_panel = self._render_activity(logs)

        return Group(header_panel, progress_panel, mid_table, act_panel)

    def start(self) -> None:
        """Start the progress display or live dashboard."""
        if self.dashboard and self.live is not None:
            self.live.start()
        else:
            self.progress.start()

    def stop(self) -> None:
        """Stop the progress display or live dashboard."""
        if self.dashboard and self.live is not None:
            self.live.stop()
        else:
            self.progress.stop()
        self._cleanup_logging()

    def _cleanup_logging(self) -> None:
        """Remove logging handlers installed by this ConsoleUI instance."""
        root_logger = logging.getLogger()
        dedupe_logger = logging.getLogger("dedupe_copy")
        for h in self._installed_handlers:
            if h in dedupe_logger.handlers:
                dedupe_logger.removeHandler(h)
            if h in root_logger.handlers:
                root_logger.removeHandler(h)
            try:
                h.close()
            except (OSError, ValueError):
                pass
        self._installed_handlers.clear()

    def add_task(
        self, name: str, total: Optional[float] = None, description: str = ""
    ) -> TaskID:
        """Add a new task to the progress bar."""
        task_id = self.progress.add_task(description or name, total=total)
        self.tasks[name] = task_id
        return task_id

    def update_task(
        self,
        name: str,
        advance: float = 1,
        description: Optional[str] = None,
        total: Optional[float] = None,
    ) -> None:
        """Update a task's progress."""
        if name in self.tasks:
            self.progress.update(
                self.tasks[name],
                advance=advance,
                description=description,
                total=total,
            )

    def update_stats(self, **kwargs: Any) -> None:
        """Update live statistics displayed on the dashboard."""
        with self._lock:
            self._stats.update(kwargs)

    def add_log_record(self, level: str, message: str) -> None:
        """Add a log record to the recent activity ring buffer."""
        now = datetime.datetime.now().strftime("%H:%M:%S")
        with self._lock:
            self._recent_logs.append((now, level, message))

    def log(self, message: str, level: str = "info") -> None:
        """Log a message to the console or dashboard ring buffer."""
        if self.dashboard:
            self.add_log_record(level.upper(), message)
        else:
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


def _run_demo() -> None:
    """Run an interactive demonstration of the Dashboard UI."""
    import time  # pylint: disable=import-outside-toplevel

    demo_ui = ConsoleUI(dashboard=True, log_file="demo_dedupe_copy.log")
    demo_ui.start()
    demo_events = [
        (0.5, "INFO", "Scanning /Volumes/Archive/Photos"),
        (1.0, "INFO", "Discovered directory tree: 24 subfolders"),
        (1.5, "INFO", "Accepted 450 unique image files"),
        (2.0, "WARNING", "Permission denied reading system cache; skipping"),
        (2.6, "INFO", "Deduplicating: found 50 colliding hashes"),
        (3.2, "INFO", "Beginning parallel copy phase (8 workers)"),
        (3.8, "INFO", "Copied 'IMG_2026_01.RAW' -> '/Backup/2026_01/'"),
        (4.4, "ERROR", "I/O timeout on network destination; retrying"),
        (5.0, "INFO", "Retry succeeded; resumed transfer"),
        (5.6, "INFO", "Copy completed; finalizing SQLite manifest"),
    ]
    try:
        demo_ui.add_task("Walking filesystem...", total=1000)
        demo_ui.add_task("Copying files...", total=500)
        start = time.time()
        for step in range(1, 41):
            time.sleep(0.15)
            elapsed = time.time() - start
            walk_prog = min(1000, step * 25)
            copy_prog = min(500, max(0, (step - 15) * 20))
            demo_ui.update_task("Walking filesystem...", advance=25)
            if step > 15:
                demo_ui.update_task("Copying files...", advance=20)
            for t_ev, lvl, msg in demo_events:
                if (step - 1) * 0.15 <= t_ev < step * 0.15:
                    demo_ui.add_log_record(lvl, msg)
            demo_ui.update_stats(
                discovered=walk_prog * 15,
                directories=24,
                accepted=int(walk_prog * 12.5),
                ignored=walk_prog * 2,
                copied=copy_prog,
                total_to_copy=500,
                remaining_copy=max(0, 500 - copy_prog),
                deleted=0,
                errors=1 if step > 30 else 0,
                rate=walk_prog * 15 / elapsed if elapsed > 0 else 0.0,
                work_queue=max(0, 1000 - walk_prog),
                walk_queue=max(0, 24 - step),
                result_queue=max(0, (walk_prog - copy_prog) // 5),
                progress_queue=3,
                elapsed=elapsed,
            )
    finally:
        demo_ui.stop()
        if os.path.exists("demo_dedupe_copy.log"):
            try:
                os.remove("demo_dedupe_copy.log")
            except OSError:
                pass


if __name__ == "__main__":
    _run_demo()
