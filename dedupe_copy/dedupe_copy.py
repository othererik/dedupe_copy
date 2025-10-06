"""Find duplicate files on a file system


work_queue -> files put on here by WalkThread(s), read by ReadThread(s)
result_queue -> Put to by ReadThreads(s) read by ResultProcessor
progress_queue -> read by ProgressThread and put to by all others
walk_queue -> holds directories discovered by WalkThread(s)
copy_queue -> read by CopyThread(s) put to by queue_copy_work
"""

# pylint: disable=too-many-lines

from collections import defaultdict, Counter
import datetime
import fnmatch
import functools
import hashlib
import logging
import os
import queue
import random
import shutil
import sys
import tempfile
import threading
import time
from typing import Dict, List, Optional, Tuple, Set, Callable, Any, Iterator, Union

from .disk_cache_dict import CacheDict, DefaultCacheDict

# Configure module logger
logger = logging.getLogger(__name__)


# Default logging setup for programmatic use (tests, API calls)
def _ensure_logging_configured() -> None:
    """Ensure logging is configured with sensible defaults if not already set"""
    root_logger = logging.getLogger("dedupe_copy")
    if not root_logger.handlers:
        # No handlers configured yet, set up basic configuration
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)
        root_logger.propagate = False


def _format_error_message(path: str, error: Union[str, Exception]) -> str:
    """Format an error message with helpful context and suggestions

    Args:
        path: File path that caused the error
        error: The error or exception that occurred

    Returns:
        Formatted error message with suggestions
    """
    error_str = str(error)
    error_type = type(error).__name__ if isinstance(error, Exception) else "Error"

    # Build helpful message based on error type
    suggestions = []
    if isinstance(error, PermissionError) or "Permission denied" in error_str:
        suggestions.append("Check file permissions")
        suggestions.append("Ensure you have read access to source files")
        suggestions.append("Ensure you have write access to destination")
    elif isinstance(error, FileNotFoundError) or "No such file" in error_str:
        suggestions.append("File may have been deleted during processing")
        suggestions.append("Check if path is on a network share that disconnected")
    elif isinstance(error, OSError) and "Errno 28" in error_str:  # No space left
        suggestions.append("Destination disk is full")
        suggestions.append("Free up space or choose a different destination")
    elif isinstance(error, (IOError, OSError)):
        suggestions.append("Check disk health and connection")
        suggestions.append("For network paths, verify network stability")

    msg = f"Error processing {repr(path)}: [{error_type}] {error_str}"
    if suggestions:
        msg += "\n  Suggestions: " + "; ".join(suggestions)

    return msg


# For message output
HIGH_PRIORITY = 1
MEDIUM_PRIORITY = 5
LOW_PRIORITY = 10

MAX_TARGET_QUEUE_SIZE = 50000

READ_CHUNK = 1048576  # 1 MB

INCREMENTIAL_SAVE_SIZE = 50000
PATH_RULES = {
    "mtime": "Put each file into a directory of the form YYYY_MM",
    "no_change": 'Preserve directory structure from "read_path" up',
    "extension": "Put all items into directories of their extension",
}


# Path rule matching


def _path_rules_parser(
    rules: Dict[str, List[str]],
    dest_dir: str,
    extension: str,
    mtime_str: str,
    _size: int,
    *,
    source_dirs: str,
    src: str,
    read_paths: List[str],
) -> Tuple[str, str]:
    """Builds a path based on the path rules for the given extension pattern"""
    ext_key = extension if extension else ""
    best_match_key = _best_match(rules.keys(), ext_key)
    rule_list = (
        rules.get(best_match_key, ["no_change"]) if best_match_key else ["no_change"]
    )
    dest = None
    for rule in rule_list:
        if rule == "mtime":
            dest_dir = os.path.join(dest_dir, mtime_str)
        elif rule == "extension":
            dest_dir = os.path.join(dest_dir, extension)
        elif rule == "no_change":
            # remove the path up to our source_dirs from src so we don't
            # preserve the structure "below" where our copy is from
            for p in read_paths:
                if source_dirs.startswith(p):
                    source_dirs = source_dirs.replace(p, "", 1)
            if source_dirs.startswith(os.sep):
                source_dirs = source_dirs[1:]
            dest_dir = os.path.join(dest_dir, source_dirs)
    if dest is None:
        dest = os.path.join(dest_dir, src)
    return dest, dest_dir


def _best_match(extensions: Any, ext: str) -> Optional[str]:
    """Returns the best matching extension_pattern for ext from a list of
    extension patterns or none if no extension applies
    """
    ext = f"*.{ext}"
    if ext in extensions:
        return ext
    matches = []
    for extension_pattern in extensions:
        if fnmatch.fnmatch(ext, extension_pattern):
            matches.append(extension_pattern)
    if not matches:
        return None
    # take the pattern that is the closest to the given extension by length
    best = matches.pop()
    score = abs(len(best.replace("?", "").replace("*", "")) - len(ext))
    for m in matches:
        current_score = abs(len(m.replace("?", "").replace("*", "")) - len(ext))
        if current_score < score:
            score = current_score
            best = m
    return best


def _match_extension(extensions: Optional[List[str]], fn: str) -> bool:
    """Returns true if extensions is empty"""
    if not extensions:
        return True
    for included_pattern in extensions:
        # first look for an exact match
        if fn.lower().endswith(included_pattern):
            return True
        # now try a pattern match
        if fnmatch.fnmatch(fn.lower(), included_pattern):
            return True
    return False


def _throttle_puts(current_size: int) -> None:
    """Delay for some factor to avoid overloading queues"""
    time.sleep(min((current_size * 2) / float(MAX_TARGET_QUEUE_SIZE), 60))


# Worker threads


class CopyThread(threading.Thread):
    """Copy to target_path for given extensions (all if None)"""

    def __init__(
        self,
        work_queue: "queue.Queue[Tuple[str, str, int]]",
        target_path: str,
        read_paths: List[str],
        stop_event: threading.Event,
        *,
        extensions: Optional[List[str]] = None,
        path_rules: Optional[Callable[..., Tuple[str, str]]] = None,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        preserve_stat: bool = False,
    ) -> None:
        super().__init__()
        self.work = work_queue
        self.target_path = target_path
        self.extensions = extensions
        self.path_rules = path_rules
        self.read_paths = read_paths
        self.stop_event = stop_event
        self.progress_queue = progress_queue
        self.preserve_stat = preserve_stat
        self.daemon = True

    def run(self) -> None:  # pylint: disable=too-complex,too-many-branches
        while not self.work.empty() or not self.stop_event.is_set():
            try:
                src, mtime, size = self.work.get(True, 0.1)
                ext = lower_extension(src)
                if not ext:
                    ext = "no_extension"
                if not _match_extension(self.extensions, src):
                    continue
                if self.path_rules is not None:
                    source_dirs = os.path.dirname(src)
                    dest, dest_dir = self.path_rules(
                        self.target_path,
                        ext,
                        mtime,
                        size,
                        source_dirs=source_dirs,
                        src=os.path.basename(src),
                        read_paths=self.read_paths,
                    )
                else:
                    dest = os.path.join(
                        self.target_path, ext, mtime, os.path.basename(src)
                    )
                    dest_dir = os.path.dirname(dest)
                try:
                    if not os.path.exists(dest_dir):
                        try:
                            os.makedirs(dest_dir)
                        except OSError:
                            # thread race if it now exists, no issue
                            if not os.path.exists(dest_dir):
                                raise
                    if self.preserve_stat:
                        shutil.copy2(src, dest)
                    else:
                        shutil.copyfile(src, dest)
                    if self.progress_queue:
                        self.progress_queue.put((LOW_PRIORITY, "copied", src, dest))
                except (OSError, IOError, shutil.Error) as e:
                    if self.progress_queue:
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                src,
                                f"Error copying to {repr(dest)}: {e}",
                            )
                        )
            except queue.Empty:
                pass


class ResultProcessor(threading.Thread):
    """Takes results of work queue and builds result data structure"""

    def __init__(
        self,
        stop_event: threading.Event,
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        collisions: Any,
        manifest: Any,
        *,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        keep_empty: bool = False,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        super().__init__()
        self.stop_event = stop_event
        self.results = result_queue
        self.collisions = collisions
        self.md5_data = manifest
        self.progress_queue = progress_queue
        self.empty = keep_empty
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:  # pylint: disable=too-complex
        processed = 0
        while not self.results.empty() or not self.stop_event.is_set():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = ""
            try:
                md5, size, mtime, src = self.results.get(True, 0.1)
                collision = md5 in self.md5_data
                if self.empty and md5 == "d41d8cd98f00b204e9800998ecf8427e":
                    collision = False
                self.md5_data[md5].append([src, size, mtime])
                if collision:
                    self.collisions[md5] = self.md5_data[md5]
                processed += 1
            except queue.Empty:
                pass
            except (KeyError, ValueError, TypeError) as err:
                if self.progress_queue:
                    self.progress_queue.put(
                        (
                            MEDIUM_PRIORITY,
                            "error",
                            src,
                            f"ERROR in result processing: {err}",
                        )
                    )
            if processed > INCREMENTIAL_SAVE_SIZE:
                if self.progress_queue:
                    self.progress_queue.put(
                        (
                            HIGH_PRIORITY,
                            "message",
                            "Hit incremental save size, " "will save manifest files",
                        )
                    )
                processed = 0
                try:
                    self.md5_data.save()
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                self.md5_data.path,
                                f"ERROR Saving incremental: {e}",
                            )
                        )


class ReadThread(threading.Thread):
    """Thread worker for hashing"""

    def __init__(
        self,
        work_queue: "queue.Queue[str]",
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        stop_event: threading.Event,
        *,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        super().__init__()
        self.work = work_queue
        self.results = result_queue
        self.stop_event = stop_event
        self.progress_queue = progress_queue
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        while not self.stop_event.is_set() or not self.work.empty():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = ""
            try:
                src = self.work.get(True, 0.1)
                try:
                    _throttle_puts(self.results.qsize())
                    self.results.put(read_file(src))
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        self.progress_queue.put((MEDIUM_PRIORITY, "error", src, e))
            except queue.Empty:
                pass
            except (OSError, IOError, ValueError, TypeError) as err:
                if self.progress_queue:
                    self.progress_queue.put(
                        (MEDIUM_PRIORITY, "error", src, f"ERROR in file read: {err},")
                    )


class ProgressThread(threading.Thread):
    """All Status updates should come through here.
    Can process raw messages as well as message of the type:
        message, msg
        file, path
        accepted, path
        ignored, path, reason
        error, path, reason
        copied, src, dest
        not_copied, path
    """

    file_count_log_interval = 1000

    def __init__(
        self,
        work_queue: "queue.Queue[str]",
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        progress_queue: "queue.PriorityQueue[Any]",
        *,
        walk_queue: "queue.Queue[str]",
        stop_event: threading.Event,
        save_event: threading.Event,
    ) -> None:

        super().__init__()
        self.work = work_queue
        self.result_queue = result_queue
        self.progress_queue = progress_queue
        self.walk_queue = walk_queue
        self.stop_event = stop_event
        self.daemon = True
        self.last_accepted: Optional[str] = None
        self.file_count = 0
        self.directory_count = 0
        self.accepted_count = 0
        self.ignored_count = 0
        self.error_count = 0
        self.copied_count = 0
        self.not_copied_count = 0
        self.last_copied: Optional[str] = None
        self.save_event = save_event
        # Performance tracking
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.bytes_processed = 0

    def do_log_dir(self, _path: str) -> None:
        """Log directory processing."""
        self.directory_count += 1

    def do_log_file(self, path: str) -> None:
        """Log file discovery and progress."""
        self.file_count += 1
        if self.file_count % self.file_count_log_interval == 0 or self.file_count == 1:
            elapsed = time.time() - self.start_time
            files_per_sec = self.file_count / elapsed if elapsed > 0 else 0
            message = (
                f"Discovered {self.file_count} files (dirs: {self.directory_count}), accepted "
                f"{self.accepted_count}. Rate: {files_per_sec:.1f} files/sec\n"
                f"Work queue has {self.work.qsize()} items. "
                f"Progress queue has {self.progress_queue.qsize()} items. Walk queue has "
                f"{self.walk_queue.qsize()} items.\nCurrent file: {repr(path)} "
                f"(last accepted: {repr(self.last_accepted)})"
            )
            logger.info(message)

    def do_log_copied(self, src: str, dest: str) -> None:
        """Log successful file copy operations."""
        self.copied_count += 1
        if (
            self.copied_count % self.file_count_log_interval == 0
            or self.copied_count == 1
        ):
            elapsed = time.time() - self.start_time
            copy_rate = self.copied_count / elapsed if elapsed > 0 else 0
            logger.info(
                f"Copied {self.copied_count} items. Skipped {self.not_copied_count} items. "
                f"Rate: {copy_rate:.1f} files/sec\n"
                f"Last file: {repr(src)} -> {repr(dest)}"
            )
        self.last_copied = src

    def do_log_not_copied(self, _path: str) -> None:
        """Log files that were not copied."""
        self.not_copied_count += 1

    def do_log_accepted(self, path: str) -> None:
        """Log files that were accepted for processing."""
        self.accepted_count += 1
        self.last_accepted = path

    def do_log_ignored(self, path: str, reason: str) -> None:
        """Log files that were ignored during processing."""
        self.ignored_count += 1
        logger.info(f"Ignoring {repr(path)} for {repr(reason)}")

    def do_log_error(self, path: str, reason: Union[str, Exception]) -> None:
        """Log files that caused errors during processing."""
        self.error_count += 1
        error_msg = _format_error_message(path, reason)
        logger.error(error_msg)

    @staticmethod
    def do_log_message(message: str) -> None:
        """Log a generic message."""
        logger.info(message)

    def run(self) -> None:  # pylint: disable=too-complex
        """Run loop that slurps items off the progress queue and dispatches
        the correct handler
        """
        last_update = time.time()
        while not self.stop_event.is_set() or not self.progress_queue.empty():
            try:
                # we should only be getting directories on this queue
                item = self.progress_queue.get(True, 0.1)[1:]
                method_name = f"do_log_{item[0]}"
                method = getattr(self, method_name)
                method(*item[1:])
                last_update = time.time()
            except queue.Empty:
                if self.save_event and self.save_event.is_set():
                    logger.info("Saving...")
                    time.sleep(1)
                if time.time() - last_update > 60:
                    last_update = time.time()
                    work_size = self.work.qsize()
                    result_size = self.result_queue.qsize()
                    progress_size = self.progress_queue.qsize()
                    walk_size = self.walk_queue.qsize()

                    logger.debug(
                        f"Status:  Work Queue Size: {work_size}. "
                        f"Result Queue Size: {result_size}. "
                        f"Progress Queue Size: {progress_size}. "
                        f"Walk Queue Size: {walk_size}."
                    )

                    # Warn about large queue sizes (potential memory issues)
                    if work_size > 40000:
                        logger.warning(
                            f"Work queue is large ({work_size} items). "
                            f"Consider reducing thread counts to avoid memory issues."
                        )
                    if progress_size > 10000:
                        logger.warning(
                            f"Progress queue is backing up ({progress_size} items). "
                            f"This may indicate slow processing."
                        )
            except (AttributeError, ValueError) as e:
                logger.error(f"Failed in progress thread: {e}")

        # Final summary with timing and rates
        elapsed = time.time() - self.start_time
        if self.file_count:
            logger.info("=" * 60)
            logger.info("RESULTS FROM WALK:")
            logger.info(f"Total files discovered: {self.file_count}")
            logger.info(f"Total accepted: {self.accepted_count}")
            logger.info(f"Total ignored: {self.ignored_count}")
            files_per_sec = self.file_count / elapsed if elapsed > 0 else 0
            logger.info(f"Average discovery rate: {files_per_sec:.1f} files/sec")
        if self.copied_count:
            logger.info("-" * 60)
            logger.info("RESULTS FROM COPY:")
            logger.info(f"Total copied: {self.copied_count}")
            logger.info(f"Total skipped: {self.not_copied_count}")
            copy_rate = self.copied_count / elapsed if elapsed > 0 else 0
            logger.info(f"Average copy rate: {copy_rate:.1f} files/sec")
        if self.error_count:
            logger.info("-" * 60)
        logger.info(f"Total errors: {self.error_count}")
        logger.info(
            f"Total elapsed time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)"
        )
        logger.info("=" * 60)


class WalkThread(threading.Thread):
    """Thread that walks directory trees to discover files."""

    def __init__(
        self,
        walk_queue: "queue.Queue[str]",
        stop_event: threading.Event,
        extensions: Optional[List[str]],
        ignore: Optional[List[str]],
        *,
        work_queue: "queue.Queue[str]",
        already_processed: Any,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        super().__init__()
        self.walk = walk_queue
        self.work = work_queue
        self.extensions = extensions
        self.ignore = ignore
        self.already_processed = already_processed
        self.progress = progress_queue
        self.stop_event = stop_event
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        while not self.walk.empty() or not self.stop_event.is_set():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = None
            try:
                # we should only be getting directories on this queue
                src = self.walk.get(True, 0.5)
                if not os.path.exists(src):
                    # are we dealing with a network path, retry after sleeping
                    time.sleep(3)
                    if not os.path.exists(src):
                        raise RuntimeError(
                            f"Directory disappeared during walk: {src!r}"
                        )
                if not os.path.isdir(src):
                    raise ValueError(f"Unexpected file in work queue: {src!r}")
                # process the files, sending items off to be read and getting
                # new directories put onto the queue we read from
                _distribute_work(
                    src,
                    self.already_processed,
                    self.ignore,
                    extensions=self.extensions,
                    progress_queue=self.progress,
                    work_queue=self.work,
                    walk_queue=self.walk,
                )
            except queue.Empty:
                pass
            except (OSError, ValueError, RuntimeError) as e:
                if self.progress:
                    self.progress.put((MEDIUM_PRIORITY, "error", src, e))


# walk / hash helpers


def _distribute_work(  # pylint: disable=too-complex,too-many-branches
    src: str,
    already_processed: Any,
    ignore: Optional[List[str]],
    *,
    extensions: Optional[List[str]],
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    work_queue: "queue.Queue[str]",
    walk_queue: "queue.Queue[str]",
) -> None:
    # if the path is excluded, don't traverse
    if ignore:
        for ignored_pattern in ignore:
            if fnmatch.fnmatch(src, ignored_pattern):
                if progress_queue:
                    progress_queue.put((HIGH_PRIORITY, "ignored", src, ignored_pattern))
                return
    for item in os.listdir(src):
        fn = os.path.join(src, item)
        if os.path.isdir(fn):
            if progress_queue:
                progress_queue.put((LOW_PRIORITY, "dir", fn))
            _throttle_puts(walk_queue.qsize())
            walk_queue.put(fn)
            continue
        if progress_queue:
            progress_queue.put((LOW_PRIORITY, "file", fn))
        # first check if this has already been processed, then
        # check the ignore file pattern first, then check if we
        # need to process the extension pattern
        action_required = True
        if fn in already_processed:
            action_required = False
        if ignore and action_required:
            for ignored_pattern in ignore:
                if fnmatch.fnmatch(fn, ignored_pattern):
                    action_required = False
                    if progress_queue:
                        progress_queue.put(
                            (HIGH_PRIORITY, "ignored", fn, ignored_pattern)
                        )
                    break
        if extensions and action_required:
            if not _match_extension(extensions, fn):
                action_required = False  # didn't find a match
        if action_required:
            _throttle_puts(walk_queue.qsize())
            work_queue.put(fn)
            if progress_queue:
                progress_queue.put((HIGH_PRIORITY, "accepted", fn))


def _walk_fs(
    read_paths: List[str],
    extensions: Optional[List[str]],
    ignore: Optional[List[str]],
    *,
    work_queue: "queue.Queue[str]",
    walk_queue: Optional["queue.Queue[str]"] = None,
    already_processed: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    walk_threads: int = 4,
    save_event: Optional[threading.Event] = None,
) -> None:
    if walk_queue is None:
        walk_queue = queue.Queue()
    walk_done = threading.Event()
    walkers = []
    if progress_queue:
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Starting {walk_threads} walk workers")
        )
    for _ in range(walk_threads):
        w = WalkThread(
            walk_queue,
            walk_done,
            extensions,
            ignore,
            work_queue=work_queue,
            already_processed=already_processed,
            progress_queue=progress_queue,
            save_event=save_event,
        )
        walkers.append(w)
        w.start()
    for src in read_paths:
        _throttle_puts(walk_queue.qsize())
        walk_queue.put(src)
    walk_done.set()
    for w in walkers:
        w.join()


def lower_extension(src: str) -> str:
    """Extract and return the lowercase file extension."""
    _, extension = os.path.splitext(src)
    return extension[1:].lower()


def hash_file(src: str) -> str:
    """Hash a file, returning the md5 hexdigest

    :param src: Full path of the source file.
    :type src: str
    """
    checksum = hashlib.md5()
    with open(src, "rb") as inhandle:
        chunk = inhandle.read(READ_CHUNK)
        while chunk:
            checksum.update(chunk)
            chunk = inhandle.read(READ_CHUNK)
    return checksum.hexdigest()


def read_file(src: str) -> Tuple[str, int, float, str]:
    """Read file and return its metadata including checksum and size."""
    size = os.path.getsize(src)
    mtime = os.path.getmtime(src)
    md5 = hash_file(src)
    return (md5, size, mtime, src)


def _extension_report(md5_data: Any, show_count: int = 10) -> int:
    """Print details for each extension, sorted by total size, return
    total size for all extensions
    """
    sizes: Counter[str] = Counter()
    extension_counts: Counter[str] = Counter()
    for key, info in md5_data.items():
        for items in info:
            extension = lower_extension(items[0])
            if not extension:
                extension = "no_extension"
            sizes[extension] += items[1]
            extension_counts[extension] += 1
    logger.info(f"Top {show_count} extensions by size:")
    for key, _ in zip(
        sorted(sizes, key=lambda x: sizes.get(x, 0), reverse=True), range(show_count)
    ):
        logger.info(f"  {key}: {sizes[key]} bytes")
    logger.info(f"Top {show_count} extensions by count:")
    for key, _ in zip(
        sorted(
            extension_counts, key=lambda x: extension_counts.get(x, 0), reverse=True
        ),
        range(show_count),
    ):
        logger.info(f"  {key}: {extension_counts[key]}")
    return sum(sizes.values())


# duplicate finding


def find_duplicates(  # pylint: disable=too-complex,too-many-branches
    read_paths: List[str],
    work_queue: "queue.Queue[str]",
    result_queue: "queue.Queue[Tuple[str, int, float, str]]",
    manifest: Any,
    collisions: Any,
    *,
    result_src: Optional[str] = None,
    extensions: Optional[List[str]] = None,
    ignore: Optional[List[str]] = None,
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    walk_threads: int = 4,
    read_threads: int = 8,
    keep_empty: bool = False,
    save_event: Optional[threading.Event] = None,
    walk_queue: Optional["queue.Queue[str]"] = None,
) -> Tuple[Any, Any]:
    """Find duplicate files by comparing checksums across directories."""
    work_stop_event = threading.Event()
    result_stop_event = threading.Event()
    result_processor = ResultProcessor(
        result_stop_event,
        result_queue,
        collisions,
        manifest,
        keep_empty=keep_empty,
        progress_queue=progress_queue,
        save_event=save_event,
    )
    result_processor.start()
    result_fh = None
    if result_src is not None:
        result_fh = open(result_src, "ab")  # pylint: disable=consider-using-with
        result_fh.write(f"Src: {read_paths}\n".encode("utf-8"))
        result_fh.write("Collision #, MD5, Path, Size (bytes), mtime\n".encode("utf-8"))
    try:
        if progress_queue:
            progress_queue.put(
                (HIGH_PRIORITY, "message", f"Starting {read_threads} read workers")
            )
        work_threads = []
        for _ in range(read_threads):
            w = ReadThread(
                work_queue,
                result_queue,
                work_stop_event,
                progress_queue=progress_queue,
                save_event=save_event,
            )
            work_threads.append(w)
            w.start()
        _walk_fs(
            read_paths,
            extensions,
            ignore,
            work_queue=work_queue,
            walk_queue=walk_queue,
            already_processed=manifest.read_sources,
            progress_queue=progress_queue,
            walk_threads=walk_threads,
            save_event=save_event,
        )
        while not work_queue.empty():
            if progress_queue:
                progress_queue.put(
                    (
                        HIGH_PRIORITY,
                        "message",
                        f"Waiting for work queue to empty: {work_queue.qsize()} "
                        f"items remain",
                    )
                )
            time.sleep(5)
        work_stop_event.set()
        # let the workers finish
        for worker in work_threads:
            worker.join()
        result_stop_event.set()
        while result_processor.is_alive():
            result_processor.join(5)  # wait for result processor to complete
        if collisions:
            group = 0
            logger.info("Hash Collisions:")
            for md5, info in collisions.items():
                group += 1
                logger.info(f"  MD5: {md5}")
                for item in info:
                    logger.info(f"    {repr(item[0])}, {item[1]}")
                    if result_fh:
                        line = (
                            f"{group}, {md5}, {repr(item[0])}, {item[1]}, {item[2]}\n"
                        )
                        result_fh.write(line.encode("utf-8"))
        else:
            logger.info("No Duplicates Found")
        return (collisions, manifest)
    finally:
        if result_fh:
            result_fh.close()


def info_parser(data: Any) -> Iterator[Tuple[str, str, str, int]]:
    """Yields (MD5, path, mtime_string, size) tuples from a md5_data
    dictionary"""
    if data:
        for md5, info in data.items():
            for item in info:
                try:
                    time_stamp = datetime.datetime.fromtimestamp(item[2])
                    year_month = f"{time_stamp.year}_{time_stamp.month:0>2}"
                except (OverflowError, OSError, ValueError) as e:
                    logger.error(f"ERROR: {repr(item[0])} {e}")
                    year_month = "Unknown"
                yield md5, item[0], year_month, item[1]


# copy core


def queue_copy_work(  # pylint: disable=too-complex
    copy_queue: "queue.Queue[Tuple[str, str, int]]",
    data: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    copied: Any,
    *,
    ignore: Optional[List[str]] = None,
    ignore_empty_files: bool = False,
) -> Any:
    """Queue copy operations for file duplication."""
    for md5, path, mtime, size in info_parser(data):
        if md5 not in copied:
            action_required = True
            if ignore:
                for ignored_pattern in ignore:
                    if fnmatch.fnmatch(path, ignored_pattern):
                        action_required = False
                        break
            if action_required:
                if not ignore_empty_files:
                    copied[md5] = None
                elif md5 != "d41d8cd98f00b204e9800998ecf8427e":
                    copied[md5] = None
                _throttle_puts(copy_queue.qsize())
                copy_queue.put((path, mtime, size))
            elif progress_queue:
                progress_queue.put((LOW_PRIORITY, "not_copied", path))
        elif progress_queue:
            progress_queue.put((LOW_PRIORITY, "not_copied", path))
    return copied


def copy_data(
    dupes: Any,
    all_data: Any,
    target_base: str,
    read_paths: List[str],
    *,
    copy_threads: int = 8,
    extensions: Optional[List[str]] = None,
    path_rules: Optional[Callable[..., Tuple[str, str]]] = None,
    ignore: Optional[List[str]] = None,
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    no_copy: Optional[Any] = None,
    ignore_empty_files: bool = False,
    preserve_stat: bool = False,
) -> None:
    """Queues up the copy work, waits for threads to finish"""
    stop_event = threading.Event()
    copy_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue()
    workers = []
    copied = no_copy
    if progress_queue:
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Starting {copy_threads} copy workers")
        )
    for _ in range(copy_threads):
        c = CopyThread(
            copy_queue,
            target_base,
            read_paths,
            stop_event,
            extensions=extensions,
            path_rules=path_rules,
            progress_queue=progress_queue,
            preserve_stat=preserve_stat,
        )
        workers.append(c)
        c.start()
    if progress_queue:
        progress_queue.put((HIGH_PRIORITY, "message", f"Copying to {target_base}"))
    # copied is passed to here so we don't try to copy "comparison" manifests
    # copied is a dict-like, so it's update in place
    queue_copy_work(
        copy_queue,
        dupes,
        progress_queue,
        copied,
        ignore=ignore,
        ignore_empty_files=ignore_empty_files,
    )
    queue_copy_work(
        copy_queue,
        all_data,
        progress_queue,
        copied,
        ignore=ignore,
        ignore_empty_files=ignore_empty_files,
    )
    stop_event.set()
    for c in workers:
        c.join()
    if progress_queue and copied is not None:
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Processed {len(copied)} unique items")
        )


def _clean_extensions(extensions: Optional[List[str]]) -> List[str]:
    clean: List[str] = []
    if extensions is not None:
        for ext in extensions:
            ext = ext.strip().lower()
            if ext.startswith(".") and len(ext) > 1:
                ext = ext[1:]
            clean.append(f"*.{ext}")
    return clean


def _build_path_rules(rule_pairs: List[str]) -> Callable[..., Tuple[str, str]]:
    """Create the rule applying function for path rule pairs"""
    rules: Dict[str, List[str]] = defaultdict(list)
    for rule in rule_pairs:
        extension, rule = rule.split(":")
        # Don't double-wildcard if extension already has wildcard
        if not extension.startswith("*"):
            extension = _clean_extensions([extension])[0]
        else:
            # Already wildcarded, just normalize
            extension = extension.strip().lower()
        if rule not in PATH_RULES:
            raise ValueError(f"Unexpected path rule: {rule}")
        rules[extension].append(rule)
    return functools.partial(_path_rules_parser, rules)


class Manifest:
    """Storage of manifest data. Presents the hash dict but tracks the
    read files in a separate structure"""

    cache_size = 10000

    def __init__(
        self,
        manifest_paths: Optional[Union[str, List[str]]],
        save_path: Optional[str] = None,
        temp_directory: Optional[str] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        self.temp_directory = temp_directory
        if save_path:
            self.path = save_path
        else:
            assert (
                temp_directory is not None
            ), "temp_directory must be provided if save_path is not"
            self.path = os.path.join(
                temp_directory, f"temporary_{random.getrandbits(16)}.dict"
            )
        self.md5_data: Any = {}
        self.read_sources: Any = {}
        self.save_event = save_event
        if manifest_paths:
            if not isinstance(manifest_paths, list):
                self.load(manifest_paths)
            else:
                self._load_manifest_list(manifest_paths)
        else:
            sources_path = f"{self.path}.read"
            # no data yet
            if os.path.exists(self.path):
                logger.info(f"Removing old manifest file at: {repr(self.path)}")
                os.unlink(self.path)
            if os.path.exists(sources_path):
                logger.info(
                    f"Removing old manifest sources file at: {repr(sources_path)}"
                )
                os.unlink(sources_path)
            logger.info(f"creating manifests {self.path} / {sources_path}")
            self.md5_data = DefaultCacheDict(
                list, db_file=self.path, max_size=self.cache_size
            )
            self.read_sources = CacheDict(
                db_file=sources_path, max_size=self.cache_size
            )

    def __contains__(self, key: str) -> bool:
        """Check if key exists in manifest."""
        return key in self.md5_data

    def __getitem__(self, key: str) -> Any:
        """Get value for key from manifest."""
        return self.md5_data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set value for key in manifest."""
        self.md5_data[key] = value

    def __delitem__(self, key: str) -> None:
        """Delete key from manifest."""
        del self.md5_data[key]

    def __len__(self) -> int:
        """Return number of hash keys in manifest."""
        return len(self.md5_data)

    def save(
        self, path: Optional[str] = None, keys: Optional[List[str]] = None
    ) -> None:
        """Save manifest to disk at specified path."""
        if self.save_event:
            self.save_event.set()
        path = path or self.path
        try:
            self._write_manifest(path=path, keys=keys)
            self.md5_data.save(db_file=path)
            self.read_sources.save(db_file=f"{path}.read")
        finally:
            if self.save_event:
                self.save_event.clear()

    def load(self, path: Optional[str] = None) -> None:
        """Load manifest from disk at specified path."""
        path = path or self.path
        self.md5_data, self.read_sources = self._load_manifest(path=path)

    def items(self) -> Any:
        """Return items view of manifest data."""
        return self.md5_data.items()

    def iteritems(self) -> Any:
        """Deprecated: Use items() instead"""
        return self.md5_data.items()

    def _write_manifest(
        self, path: Optional[str] = None, keys: Optional[List[str]] = None
    ) -> None:
        path = path or self.path
        logger.info(f"Writing manifest of {len(self.md5_data)} hashes to {path}")
        logger.info(f"Writing sources of {len(self.read_sources)} files to {path}.read")
        if not keys:
            dict_iter = self.md5_data.items()
        else:
            dict_iter = ((k, self.md5_data[k]) for k in keys)
        for _, info in dict_iter:
            for file_data in info:
                src = file_data[0]
                if src not in self.read_sources:
                    self.read_sources[src] = None

    def hash_set(self) -> Set[str]:
        """Return set of all hash keys in manifest."""
        return set(self.md5_data.keys())

    def _load_manifest(self, path: Optional[str] = None) -> Tuple[Any, Any]:
        path = path or self.path
        logger.info(f"Reading manifest from {repr(path)}...")
        # Would be nice to just get the fd, but backends require a path
        md5_data = DefaultCacheDict(list, db_file=path, max_size=self.cache_size)
        logger.info(f"... read {len(md5_data)} hashes")
        read_sources = CacheDict(db_file=f"{path}.read", max_size=self.cache_size)
        logger.info(f"... in {len(read_sources)} files")
        return md5_data, read_sources

    @staticmethod
    def _combine_manifests(manifests: List[Tuple[Any, Any]]) -> Tuple[Any, Any]:
        """Combine multiple manifest data structures into one.

        Args:
            manifests: List of (md5_data, read_sources) tuples to combine

        Returns:
            Tuple of (combined_md5_data, combined_read_sources)
        """
        base, read = manifests[0]
        for m, r in manifests[1:]:
            for key, files in m.items():
                for info in files:
                    if info not in base[key]:
                        base[key].append(info)
            for key in r:
                read[key] = None
        return base, read

    def _load_manifest_list(self, manifests: List[str]) -> None:
        if not isinstance(manifests, list):
            raise TypeError("manifests must be a list")
        read_manifest_data, read_sources = self._load_manifest(manifests[0])
        loaded = [(read_manifest_data, read_sources)]
        for src in manifests[1:]:
            loaded.append(self._load_manifest(src))
        self.md5_data, self.read_sources = Manifest._combine_manifests(loaded)

    def convert_manifest_paths(
        self, paths_from: str, paths_to: str, temp_directory: Optional[str] = None
    ) -> None:
        """Replaces all prefixes for all paths in the manifest with a new prefix"""
        temp_directory = temp_directory or self.temp_directory
        for key, val in self.md5_data.items():
            new_values = []
            for file_data in val:
                new_values.append(
                    [file_data[0].replace(paths_from, paths_to, 1)] + file_data[1:]
                )
            self.md5_data[key] = new_values
        # build a new set of values and move into place
        # Note: Using CacheDict for read_sources (could optimize with a persistent set implementation)
        db_file = self.read_sources.db_file_path()
        assert temp_directory is not None, "temp_directory must be provided"
        new_sources = CacheDict(
            db_file=os.path.join(temp_directory, "temp_convert.dict"),
            max_size=self.cache_size,
        )
        for key in self.read_sources:
            new_sources[key.replace(paths_from, paths_to, 1)] = None
        del self.read_sources
        new_sources.save(db_file=db_file)
        self.read_sources = new_sources
        self.md5_data.save()
        self.read_sources.save()


def run_dupe_copy(  # pylint: disable=too-complex,too-many-branches,too-many-statements
    read_from_path: Optional[Union[str, List[str]]] = None,
    extensions: Optional[List[str]] = None,
    manifests_in_paths: Optional[Union[str, List[str]]] = None,
    manifest_out_path: Optional[str] = None,
    *,
    path_rules: Optional[List[str]] = None,
    copy_to_path: Optional[str] = None,
    ignore_old_collisions: bool = False,
    ignored_patterns: Optional[List[str]] = None,
    csv_report_path: Optional[str] = None,
    walk_threads: int = 4,
    read_threads: int = 8,
    copy_threads: int = 8,
    convert_manifest_paths_to: str = "",
    convert_manifest_paths_from: str = "",
    no_walk: bool = False,
    no_copy: Optional[List[str]] = None,
    keep_empty: bool = False,
    compare_manifests: Optional[Union[str, List[str]]] = None,
    preserve_stat: bool = False,
) -> None:
    """For external callers this is the entry point for dedupe + copy"""
    # Ensure logging is configured for programmatic calls
    _ensure_logging_configured()

    # Display pre-flight summary
    logger.info("=" * 70)
    logger.info("DEDUPE COPY - Operation Summary")
    logger.info("=" * 70)
    if read_from_path:
        paths = read_from_path if isinstance(read_from_path, list) else [read_from_path]
        logger.info(f"Source path(s): {len(paths)} path(s)")
        for p in paths:
            logger.info(f"  - {p}")
    if copy_to_path:
        logger.info(f"Destination: {copy_to_path}")
    if manifests_in_paths:
        manifests = (
            manifests_in_paths
            if isinstance(manifests_in_paths, list)
            else [manifests_in_paths]
        )
        logger.info(f"Input manifest(s): {len(manifests)} manifest(s)")
    if manifest_out_path:
        logger.info(f"Output manifest: {manifest_out_path}")
    if extensions:
        logger.info(f"Extension filter: {', '.join(extensions)}")
    if ignored_patterns:
        logger.info(f"Ignored patterns: {', '.join(ignored_patterns)}")
    if path_rules:
        logger.info(f"Path rules: {', '.join(path_rules)}")
    logger.info(
        f"Threads: walk={walk_threads}, read={read_threads}, copy={copy_threads}"
    )
    logger.info(
        f"Options: keep_empty={keep_empty}, preserve_stat={preserve_stat}, no_walk={no_walk}"
    )
    if compare_manifests:
        comp_list = (
            compare_manifests
            if isinstance(compare_manifests, list)
            else [compare_manifests]
        )
        logger.info(f"Compare manifests: {len(comp_list)} manifest(s)")
    logger.info("=" * 70)
    logger.info("")

    temp_directory = tempfile.mkdtemp(suffix="dedupe_copy")

    save_event = threading.Event()
    manifest = Manifest(
        manifests_in_paths,
        save_path=manifest_out_path,
        temp_directory=temp_directory,
        save_event=save_event,
    )
    compare = Manifest(compare_manifests, save_path=None, temp_directory=temp_directory)

    if no_copy:
        for item in no_copy:
            compare[item] = None

    if no_walk and not manifest:
        raise ValueError("If --no-walk is specified, a manifest must be " "supplied.")
    if read_from_path and not isinstance(read_from_path, list):
        read_from_path = [read_from_path]
    path_rules_func: Optional[Callable[..., Tuple[str, str]]] = None
    if path_rules:
        path_rules_func = _build_path_rules(path_rules)
    all_stop = threading.Event()
    work_queue: "queue.Queue[str]" = queue.Queue()
    result_queue: "queue.Queue[Tuple[str, int, float, str]]" = queue.Queue()
    progress_queue: "queue.PriorityQueue[Any]" = queue.PriorityQueue()
    walk_queue: "queue.Queue[str]" = queue.Queue()
    progress_thread = ProgressThread(
        work_queue,
        result_queue,
        progress_queue,
        walk_queue=walk_queue,
        stop_event=all_stop,
        save_event=save_event,
    )
    progress_thread.start()
    collisions = None
    if manifest and (convert_manifest_paths_to or convert_manifest_paths_from):
        manifest.convert_manifest_paths(
            convert_manifest_paths_from, convert_manifest_paths_to
        )

    # storage for hash collisions
    collisions_file = os.path.join(temp_directory, "collisions.db")
    collisions = DefaultCacheDict(list, db_file=collisions_file, max_size=10000)
    if manifest and not ignore_old_collisions:
        # rebuild collision list
        for md5, info in manifest.iteritems():
            if len(info) > 1:
                collisions[md5] = info
    if no_walk:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Not walking file system. Using stored manifests",
            )
        )
        all_data = manifest
        dupes = collisions
    else:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Running the duplicate search, generating reports",
            )
        )
        dupes, all_data = find_duplicates(
            read_from_path or [],
            work_queue,
            result_queue,
            manifest,
            collisions,
            result_src=csv_report_path,
            extensions=extensions,
            ignore=ignored_patterns,
            progress_queue=progress_queue,
            walk_threads=walk_threads,
            read_threads=read_threads,
            keep_empty=keep_empty,
            save_event=save_event,
            walk_queue=walk_queue,
        )
    total_size = _extension_report(all_data)
    logger.info(f"Total Size of accepted: {total_size} bytes")
    if manifest_out_path:
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Saving complete manifest from search")
        )
        all_data.save(path=manifest_out_path)
    if copy_to_path is not None:
        # Warning: strip dupes out of all data, this assumes dupes correctly
        # follows handling of keep_empty (not a dupe even if md5 is same for
        # zero byte files)
        for md5 in dupes:
            if md5 in all_data:
                del all_data[md5]
        # copy the duplicate files first and then ignore them for the full pass
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Running copy to {repr(copy_to_path)}")
        )
        copy_data(
            dupes,
            all_data,
            copy_to_path,
            read_from_path or [],
            copy_threads=copy_threads,
            extensions=extensions,
            path_rules=path_rules_func,
            ignore=ignored_patterns,
            progress_queue=progress_queue,
            no_copy=compare,
            ignore_empty_files=keep_empty,
            preserve_stat=preserve_stat,
        )
    all_stop.set()
    while progress_thread.is_alive():
        progress_thread.join(5)
    del collisions
    try:
        time.sleep(1)
        shutil.rmtree(temp_directory)
    except OSError as err:
        logger.warning(
            f"Failed to cleanup the collisions file: {collisions_file} with err: {err}"
        )
