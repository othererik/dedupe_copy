"""Utility functions for the dedupe_copy application."""

import datetime
import fnmatch
import functools
import hashlib
import logging
import os
import queue
import time
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
    Tuple,
    Any,
    Callable,
    Counter,
    Union,
)

if TYPE_CHECKING:
    from .dedupe_copy import DedupeCopyConfig

logger = logging.getLogger(__name__)

READ_CHUNK = 1048576  # 1 MB
MAX_TARGET_QUEUE_SIZE = 50000
PATH_RULES = {
    "mtime": "Put each file into a directory of the form YYYY_MM",
    "no_change": 'Preserve directory structure from "read_path" up',
    "extension": "Put all items into directories of their extension",
}

# For message output
HIGH_PRIORITY = 1
MEDIUM_PRIORITY = 5
LOW_PRIORITY = 10


def format_error_message(path: str, error: Union[str, Exception]) -> str:
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

    msg = f"Error processing {path!r}: [{error_type}] {error_str}"
    if suggestions:
        msg += f"\n  Suggestions: {'; '.join(suggestions)}"
    return msg


def path_rules_parser(
    rules: Dict[str, List[str]], file_context: Dict[str, Any]
) -> Tuple[str, str]:
    """Builds a path based on the path rules for the given extension pattern"""
    dest_dir = file_context["copy_to_path"]
    extension = file_context["extension"]
    mtime_str = file_context["mtime_str"]
    source_dirs = file_context["source_dirs"]
    src = file_context["src"]
    read_paths = file_context["read_paths"]

    ext_key = extension if extension else ""
    best_match_key = best_match(rules.keys(), ext_key)
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


def best_match(extensions: Any, ext: str) -> Optional[str]:
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


def match_extension(extensions: Optional[List[str]], fn: str) -> bool:
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


def throttle_puts(current_size: int) -> None:
    """Delay for some factor to avoid overloading queues"""
    time.sleep(min((current_size * 2) / float(MAX_TARGET_QUEUE_SIZE), 60))


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


def clean_extensions(extensions: Optional[List[str]]) -> List[str]:
    clean: List[str] = []
    if extensions is not None:
        for ext in extensions:
            ext = ext.strip().lower()
            if ext.startswith(".") and len(ext) > 1:
                ext = ext[1:]
            clean.append(f"*.{ext}")
    return clean


def build_path_rules(rule_pairs: List[str]) -> Callable[..., Tuple[str, str]]:
    """Create the rule applying function for path rule pairs"""
    rules: Dict[str, List[str]] = {}
    for rule in rule_pairs:
        extension, rule_name = rule.split(":")
        # Don't double-wildcard if extension already has wildcard
        if not extension.startswith("*"):
            extension = clean_extensions([extension])[0]
        else:
            # Already wildcarded, just normalize
            extension = extension.strip().lower()
        if rule_name not in PATH_RULES:
            raise ValueError(f"Unexpected path rule: {rule_name}")
        rules.setdefault(extension, []).append(rule_name)
    return functools.partial(path_rules_parser, rules)


def distribute_work(
    src: str,
    already_processed: Any,
    config: "DedupeCopyConfig",
    progress_queue: "queue.PriorityQueue[Any]",
    work_queue: "queue.Queue[str]",
    walk_queue: "queue.Queue[str]",
) -> None:
    """Scan a directory and distribute files and subdirectories."""
    if progress_queue:
        progress_queue.put((HIGH_PRIORITY, "dir", src))
    for item in os.scandir(src):
        if item.is_symlink():
            if progress_queue:
                progress_queue.put(
                    (LOW_PRIORITY, "ignored", item.path, "symlink")
                )
            continue
        if item.is_dir():
            if progress_queue:
                progress_queue.put((LOW_PRIORITY, "dir", item.path))
            throttle_puts(walk_queue.qsize())
            walk_queue.put(item.path)
            continue
        if item.path in already_processed:
            if progress_queue:
                progress_queue.put(
                    (LOW_PRIORITY, "ignored", item.path, "already processed")
                )
            continue
        if config.ignored_patterns and any(
            fnmatch.fnmatch(item.name, pattern) for pattern in config.ignored_patterns
        ):
            if progress_queue:
                progress_queue.put(
                    (LOW_PRIORITY, "ignored", item.path, "ignored pattern")
                )
            continue
        if config.extensions and not match_extension(
            config.extensions, item.name
        ):
            if progress_queue:
                progress_queue.put(
                    (LOW_PRIORITY, "ignored", item.path, "extension mismatch")
                )
            continue
        if progress_queue:
            progress_queue.put((LOW_PRIORITY, "accepted", item.path))
        throttle_puts(work_queue.qsize())
        work_queue.put(item.path)


def extension_report(md5_data: Any, show_count: int = 10) -> int:
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
    logger.info("Top %s extensions by size:", show_count)
    for key, _ in zip(
        sorted(sizes, key=lambda x: sizes.get(x, 0), reverse=True), range(show_count)
    ):
        logger.info("  %s: %s bytes", key, sizes[key])
    logger.info("Top %s extensions by count:", show_count)
    for key, _ in zip(
        sorted(
            extension_counts, key=lambda x: extension_counts.get(x, 0), reverse=True
        ),
        range(show_count),
    ):
        logger.info("  %s: %s", key, extension_counts[key])
    return sum(sizes.values())