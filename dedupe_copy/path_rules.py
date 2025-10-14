"""This module handles the path rule parsing logic for dedupe_copy."""

import fnmatch
import functools
import os
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

from .utils import clean_extensions

PATH_RULES = {
    "mtime": "Put each file into a directory of the form YYYY_MM",
    "no_change": 'Preserve directory structure from "read_path" up',
    "extension": "Put all items into directories of their extension",
}


def path_rules_parser(
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
    """Constructs a destination path for a file based on a set of rules.

    This function applies a series of rules to determine the final directory
    and file path for a copied file. The rules are matched based on the
    file's extension.

    Args:
        rules: A dictionary mapping extension patterns to a list of path rules.
        dest_dir: The base destination directory.
        extension: The file extension of the source file.
        mtime_str: A string representation of the file's modification time.
        _size: The size of the file (currently unused).
        source_dirs: The source directory of the file.
        src: The source filename.
        read_paths: A list of the root paths for the copy operation.

    Returns:
        A tuple containing the full destination path and the destination
        directory.
    """
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


def build_path_rules(rule_pairs: List[str]) -> Callable[..., Tuple[str, str]]:
    """Parses a list of rule strings and returns a configured path rule function.

    This function takes a list of strings, each defining a path rule for a
    specific file extension pattern, and returns a partially configured
    `path_rules_parser` function that is ready to be used in the copy process.

    Args:
        rule_pairs: A list of strings, where each string is in the format
                    "extension:rule".

    Returns:
        A callable that takes file information and returns a destination path.

    Raises:
        ValueError: If an unknown path rule is encountered.
    """
    rules: Dict[str, List[str]] = defaultdict(list)
    for rule in rule_pairs:
        extension, rule = rule.split(":")
        # Don't double-wildcard if extension already has wildcard
        if not extension.startswith("*"):
            extension = clean_extensions([extension])[0]
        else:
            # Already wildcarded, just normalize
            extension = extension.strip().lower()
        if rule not in PATH_RULES:
            raise ValueError(f"Unexpected path rule: {rule}")
        rules[extension].append(rule)
    return functools.partial(path_rules_parser, rules)
