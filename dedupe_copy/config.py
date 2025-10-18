"""Configuration classes for dedupe_copy"""

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple


@dataclass
class WalkConfig:
    """Defines the configuration for the filesystem walking process.

    Attributes:
        extensions: An optional list of file extensions to include in the walk.
                    If None, all extensions are included.
        ignore: An optional list of glob patterns to ignore during the walk.
        hash_algo: The hashing algorithm to use for file content.
        dedupe_empty: If True, empty files are included in the walk.
    """

    extensions: Optional[List[str]] = None
    ignore: Optional[List[str]] = None
    hash_algo: str = "md5"
    dedupe_empty: bool = False


@dataclass
class CopyConfig:
    """Specifies the parameters for a file copy operation.

    Attributes:
        target_path: The destination directory for the copy operation.
        read_paths: A list of source directories to read files from.
        extensions: An optional list of file extensions to include.
        path_rules: Optional rules for transforming file paths during the copy.
        preserve_stat: If True, file metadata (like timestamps) is preserved.
        delete_on_copy: If True, delete source files after a successful copy.
        dry_run: If True, simulate operations without making changes.
    """

    target_path: str
    read_paths: List[str]
    extensions: Optional[List[str]] = None
    path_rules: Optional[Callable[..., Tuple[str, str]]] = None
    preserve_stat: bool = False
    delete_on_copy: bool = False
    dry_run: bool = False


@dataclass
class CopyJob:
    """Defines a complete copy job, combining configuration and operational settings.

    Attributes:
        copy_config: The core configuration for the copy operation.
        ignore: A list of glob patterns to exclude from the copy.
        no_copy: A set-like object of hashes to prevent from being copied.
        dedupe_empty: If True, files with zero size are not copied.
        copy_threads: The number of concurrent threads to use for copying.
        delete_on_copy: If True, delete source files after a successful copy.
        dry_run: If True, simulate operations without making changes.
    """

    copy_config: CopyConfig
    ignore: Optional[List[str]] = None
    no_copy: Optional[Any] = None
    dedupe_empty: bool = False
    copy_threads: int = 8
    delete_on_copy: bool = False
    dry_run: bool = False


@dataclass
class DeleteJob:
    """Configures the behavior of a duplicate file deletion job.

    Attributes:
        delete_threads: The number of concurrent threads for deletion.
        dry_run: If True, simulates deletion without removing files.
        min_delete_size_bytes: The minimum size for a file to be deleted.
        dedupe_empty: If True, empty files are considered for deletion.
    """

    delete_threads: int = 8
    dry_run: bool = False
    min_delete_size_bytes: int = 0
    dedupe_empty: bool = False
