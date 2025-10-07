"""Configuration classes for dedupe_copy
"""

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple


@dataclass
class WalkConfig:
    """Configuration for the walking phase."""

    extensions: Optional[List[str]] = None
    ignore: Optional[List[str]] = None
    hash_algo: str = "md5"


@dataclass
class CopyConfig:
    """Configuration for the copy phase."""

    target_path: str
    read_paths: List[str]
    extensions: Optional[List[str]] = None
    path_rules: Optional[Callable[..., Tuple[str, str]]] = None
    preserve_stat: bool = False


@dataclass
class CopyJob:
    """Configuration for a copy job."""

    copy_config: CopyConfig
    ignore: Optional[List[str]] = None
    no_copy: Optional[Any] = None
    ignore_empty_files: bool = False
    copy_threads: int = 8


@dataclass
class DeleteJob:
    """Configuration for a delete job."""

    delete_threads: int = 8
    dry_run: bool = False
    min_delete_size_bytes: int = 0
