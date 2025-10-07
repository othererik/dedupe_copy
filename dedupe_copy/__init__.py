"""DedupeCopy - Find duplicates and copy/restructure file layouts."""

__version__ = "1.0.0"
__author__ = "Erik Schweller"
__email__ = "othererik@gmail.com"

from .dedupe_copy import DedupeCopyConfig, run_dupe_copy
from . import disk_cache_dict
from .utils_dedupe import PATH_RULES, clean_extensions

__all__ = [
    "run_dupe_copy",
    "clean_extensions",
    "PATH_RULES",
    "disk_cache_dict",
    "DedupeCopyConfig",
]
