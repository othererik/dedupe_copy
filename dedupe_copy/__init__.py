"""DedupeCopy - Find duplicates and copy/restructure file layouts."""

__version__ = "1.0.0"
__author__ = "Erik Schweller"
__email__ = "othererik@gmail.com"

from .dedupe_copy import run_dupe_copy, _clean_extensions, PATH_RULES
from . import disk_cache_dict

__all__ = ["run_dupe_copy", "_clean_extensions", "PATH_RULES", "disk_cache_dict"]
