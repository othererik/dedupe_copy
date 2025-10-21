"""DedupeCopy - Find duplicates and copy/restructure file layouts."""

__version__ = "1.1.7"
__author__ = "Erik Schweller"
__email__ = "othererik@gmail.com"

from .core import run_dupe_copy
from .utils import clean_extensions
from .path_rules import PATH_RULES
from . import disk_cache_dict

__all__ = ["run_dupe_copy", "clean_extensions", "PATH_RULES", "disk_cache_dict"]
