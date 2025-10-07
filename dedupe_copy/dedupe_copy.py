"""Find duplicate files on a file system


work_queue -> files put on here by WalkThread(s), read by ReadThread(s)
result_queue -> Put to by ReadThreads(s) read by ResultProcessor
progress_queue -> read by ProgressThread and put to by all others
walk_queue -> holds directories discovered by WalkThread(s)
copy_queue -> read by CopyThread(s) put to by queue_copy_work
"""

# pylint: disable=unused-import
from .core import run_dupe_copy
from .utils import (
    ensure_logging_configured,
    format_error_message,
    lower_extension,
    read_file,
)