#!/usr/bin/env python3
"""Command-line interface for the dedupe_copy tool."""
import argparse
import logging
import sys

from dedupe_copy.utils import clean_extensions
from dedupe_copy.core import run_dupe_copy
from dedupe_copy.path_rules import PATH_RULES
from dedupe_copy.logging_config import setup_logging


DESCRIPTION = "Find duplicates / copy and restructure file layout tool"
EPILOGUE = r"""
Examples:

  Generate a duplicate file report for a path:
      dedupecopy -p /Users/ -r dupes.csv -m manifest

  Copy all *.jpg files from multiple paths to a /YYYY_MM/*.jpg structure
      dedupecopy -p C:\pics -p D:\pics -e jpg -R jpg:mtime -c X:\pics

  Copy all files from two drives to a single target (preserving structure):
      dedupecopy -p C:\ -p D:\ -c X:\ -m X:\manifest
      
      Note: Directory structure is preserved by default. Use -R for custom organization.

  Resume an interrupted run (assuming "-m manifest" used in prior run):
    dedupecopy -p /Users/ -r dupes_2.csv -i manifest -m manifest

  Verify that files in a manifest exist and sizes match:
    dedupecopy --no-walk --verify --manifest-read-path my_manifest

  Sequentially copy different sources into the same target, not copying
  duplicate files (2 sources and 1 target):
    1.) First record manifests for all devices
        dedupecopy -p \\target\share -m target_manifest
        dedupecopy -p \\source1\share -m source1_manifest
        dedupecopy -p \\source2\share -m source2_manifest

    2.) Copy each source to the target (specifying --compare so manifests from
        other sources are loaded but not used as part of the set to copy and
        --no-walk to skip re-scan of the source):
        dedupecopy -p \\source1\share -c \\target\share -i source1_manifest \
            --compare source2_manifest --compare target_manifest  --no-walk
        dedupecopy -p \\source2\share -c \\target\share -i source2_manifest \
            --compare source1_manifest --compare target_manifest --no-walk

  Delete duplicates from a manifest, skipping files smaller than 1MB:
    dedupecopy --no-walk --delete --manifest-read-path my_manifest.db --min-delete-size 1048576
"""


def _create_parser():
    """Creates and returns the argparse parser."""
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        epilog=EPILOGUE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    walk_group = parser.add_mutually_exclusive_group(required=True)
    walk_group.add_argument(
        "-p",
        "--read-path",
        help="Path (s) to start walk for dupes",
        required=False,
        action="append",
    )
    walk_group.add_argument(
        "--no-walk",
        help="Use paths from a loaded manifest, " "do not re-scan the file system",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--min-delete-size",
        help="Minimum size of a file to be considered for deletion (bytes).",
        required=False,
        default=0,
        type=int,
        dest="min_delete_size",
    )

    parser.add_argument(
        "-r",
        "--result-path",
        help="Path for result output",
        required=False,
        default=None,
    )

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "-c", "--copy-path", help="Path to copy to", required=False, default=None
    )
    action_group.add_argument(
        "--delete",
        help="Delete duplicate files",
        required=False,
        default=False,
        action="store_true",
    )
    action_group.add_argument(
        "--verify",
        help="Verify manifest files exist and sizes match",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--delete-on-copy",
        help="Delete source files after successful copy. If --compare is also used, "
        "source files that are duplicates of those in the compare manifest will "
        "also be deleted. Requires --copy-path.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--copy-metadata",
        help="Copy file stat data on copy as well",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--compare",
        help="Path to existing manifest, used to prevent "
        "duplicates from being copied to a target, but these "
        "items are not themselves copied",
        required=False,
        default=None,
        dest="compare",
        action="append",
    )
    parser.add_argument(
        "-m",
        "--manifest-dump-path",
        help="Where to write the manifest dump",
        required=False,
        default=None,
        dest="manifest_out",
    )
    parser.add_argument(
        "-i",
        "--manifest-read-path",
        help="Where to read an existing the manifest dump",
        required=False,
        default=None,
        dest="manifest_in",
        action="append",
    )
    parser.add_argument(
        "-e",
        "--extensions",
        help="extension (s) to record/copy (may include ?/*)",
        required=False,
        default=None,
        action="append",
    )
    parser.add_argument(
        "--ignore",
        help="file patterns (s) to ignore during record/copy "
        "(may include ?/*). For example: using fred*.jpg "
        "excludes fred_1.jpg from from being copied and/or "
        "reported as a dupe",
        required=False,
        default=None,
        action="append",
    )
    parser.add_argument(
        "-R",
        "--path-rules",
        help=f"extension:rule_name pair(s) for organizing files. "
        f"Default: preserves original directory structure (no_change). "
        f"Example: -R png:mtime organizes PNG files by date. "
        f"Rules are cumulative: -R png:extension -R png:mtime creates "
        f"/copy_path/png/2012_08/file.png. Available rules: {PATH_RULES}",
        required=False,
        default=None,
        action="append",
    )
    parser.add_argument(
        "--ignore-old-collisions",
        help="Only find collisions with un-scanned files",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--dedupe-empty",
        help="Count empty files as duplicates",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--dry-run",
        help="Simulate operations without making changes",
        required=False,
        default=False,
        action="store_true",
    )

    performance = parser.add_argument_group("Performance Related")
    performance.add_argument(
        "--walk-threads",
        help="Number of threads to use in the file " "system walk",
        required=False,
        default=4,
        type=int,
    )
    performance.add_argument(
        "--read-threads",
        help="Number of threads to read with",
        required=False,
        default=8,
        type=int,
    )
    performance.add_argument(
        "--copy-threads",
        help="Number of threads to use for copying files",
        required=False,
        default=8,
        type=int,
    )
    performance.add_argument(
        "--hash-algo",
        help="Hashing algorithm to use ('md5' or 'xxh64'). "
        "'xxh64' requires the 'fast_hash' extra "
        "('pip install dedupe_copy[fast_hash]')",
        required=False,
        default="md5",
        choices=["md5", "xxh64"],
        type=str,
    )

    output_group = parser.add_argument_group("Output Control")
    verbosity_group = output_group.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "--quiet",
        "-q",
        help="Show only warnings and errors",
        action="store_true",
        required=False,
        default=False,
    )
    verbosity_group.add_argument(
        "--verbose",
        "-v",
        help="Show more detailed progress information, including processing rates.",
        action="store_true",
        required=False,
        default=False,
    )
    verbosity_group.add_argument(
        "--debug",
        help="Show debug information including queue states",
        action="store_true",
        required=False,
        default=False,
    )
    output_group.add_argument(
        "--no-color",
        help="Disable colored output",
        action="store_true",
        required=False,
        default=False,
    )

    group = parser.add_argument_group("Path conversion")
    group.add_argument(
        "--convert-manifest-paths-from",
        help="Prefix of paths stored in the input manifest to " "replace",
        required=False,
        default="",
    )
    group.add_argument(
        "--convert-manifest-paths-to",
        help="Replacement prefix (replaces the prefix found in "
        "--convert-manifest-paths-from)",
        required=False,
        default="",
    )
    return parser


def _handle_arguments(args):
    """Take the cli args and process them in prep for calling run_dedupe_copy"""
    logger = logging.getLogger(__name__)

    if args.read_path:
        logger.info("Reading from %s", args.read_path)
    # strip, lower, remove leading dot from extensions for both path rules and
    # specific extension includes
    extensions = clean_extensions(args.extensions)
    read_paths = None
    if args.read_path:
        read_paths = args.read_path
    if args.copy_path:
        copy_path = args.copy_path
    else:
        copy_path = None
    return {
        "read_from_path": read_paths,
        "extensions": extensions,
        "manifests_in_paths": args.manifest_in,
        "manifest_out_path": args.manifest_out,
        "path_rules": args.path_rules,
        "copy_to_path": copy_path,
        "ignore_old_collisions": args.ignore_old_collisions,
        "ignored_patterns": args.ignore,
        "csv_report_path": args.result_path,
        "walk_threads": args.walk_threads,
        "read_threads": args.read_threads,
        "copy_threads": args.copy_threads,
        "hash_algo": args.hash_algo,
        "convert_manifest_paths_to": args.convert_manifest_paths_to,
        "convert_manifest_paths_from": args.convert_manifest_paths_from,
        "no_walk": args.no_walk,
        "no_copy": None,
        "dedupe_empty": args.dedupe_empty,
        "compare_manifests": args.compare,
        "preserve_stat": args.copy_metadata,
        "delete_duplicates": args.delete,
        "delete_on_copy": args.delete_on_copy,
        "dry_run": args.dry_run,
        "min_delete_size": args.min_delete_size,
        "verify_manifest": args.verify,
    }


def run_cli():
    """Main entry point for the command-line interface."""
    parser = _create_parser()
    args = parser.parse_args()

    # Argument validation
    if args.delete_on_copy and not args.copy_path:
        parser.error("--delete-on-copy requires --copy-path.")

    if (args.delete or args.delete_on_copy) and not args.manifest_out:
        parser.error(
            "Operations that modify the manifest (--delete, --delete-on-copy) "
            "require -m/--manifest-dump-path."
        )

    # Setup logging based on verbosity flags
    verbosity = "normal"
    if args.quiet:
        verbosity = "quiet"
    elif args.debug:
        verbosity = "debug"
    elif args.verbose:
        verbosity = "verbose"
    setup_logging(verbosity=verbosity, use_colors=not args.no_color)

    logger = logging.getLogger(__name__)
    logger.debug("Running with arguments: %s", args)

    processed_args = _handle_arguments(args)
    try:
        return run_dupe_copy(**processed_args)
    except ValueError as e:
        parser.error(str(e))
        sys.exit(1)


if __name__ == "__main__":
    run_cli()
