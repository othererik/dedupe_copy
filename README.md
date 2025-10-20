# DedupeCopy

A multi-threaded command-line tool for finding duplicate files and copying/restructuring file layouts while eliminating duplicates.

[![License](https://img.shields.io/badge/license-BSD-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Key Concepts](#key-concepts)
- [Usage Examples](#usage-examples)
- [Command-Line Options](#command-line-options)
- [Path Rules](#path-rules)
- [Advanced Workflows](#advanced-workflows)
- [Performance Tips](#performance-tips)
- [Troubleshooting](#troubleshooting)
- [Safety and Best Practices](#safety-and-best-practices)

## Overview

DedupeCopy is designed for consolidating and restructuring sprawling file systems, particularly useful for:

- **Backup consolidation**: Merge multiple backup sources while eliminating duplicates
- **Photo/media library organization**: Consolidate photos from various devices and organize by date
- **File system cleanup**: Identify and remove duplicate files
- **Server migration**: Copy files to new storage while preserving structure
- **Duplicate detection**: Generate reports of duplicate files without copying
- **Deleting duplicates**: Reclaim disk space by removing redundant files.

**The good bits:**
- Uses MD5 checksums for accurate duplicate detection
- Multi-threaded for fast processing
- Manifest system for resuming interrupted operations
- Flexible path restructuring rules
- Can compare against multiple file systems without full re-scans
- Configurable logging with verbosity levels (quiet, normal, verbose, debug)
- Colored output for better readability (optional)
- Helpful error messages with actionable suggestions
- Real-time progress with processing rates

**Note:** This is *not* a replacement for rsync or Robocopy for incremental synchronization. Those are good tools that might work for you, so do try them.

## Architecture

DedupeCopy uses a multi-threaded pipeline architecture to maximize performance when processing large file systems. Understanding this architecture helps explain performance characteristics and tuning options.

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                         MAIN THREAD                             │
│  • Orchestrates the entire operation                            │
│  • Manages thread lifecycle and coordination                    │
│  • Handles manifest loading/saving                              │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     THREAD POOLS (Queues)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌────────────────┐    ┌────────────────┐    ┌───────────────┐  │
│  │  Walk Threads  │───▶│  Read Threads  │───▶│ Copy/Delete   │  │
│  │  (4 default)   │    │  (8 default)   │    │   Threads     │  │
│  │                │    │                │    │  (8 default)  │  │
│  └────────────────┘    └────────────────┘    └───────────────┘  │
│         │                      │                      │         │
│         ▼                      ▼                      ▼         │
│   Walk Queue            Work Queue            Copy/Delete       │
│   (directories)         (files to hash)         Queue           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      PROGRESS THREAD                            │
│  • Collects status updates from all worker threads              │
│  • Displays progress, rates, and statistics                     │
│  • Logs errors and warnings                                     │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RESULT PROCESSOR                             │
│  • Processes file hashes from read threads                      │
│  • Detects duplicate files                                      │
│  • Updates manifest and collision dictionaries                  │
│  • Performs incremental saves every 50,000 files                │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PERSISTENT STORAGE                           │
│  • Manifest: Maps hash → list of files with that hash           │
│  • Collision DB: Tracks duplicate files                         │
│  • SQLite-backed with disk caching (LRU-like eviction)          │
│  • Auto-saves every 50,000 files for crash recovery             │
└─────────────────────────────────────────────────────────────────┘
```

### Pipeline Stages

#### 1. **Walk Stage** (WalkThread)
- **Purpose**: Discover files and directories in the source paths
- **Thread Count**: 4 by default (configurable with `--walk-threads`)
- **Output**: Adds directories to `walk_queue` and files to `work_queue`
- **Filtering**: Applies extension filters and ignore patterns

#### 2. **Read Stage** (ReadThread)
- **Purpose**: Hash file contents to detect duplicates
- **Thread Count**: 8 by default (configurable with `--read-threads`)
- **Input**: Files from `work_queue`
- **Output**: Tuple of (hash, size, mtime, filepath) to `result_queue`
- **Algorithm**: `md5` or `xxh64` (configurable with `--hash-algo`)

#### 3. **Result Processing** (ResultProcessor)
- **Purpose**: Aggregate hashes and detect collisions
- **Thread Count**: 1 (single-threaded for data consistency)
- **Input**: Hash results from `result_queue`
- **Output**: Updates `manifest` and `collisions` dictionaries
- **Auto-save**: Incremental saves every 50,000 processed files

#### 4. **Copy/Delete Stage** (CopyThread/DeleteThread)
- **Purpose**: Perform file operations based on duplicate analysis
- **Thread Count**: 8 by default (configurable with `--copy-threads`)
- **Input**: Files from `copy_queue` or `delete_queue`
- **Operations**:
  - Copy unique files to destination (with optional path rules)
  - Delete duplicate files (keeping first occurrence)

#### 5. **Progress Monitoring** (ProgressThread)
- **Purpose**: Centralized logging and status updates
- **Thread Count**: 1 (collects events from all other threads)
- **Features**:
  - Real-time progress with processing rates
  - Queue size monitoring (to detect bottlenecks)
  - Error aggregation and reporting
  - Final summary statistics

### Data Structures

#### Manifest
- **Storage**: Disk-backed dictionary (SQLite + cache layer)
- **Format**: `hash → [(filepath, size, mtime), ...]`
- **Cache**: In-memory LRU cache (10,000 items default)
- **Persistence**: Auto-saved during operation and on completion

#### Disk Cache Dictionary
- **Purpose**: Handle datasets larger than available RAM
- **Backend**: SQLite with Write-Ahead Logging (WAL)
- **Optimization**: Batched commits for performance
- **Eviction**: LRU or random eviction when cache is full

### Performance Characteristics

**Queue-Based Throttling**: When queues exceed 100,000 items, the system introduces deliberate delays to prevent memory exhaustion.

**Bottleneck Detection**: Progress thread monitors queue sizes:
- **Walk queue growing**: Too many directories, consider reducing `--walk-threads`
- **Work queue growing**: Hashing is slower than discovery, increase `--read-threads`
- **Copy queue growing**: I/O is slower than hashing, increase `--copy-threads`

**I/O Patterns**:
- **Walk threads**: Mostly metadata operations (directory listings)
- **Read threads**: Sequential reads of entire files
- **Copy threads**: Large sequential writes

### Thread Safety

- **Queues**: Python's `queue.Queue` provides thread-safe operations
- **Manifest**: Uses database-level locking (SQLite RLock)
- **Save operations**: Coordinated via `save_event` to pause workers during saves

## Installation

### Via pip (recommended)

```bash
pip install DedupeCopy
```

### With color support (optional)

For colored console output (errors in red, warnings in yellow, etc.):

```bash
pip install DedupeCopy[color]
```

### With fast hashing (optional)

For faster file hashing using xxhash:
```bash
pip install DedupeCopy[fast_hash]
```

### From source

```bash
git clone https://github.com/othererik/dedupe_copy.git
cd dedupe_copy
pip install -e .
# Or with color support:
pip install -e .[color]
```

### Requirements

- Python 3.11 or later
- Sufficient disk space for manifest files (typically small, but can grow for very large file sets)
- Optional: colorama for colored console output (installed with `[color]` extra)

## Quick Start

### Find duplicates in a directory

```bash
dedupecopy -p /path/to/search -r duplicates.csv
```

This scans `/path/to/search` and creates a CSV report of all duplicate files.

### Copy files while removing duplicates

```bash
dedupecopy -p /source/path -c /destination/path
```

This copies all files from source to destination, skipping any duplicates.

### Copy with manifest (recommended for large operations)

```bash
dedupecopy -p /source/path -c /destination/path -m manifest.db
```

Creates a manifest file that allows you to resume if interrupted. For example, if the operation is stopped, you can resume it with:

```bash
dedupecopy -p /source/path -c /destination/path -i manifest.db -m manifest.db
```

### Delete Duplicates

```bash
dedupecopy -p /path/to/search --delete --dry-run
```

This will scan the specified path and show you which files would be deleted. Once you are sure, you can run the command again without `--dry-run` to perform the deletion.

## Key Concepts

### Manifests

Manifests are database files that store:
- MD5 checksums of processed files
- File metadata (size, modification time, path)
- Which files have been scanned

**Benefits:**
- **Resumability**: If an operation is interrupted, you can resume it by loading the manifest. The tool will skip any files that have already been processed.
- **Comparison**: Manifests can be used to compare file systems without re-scanning. For example, you can compare a source and a destination to see which files are missing from the destination.
- **Incremental Backups**: By loading a manifest from a previous backup, you can process only new or modified files.
- **Tracking**: Manifests keep a record of which files have been processed, which is useful for auditing and tracking.

**Manifest Options:**
- `-m manifest.db` - Save manifest after processing (output)
- `-i manifest.db` - Load existing manifest before processing (input)
- `--compare manifest.db` - Load manifest for duplicate checking only (does not copy those files)

**Important Safety Rule:** You **cannot** use the same file path for both `-i` (input) and `-m` (output). This prevents accidental manifest corruption during operations.

### Understanding `-i` vs `--compare`

These two options handle manifests differently and are suited for different use cases:

#### `-i` / `--manifest-read-path` (Input Manifest)
- **Purpose**: Resume or continue a previous operation
- **Behavior**: 
  - Files in this manifest are considered "already processed"
  - The scan will skip re-processing these files
  - These files ARE included in the output manifest
- **Use when**: Resuming an interrupted operation or continuing from a previous run
- **Example**: Copying files and resuming after interruption

```bash
# Initial run (interrupted)
dedupecopy -p /source -c /dest -m progress.db

# Resume (loads progress.db to skip already-copied files)
dedupecopy -p /source -c /dest -i progress.db -m progress_new.db
```

#### `--compare` (Comparison Manifest)
- **Purpose**: Skip files that exist elsewhere (for deduplication)
- **Behavior**:
  - Files in this manifest are considered "duplicates to skip"
  - These files will NOT be copied
  - These files are NOT included in the output manifest
- **Use when**: Consolidating multiple sources or performing incremental backups
- **Example**: Backing up new files without copying ones already in destination

```bash
# Incremental backup (skip files already backed up)
dedupecopy -p /source -c /backup --compare backup_v1.db -m backup_v2.db
```

#### Quick Comparison Table

| Feature | `-i` (Input) | `--compare` |
|---------|--------------|-------------|
| Files are copied? | No (already processed) | No (treated as duplicates) |
| Included in output manifest? | Yes | No |
| Use case | Resume operations | Deduplicate across sources |
| Can use with `-m` same path? | **No** (safety rule) | Yes |

**Note:** Manifest files are SQLite databases. The main manifest file has a `.db` extension, and there is a corresponding `.db.read` file that tracks which files have been read.

### Duplicate Detection

Files are considered duplicates when:
1. They have identical MD5 checksums
2. They have the same file size

**Special case:** Empty (zero-byte) files are treated as unique by default. Use `--dedupe-empty` to treat them as duplicates.

## Usage Examples

### Basic Operations

#### 1. Generate a duplicate file report

```bash
dedupecopy -p /Users/johndoe -r dupes.csv -m manifest.db
```

Creates a CSV report of all duplicates and saves a manifest for future use.

**With quiet output (minimal):**
```bash
dedupecopy -p /Users/johndoe -r dupes.csv -m manifest.db --quiet
```

**With verbose output (detailed progress):**
```bash
dedupecopy -p /Users/johndoe -r dupes.csv -m manifest.db --verbose
```

#### 2. Copy specific file types

```bash
dedupecopy -p /source -c /backup -e jpg -e png -e gif
```

Copy only image files (jpg, png, gif) to the backup directory.

### Photo Organization

#### Organize photos by date

```bash
dedupecopy -p C:\pics -p D:\pics -e jpg -R "jpg:mtime" -c X:\organized_photos
```

Copies all JPG files from C: and D: drives, organizing them into folders by year/month (e.g., `2024_03/`). See the "Path and Extension Rules" section for more on combining rules.

### Multi-Source Consolidation

#### Copy from multiple sources to single destination

```bash
dedupecopy -p /source1 -p /source2 -p /source3 -c /backup -m backup_manifest.db
```

Scans all three source paths and copies unique files to backup.

#### Resume an interrupted copy

```bash
dedupecopy -p /source -c /destination -i manifest.db -m manifest.db
```

Loads the previous manifest and resumes where it left off.

### Advanced Pattern Matching

#### Ignore specific patterns

```bash
dedupecopy -p /source -c /backup --ignore "*.tmp" --ignore "*.cache" --ignore "**/Thumbs.db"
```

Excludes temporary files and thumbnails from processing.

#### Extension-specific rules

```bash
dedupecopy -p /media -c /organized \
  -R "*.jpg:mtime" \
  -R "*.mp4:extension" \
  -R "*.doc*:no_change"
```

Different organization rules for different file types.

## Command-Line Options

### Required Options (one of)

| Option | Description |
|--------|-------------|
| `-p PATH`, `--read-path PATH` | Source path(s) to scan. Can be specified multiple times. |
| `--no-walk` | Skip file system walk; use paths from loaded manifest only. |

### Core Options

| Option | Description |
|--------|-------------|
| `-c PATH`, `--copy-path PATH` | Destination path for copying files. Cannot be used with `--delete`. |
| `--delete` | Deletes duplicate files, keeping the first-seen file. Cannot be used with `--copy-path`. |
| `-r PATH`, `--result-path PATH` | Output path for CSV duplicate report. |
| `-m PATH`, `--manifest-dump-path PATH` | Path to save manifest file. |
| `-i PATH`, `--manifest-read-path PATH` | Path to load existing manifest. Can be specified multiple times. |
| `-e EXT`, `--extensions EXT` | File extension(s) to include (e.g., `jpg`, `*.png`). Can be specified multiple times. |
| `--ignore PATTERN` | File pattern(s) to exclude (supports wildcards). Can be specified multiple times. |
| `-R RULE`, `--path-rules RULE` | Path restructuring rule(s) in format `extension:rule`. Can be specified multiple times. |

### Special Options

| Option | Description |
|--------|-------------|
| `--compare PATH` | Load manifest but don't copy its files (for comparison only). This is useful for excluding files that are already present in a destination or another source. Can be specified multiple times. |
| `--copy-metadata` | Preserve file timestamps and permissions (uses `shutil.copy2` instead of `copyfile`). |
| `--dedupe-empty` | Treat empty (0-byte) files as duplicates rather than unique. |
| `--ignore-old-collisions` | Only detect new duplicates (ignore duplicates already in loaded manifest). |
| `--dry-run` | Simulate operations without making any changes to the filesystem. |
| `--min-delete-size BYTES` | Minimum size of a file to be considered for deletion (e.g., `1048576` for 1MB). Default: `0`. |
| `--delete-on-copy` | Deletes source files after a successful copy. Requires `--copy-path` and `-m`.  WARNING: this will consider duplicated objects as copied and remove them.  |

### Output Control Options

| Option | Description |
|--------|-------------|
| `-q`, `--quiet` | Show only warnings and errors (minimal output). |
| `-v`, `--verbose` | Show detailed progress information (same as normal, kept for compatibility). |
| `--debug` | Show debug information including queue states and internal diagnostics. |
| `--no-color` | Disable colored output (useful for logging to files or non-terminal output). |


**Output Verbosity Levels:**
- **Normal** (default): Standard progress updates, errors, and summaries.
- **Quiet** (`--quiet`): Only warnings, errors, and the final summary.
- **Verbose** (`--verbose`): More frequent progress updates that include processing rates and timing details.
- **Debug** (`--debug`): All output including queue states and internal operations for troubleshooting.

### Performance Options

| Option | Default | Description |
|--------|---------|-------------|
| `--walk-threads N` | 4 | Number of threads for file system traversal. |
| `--read-threads N` | 8 | Number of threads for reading and hashing files. |
| `--copy-threads N` | 8 | Number of threads for copying files. |
| `--hash-algo ALGO` | `md5` | Hashing algorithm to use (`md5` or `xxh64`). `xxh64` requires the `fast_hash` extra. |

### Path Conversion Options

| Option | Description |
|--------|-------------|
| `--convert-manifest-paths-from PREFIX` | Original path prefix in manifest to replace. |
| `--convert-manifest-paths-to PREFIX` | New path prefix (useful when drive letters or mount points change). |

## Path and Extension Rules

This section explains how to control file selection and organization using extension filters (`-e`), ignore patterns (`--ignore`), and path restructuring rules (`-R`).

### Filtering Files by Extension

Use the `-e` or `--extensions` option to specify which file types to process.

- **`jpg`**: Matches `.jpg` files.
- **`*.jp*g`**: Matches `.jpg`, `.jpeg`, `.jpng`, etc.
- **`*`**: Matches all extensions.

If no `-e` option is provided, all files are processed by default.

### Ignoring Files and Directories

Use `--ignore` to exclude files or directories that match a specific pattern.

- **`"*.tmp"`**: Ignores all files with a `.tmp` extension.
- **`"**/Thumbs.db"`**: Ignores `Thumbs.db` files in any directory.
- **`"*.cache"`**: Ignores all files ending in `.cache`.

### Restructuring Destination Paths

Path rules (`-R` or `--path-rules`) determine how files are organized in the destination directory. The format is `pattern:rule`.

**Default Behavior:** If no `-R` flag is specified, the original directory structure is preserved (equivalent to `-R "*:no_change"`). This is the most intuitive behavior for backup and copy operations.

#### Available Rules

| Rule        | Description                                     | Example Output                          |
|-------------|-------------------------------------------------|-----------------------------------------|
| `no_change` | **[DEFAULT]** Preserves the original directory structure | `/dest/original/path/file.jpg`          |
| `mtime`     | Organizes by modification date (`YYYY_MM`)      | `/dest/2024_03/file.jpg`                |
| `extension` | Organizes into folders by file extension        | `/dest/jpg/file.jpg`                    |

#### Combining Rules

Rules are applied in the order they are specified, creating nested directories.

```bash
# Organize first by extension, then by date
dedupecopy -p /source -c /backup -R "*:extension" -R "*:mtime"
```
**Result:** `/backup/jpg/2024_03/photo.jpg`

#### Pattern Matching for Rules

The `pattern` part of the rule determines which files the rule applies to. It supports wildcards, just like the `-e` filter.

- **`"*.jpg:mtime"`**: Applies the `mtime` rule only to JPG files.
- **`"*.jp*g:mtime"`**: Applies to `.jpg`, `.jpeg`, etc.
- **`"*:no_change"`**: Applies the `no_change` rule to all files.

The most specific pattern wins if multiple patterns could match a file.

#### Example: Different Rules for Different Files

```bash
dedupecopy -p /media -c /organized \
  -R "*.jpg:mtime" \
  -R "*.mp4:extension" \
  -R "*.pdf:no_change"
```
- **JPG files** are organized by date.
- **MP4 files** are organized into an `mp4` folder.
- **PDF files** keep their original directory structure.

## Advanced Workflows

### Sequential Multi-Source Backup

When consolidating from multiple sources to a single target while avoiding duplicates between sources:

#### Step 1: Create manifests for all locations

```bash
# Scan target (if it has existing files)
dedupecopy -p /backup/target -m target_manifest.db

# Scan each source
dedupecopy -p /source1 -m source1_manifest.db
dedupecopy -p /source2 -m source2_manifest.db
```

#### Step 2: Copy each source sequentially

```bash
# Copy source1 (skip files already in target or source2)
dedupecopy -p /source1 -c /backup/target \
  --compare target_manifest.db \
  --compare source2_manifest.db \
  -m target_v1.db \
  --no-walk

# Copy source2 (skip files already in target or source1)
dedupecopy -p /source2 -c /backup/target \
  --compare target_v1.db \
  --compare source1_manifest.db \
  -m target_v2.db \
  --no-walk
```

**How it works:**
- `--no-walk` skips re-scanning the filesystem (uses manifest data from `-i` or scans `--compare` manifests)
- `--compare` loads manifests for duplicate checking but doesn't copy those files
- Each source is copied only if files aren't already in target or other sources
- Each step creates a new manifest tracking what's been copied so far

**Note:** We use `--compare` instead of `-i` because:
- `-i` + `-m` cannot use the same file path (safety rule)
- `--compare` is designed for exactly this use case (deduplication across sources)
- The source manifests are used with `--no-walk` to avoid re-scanning

### Manifest Path Conversion

If drive letters or mount points change between runs:

```bash
dedupecopy -i old_manifest.db -m new_manifest.db \
  --convert-manifest-paths-from "/Volumes/OldDrive" \
  --convert-manifest-paths-to "/Volumes/NewDrive" \
  --no-walk
```

Updates all paths in the manifest without re-scanning files.

### Incremental Backup

To back up a directory and then incrementally update the backup with only new or modified files, you can use the following workflow:

#### Step 1: Initial Backup

Run an initial backup of the source directory, creating a manifest file.

```bash
dedupecopy -p /path/to/source -c /path/to/backup -m backup_v1.db
```

#### Step 2: Incremental Update

To perform an incremental backup, use the `--compare` flag to skip files already in the previous backup. This ensures only new or modified files are copied.

```bash
dedupecopy -p /path/to/source -c /path/to/backup --compare backup_v1.db -m backup_v2.db
```

**Important:** The `-i` (input manifest) and `-m` (output manifest) options **cannot use the same file path**. This safety feature prevents accidental manifest corruption. Always use `--compare` for incremental backups, which loads the manifest for duplicate checking without modifying it.

**How it works:**
- `--compare` loads the previous manifest to identify files already backed up
- Only new or modified files (different content hash) are copied
- A new manifest (`backup_v2.db`) is created with all files (old + new)
- The original manifest (`backup_v1.db`) remains unchanged

**Alternative: Simple incremental without separate manifests**

If you want to keep updating a single location without versioned manifests:

```bash
# Initial backup
dedupecopy -p /path/to/source -c /path/to/backup -m backup.db

# Later: Add new/modified files (no manifest for incremental)
dedupecopy -p /path/to/source -c /path/to/backup
```

Without loading a manifest, the tool will re-scan the source and copy any files not already in the destination. However, this is slower as it doesn't skip already-processed files during the scan.

### Comparison Without Copying

Compare two directories to find what's different:

```bash
# Scan both locations
dedupecopy -p /location1 -m manifest1.db
dedupecopy -p /location2 -m manifest2.db

# Generate report of files in location1 not in location2
dedupecopy -p /location1 -i manifest1.db --compare manifest2.db -r unique_files.csv --no-walk
```

## Performance Tips

### Thread Count Tuning

**Default settings (4/8/8)** work well for most scenarios.

**For SSDs/NVMe:**
```bash
--walk-threads 8 --read-threads 16 --copy-threads 16
```

**For HDDs:**
```bash
--walk-threads 2 --read-threads 4 --copy-threads 4
```

**For network shares:**
```bash
--walk-threads 2 --read-threads 4 --copy-threads 2
```
Network latency makes more threads counterproductive.

### Large File Sets

For very large directories (millions of files):

1. **Use manifests** - Essential for resumability
2. **Process in batches** - Use `--ignore` to exclude subdirectories, process separately
3. **Monitor memory** - Manifests use disk-based caching to minimize memory usage
4. **Incremental saves** - Manifests auto-save every 50,000 files

### Network Considerations

- **Network paths may timeout** - Tool retries after 3 seconds
- **SMB/CIFS shares** - Use lower thread counts
- **Bandwidth limits** - Reduce copy threads to avoid saturation
- **VPN connections** - May need much lower thread counts

### Manifest Storage

- Manifest files are stored as SQLite database files
- Size is proportional to number of unique files (typically a few MB per 100k files)
- Keep manifests on fast storage (SSD) for best performance
- Manifests are incrementally saved every 50,000 processed files

## Logging and Output Control

### Verbosity Levels

DedupeCopy provides flexible output control to suit different use cases:

#### Normal Mode (Default)
Standard output with progress updates every 1,000 files:

```bash
dedupecopy -p /source -c /destination
```

**Output includes:**
- Pre-flight configuration summary
- Progress updates with file counts and processing rates
- Error messages with helpful suggestions
- Final summary statistics

#### Quiet Mode
Minimal output - only warnings, errors, and final results:

```bash
dedupecopy -p /source -c /destination --quiet
```

**Best for:**
- Cron jobs and automated scripts
- When you only care about problems
- Reducing log file sizes

#### Verbose Mode
Detailed progress information:

```bash
dedupecopy -p /source -c /destination --verbose
```

**Output includes:**
- All normal mode output
- More frequent progress updates
- Detailed timing and rate information

#### Debug Mode
Comprehensive diagnostic information:

```bash
dedupecopy -p /source -c /destination --debug
```

**Output includes:**
- All verbose mode output
- Queue sizes and internal state
- Thread activity details
- Useful for troubleshooting performance issues

### Color Output

By default, DedupeCopy uses colored output when writing to a terminal (if colorama is installed):

- **Errors**: Red text
- **Warnings**: Yellow text
- **Info messages**: Default color
- **Debug messages**: Cyan text

To disable colors (e.g., when logging to a file):

```bash
dedupecopy -p /source -c /destination --no-color
```

Colors are automatically disabled when output is redirected to a file or pipe.

### Enhanced Features

#### Pre-Flight Summary
Before starting operations, you'll see a summary of configuration:

```
======================================================================
DEDUPE COPY - Operation Summary
======================================================================
Source path(s): 2 path(s)
  - /Volumes/Source1
  - /Volumes/Source2
Destination: /Volumes/Backup
Extension filter: jpg, png, gif
Path rules: *.jpg:mtime
Threads: walk=4, read=8, copy=8
Options: dedupe_empty=False, preserve_stat=True, no_walk=False
======================================================================
```

#### Progress with Rates
During operation, you'll see processing rates:

```
Discovered 5000 files (dirs: 250), accepted 4850. Rate: 142.3 files/sec
Work queue has 234 items. Progress queue has 12 items. Walk queue has 5 items.
...
Copied 4800 items. Skipped 50 items. Rate: 125.7 files/sec
```

#### Helpful Error Messages
Errors include context and suggestions:

```
Error processing '/path/to/file.txt': [PermissionError] Permission denied
  Suggestions: Check file permissions; Ensure you have read access to source files
```

#### Proactive Warnings
The tool warns about potential issues before they become problems:

```
WARNING: Work queue is large (42000 items). Consider reducing thread counts to avoid memory issues.
WARNING: Progress queue is backing up (12000 items). This may indicate slow processing.
```

### Examples

#### Silent operation for scripts
```bash
dedupecopy -p /source -c /backup --quiet 2>&1 | tee backup.log
```

#### Maximum detail for troubleshooting
```bash
dedupecopy -p /source -c /backup --debug --no-color > debug.log 2>&1
```

#### Normal operation with color
```bash
dedupecopy -p /source -c /backup --verbose
```

## Troubleshooting

### Common Issues

#### "Directory disappeared during walk"

**Cause:** Network path timeout or files deleted during scan.

**Solution:**
- Reduce `--walk-threads` for network paths
- Ensure stable network connection
- Exclude volatile directories with `--ignore`

#### Out of Memory Errors

**Cause:** Very large queue sizes.

**Solution:**
- Reduce thread counts
- Process in smaller batches
- Ensure sufficient swap space

#### Permission Errors

**Cause:** Insufficient permissions on source or destination.

**Solution:**
```bash
# Check permissions
ls -la /source/path
ls -la /destination/path

# Run with appropriate user or use sudo (carefully!)
```

#### Resuming Interrupted Runs

If a run is interrupted:

```bash
# Resume using the manifest
dedupecopy -p /source -c /destination -i manifest.db -m manifest.db
```

Files already processed (in manifest) are skipped.

#### Manifest Corruption

If manifest files become corrupted:

**Solution:**
- Delete manifest files and restart
- Manifest files: `manifest.db` and `manifest.db.read`
- Consider keeping backup copies of manifests for very long operations

### Getting Help

Check the output during run:
- Progress updates every 1,000 files with processing rates
- Error messages show problematic files with helpful suggestions
- Warnings alert you to potential issues proactively
- Final summary shows counts and errors

For debugging, use `--debug` mode:

```bash
dedupecopy -p /source -c /destination --debug --no-color > debug.log 2>&1
```

Debug output includes:
- File counts and progress with timing
- Queue sizes and internal state (useful if growing unbounded)
- Thread activity and performance metrics
- Specific error messages with file paths and suggestions

## Safety and Best Practices

### ⚠️ Important Warnings

1. **Test first**: Run with `-r` (report only) before using `-c` (copy) on important data
2. **Backup important data**: Always have backups before restructuring
3. **Use manifests**: They provide a record of what was processed
4. **Verify results**: Check file counts and spot-check files after copy operations
5. **Watch disk space**: Ensure sufficient space on destination

### Manifest Safety Rules

To prevent accidental data loss, DedupeCopy enforces the following rules for manifest usage:

1.  **Destructive Operations Require an Output Manifest**: Any operation that modifies the set of files being tracked (e.g., `--delete`, `--delete-on-copy`) **requires** the `-m`/`--manifest-dump-path` option. This ensures that the results of the operation are saved to a new manifest, preserving the original.

2.  **Input and Output Manifests Must Be Different**: To protect your original manifest, you cannot use the same file path for both `-i`/`--manifest-read-path` and `-m`/`--manifest-dump-path`. This prevents the input manifest from being overwritten.

**Example of a safe delete operation:**
```bash
# Load an existing manifest and save the changes to a new one
dedupecopy --no-walk --delete -i manifest_original.db -m manifest_after_delete.db
```

### Recommended Workflow

```bash
# Step 1: Generate report to understand what will happen
dedupecopy -p /source -r preview.csv -m preview_manifest.db

# Step 2: Review the CSV report
# Check duplicate counts, file types, sizes

# Step 3: Run the actual copy with manifest
dedupecopy -p /source -c /destination -i preview_manifest.db -m final_manifest.db

# Step 4: Verify
# Check file counts, spot-check files, verify important files copied
```

### What Gets Copied / Deleted

- **First occurrence** of each unique file (by MD5 hash) is kept.
- Subsequent identical files are either copied to the destination or deleted from the source.
- Files are considered unique if their MD5 hash differs.
- By default, empty files are treated as unique. Use `--dedupe-empty` to treat them as duplicates.
- Ignored patterns (`--ignore`) are never copied or deleted.

### What Doesn't Get Copied / Deleted

- The first-seen version of a file is never deleted.
- Files matching `--ignore` patterns.
- Files listed in `--compare` manifests (used for comparison only).
- Extensions not matching the `-e` filter (if specified).

### Preserving Metadata

By default, only file contents are copied. To preserve timestamps and permissions:

```bash
dedupecopy -p /source -c /destination --copy-metadata
```

This uses Python's `shutil.copy2()` which preserves:
- Modification time
- Access time
- File mode (permissions)

**Note:** Not all metadata may transfer across different file systems.

## Output Files

### CSV Duplicate Report

Format: `Collision #, MD5, Path, Size (bytes), mtime`

```csv
Src: ['/source/path']
Collision #, MD5, Path, Size (bytes), mtime
1, d41d8cd98f00b204e9800998ecf8427e, '/path/file1.jpg', 1024, 1633024800.0
1, d41d8cd98f00b204e9800998ecf8427e, '/path/file2.jpg', 1024, 1633024800.0
2, a3d5c12f8b9e4a1c2d3e4f5a6b7c8d9e, '/path/doc1.pdf', 2048, 1633111200.0
2, a3d5c12f8b9e4a1c2d3e4f5a6b7c8d9e, '/path/doc2.pdf', 2048, 1633111200.0
```

### Manifest Files

Binary database files (not human-readable):
- `manifest.db` - MD5 hashes and file metadata
- `manifest.db.read` - List of processed file paths

These enable resuming and incremental operations.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Simplified BSD License.

## Project Links

- **GitHub**: https://github.com/othererik/dedupe_copy
- **PyPI**: https://pypi.org/project/DedupeCopy/

## Author

Erik Schweller (othererik@gmail.com)

---

**Status**: Tested and seems to work, but use with caution and always backup important data!
