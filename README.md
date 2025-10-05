# DedupeCopy

A multi-threaded command-line tool for finding duplicate files and copying/restructuring file layouts while eliminating duplicates.

[![License](https://img.shields.io/badge/license-BSD-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)

## Table of Contents

- [Overview](#overview)
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

### From source

```bash
git clone https://github.com/othererik/dedupe_copy.git
cd dedupe_copy
pip install -e .
# Or with color support:
pip install -e .[color]
```

### Requirements

- Python 3.8 or later
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

Creates a manifest file that allows you to resume if interrupted.

## Key Concepts

### Manifests

Manifests are database files that store:
- MD5 checksums of processed files
- File metadata (size, modification time, path)
- Which files have been scanned

**Benefits:**
- Resume interrupted operations
- Compare file systems without re-scanning
- Incremental backup workflows
- Track what has been processed

**Usage:**
- `-m manifest.db` - Save manifest after processing
- `-i manifest.db` - Load existing manifest before processing
- Manifest files are stored in a disk-based cache format (`.db` extension)

### Duplicate Detection

Files are considered duplicates when:
1. They have identical MD5 checksums
2. They have the same file size

**Special case:** Empty (zero-byte) files are treated as duplicates by default. Use `--keep-empty` to treat each empty file as unique.

### Path Rules

Path rules control how files are organized in the destination:

| Rule | Description | Example Output |
|------|-------------|----------------|
| `no_change` | Preserve original directory structure | `/dest/original/path/file.jpg` |
| `mtime` | Organize by modification date (YYYY_MM) | `/dest/2024_03/file.jpg` |
| `extension` | Organize by file extension | `/dest/jpg/file.jpg` |

Rules can be combined and applied per extension pattern (see [Path Rules](#path-rules-1) section).

### Extension Patterns

Extension filters support wildcards:
- `jpg` - Match .jpg files
- `*.jp*g` - Match .jpg, .jpeg, .jpng, etc.
- `*` - Match all extensions

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

#### 3. Copy with preserved directory structure

```bash
dedupecopy -p /source -c /backup -R "*:no_change"
```

The `*` pattern applies the `no_change` rule to all file types.

### Photo Organization

#### Organize photos by date

```bash
dedupecopy -p C:\pics -p D:\pics -e jpg -R "jpg:mtime" -c X:\organized_photos
```

Copies all JPG files from C: and D: drives, organizing them into folders by year/month (e.g., `2024_03/`).

#### Organize by extension AND date

```bash
dedupecopy -p /media -c /organized -R "*:extension" -R "*:mtime"
```

Creates structure like: `/organized/jpg/2024_03/photo.jpg`

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
| `-c PATH`, `--copy-path PATH` | Destination path for copying files. |
| `-r PATH`, `--result-path PATH` | Output path for CSV duplicate report. |
| `-m PATH`, `--manifest-dump-path PATH` | Path to save manifest file. |
| `-i PATH`, `--manifest-read-path PATH` | Path to load existing manifest. Can be specified multiple times. |
| `-e EXT`, `--extensions EXT` | File extension(s) to include (e.g., `jpg`, `*.png`). Can be specified multiple times. |
| `--ignore PATTERN` | File pattern(s) to exclude (supports wildcards). Can be specified multiple times. |
| `-R RULE`, `--path-rules RULE` | Path restructuring rule(s) in format `extension:rule`. Can be specified multiple times. |

### Special Options

| Option | Description |
|--------|-------------|
| `--compare PATH` | Load manifest but don't copy its files (for comparison only). Can be specified multiple times. |
| `--copy-metadata` | Preserve file timestamps and permissions (uses `shutil.copy2` instead of `copyfile`). |
| `--keep-empty` | Treat empty (0-byte) files as unique rather than duplicates. |
| `--ignore-old-collisions` | Only detect new duplicates (ignore duplicates already in loaded manifest). |

### Output Control Options

| Option | Description |
|--------|-------------|
| `-q`, `--quiet` | Show only warnings and errors (minimal output). |
| `-v`, `--verbose` | Show detailed progress information (same as normal, kept for compatibility). |
| `--debug` | Show debug information including queue states and internal diagnostics. |
| `--no-color` | Disable colored output (useful for logging to files or non-terminal output). |


**Output Verbosity Levels:**
- **Normal** (default): Standard progress updates every 1,000 files, errors, and summaries
- **Quiet** (`--quiet`): Only warnings, errors, and final summary
- **Verbose** (`--verbose`): Detailed progress with processing rates and timing
- **Debug** (`--debug`): All output including queue states and internal operations

### Performance Options

| Option | Default | Description |
|--------|---------|-------------|
| `--walk-threads N` | 4 | Number of threads for file system traversal. |
| `--read-threads N` | 8 | Number of threads for reading and hashing files. |
| `--copy-threads N` | 8 | Number of threads for copying files. |

### Path Conversion Options

| Option | Description |
|--------|-------------|
| `--convert-manifest-paths-from PREFIX` | Original path prefix in manifest to replace. |
| `--convert-manifest-paths-to PREFIX` | New path prefix (useful when drive letters or mount points change). |

## Path Rules

Path rules determine how files are organized in the destination directory. Multiple rules can be applied to create nested structures.

### Available Rules

#### `no_change`
Preserves the original directory structure from the source path.

```bash
dedupecopy -p /source/photos -c /backup -R "*:no_change"
```

Result: `/backup/photos/2023/vacation/photo.jpg`

#### `mtime`
Organizes files by modification date in `YYYY_MM` format.

```bash
dedupecopy -p /source -c /backup -R "*.jpg:mtime"
```

Result: `/backup/2024_03/photo.jpg`

#### `extension`
Organizes files into folders by extension.

```bash
dedupecopy -p /source -c /backup -R "*:extension"
```

Result: `/backup/jpg/photo.jpg`

### Combining Rules

Rules are applied in the order specified and create nested directories:

```bash
dedupecopy -p /source -c /backup -R "*:extension" -R "*:mtime"
```

Result: `/backup/jpg/2024_03/photo.jpg`

### Extension-Specific Rules

Apply different rules to different file types:

```bash
dedupecopy -p /source -c /backup \
  -R "*.jpg:mtime" \
  -R "*.mp4:extension" \
  -R "*.pdf:no_change"
```

- JPG files organized by date
- MP4 files organized by extension
- PDF files keep original structure

### Wildcard Matching

Extension patterns support wildcards:

```bash
-R "*.jp*:mtime"        # Matches .jpg, .jpeg, .jpe, etc.
-R "image*.png:extension"  # Matches image1.png, image_photo.png, etc.
-R "*:no_change"        # Applies to all files
```

The most specific pattern wins when multiple patterns could match.

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
# Copy source1 (comparing against target)
dedupecopy -p /source1 -c /backup/target \
  -i source1_manifest.db \
  --compare target_manifest.db \
  --no-walk

# Copy source2 (comparing against target AND source1)
dedupecopy -p /source2 -c /backup/target \
  -i source2_manifest.db \
  --compare target_manifest.db \
  --compare source1_manifest.db \
  --no-walk
```

**How it works:**
- `--no-walk` skips re-scanning (uses manifest data)
- `--compare` loads manifests for duplicate checking but doesn't copy those files
- Each source is copied only if files aren't already in target or previous sources

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

```bash
# Initial backup
dedupecopy -p /photos -c /backup/photos -m backup_manifest.db

# Later, add new photos (resuming/adding to existing backup)
dedupecopy -p /photos -c /backup/photos -i backup_manifest.db -m backup_manifest.db
```

Only new or modified files are processed.

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

- Manifest files are stored as Berkeley DB files
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
Options: keep_empty=False, preserve_stat=True, no_walk=False
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

### What Gets Copied

- **First occurrence** of each unique file (by MD5 hash)
- Files are considered unique if MD5 differs
- Empty files are duplicates unless `--keep-empty` is used
- Ignored patterns (`--ignore`) are never copied

### What Doesn't Get Copied

- Duplicate files (already seen MD5)
- Files matching `--ignore` patterns
- Files in `--compare` manifests (used for comparison only)
- Extensions not matching `-e` filter (if specified)

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

**Version**: 1.0.0

**Status**: Tested and seems to work, but use with caution and always backup important data!
