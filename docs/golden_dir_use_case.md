# Golden Directory Backup Use Case

This document describes how to use `dedupecopy` to maintain a "golden" backup directory by copying new, unique files from multiple source directories.

## Scenario

You have a central backup directory (the "golden" directory) and multiple source directories. The source directories may contain files that are already in the golden directory, as well as new files that need to be backed up. You want to copy only the new files from the source directories to the golden directory, avoiding the creation of duplicate files.

## Commands

To achieve this, you first generate a manifest of the golden directory, and then use that manifest to check for duplicates when copying from the source directories.

### 1. Generate a manifest for the golden directory:

```bash
dedupecopy --read-path /path/to/golden_directory --manifest-dump-path /path/to/golden_manifest.json
```

### 2. Copy new files and skip duplicates:

This command will copy only the files that are not already present in the golden directory, leaving the source directories untouched.

```bash
dedupecopy --read-path /path/to/source_dir1 /path/to/source_dir2 \
    --compare /path/to/golden_manifest.json \
    --copy-path /path/to/golden_directory \
    -R *:no_change
```

### 3. Copy new files and delete all processed source files:

To clean up the source directories after the backup, you can add the `--delete-on-copy` flag. This will copy new files to the golden directory and then delete *all* files from the source directories that were processed—both the newly copied files and the duplicates that were already in the golden directory.

```bash
dedupecopy --read-path /path/to/source_dir1 /path/to/source_dir2 \
    --compare /path/to/golden_manifest.json \
    --copy-path /path/to/golden_directory \
    --delete-on-copy \
    -R *:no_change
```
This ensures that your source directories are left empty of backed-up files, ready for new data.