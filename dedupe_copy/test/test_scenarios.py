"""
Test scenarios for the dedupe_copy tool.
"""

import os
import subprocess
import unittest
import csv

from dedupe_copy.test.utils import (
    make_file_tree,
    make_temp_dir,
    remove_dir,
    walk_tree,
    get_random_dir_path,
    get_random_file_name,
    write_file,
)


class TestScenarios(unittest.TestCase):
    """Test various scenarios for the dedupe_copy tool."""

    def setUp(self):
        self.test_dir = make_temp_dir(description="test_scenarios")

    def tearDown(self):
        remove_dir(self.test_dir)

    def test_scenario_1_duplicate_report(self):
        """Scenario 1: Basic Duplicate Report Generation."""
        source_dir = os.path.join(self.test_dir, "source")
        os.makedirs(source_dir)
        report_path = os.path.join(self.test_dir, "duplicates.csv")

        # Create 10 unique files and 5 duplicates
        make_file_tree(source_dir, file_count=10, use_unique_files=True)
        make_file_tree(
            source_dir, file_count=5, use_unique_files=False, prefix="duplicate_"
        )

        # Run dedupecopy to generate the report
        subprocess.run(
            [
                "dedupecopy",
                "-p",
                source_dir,
                "-r",
                report_path,
            ],
            check=True,
        )

        # Verify the report
        self.assertTrue(os.path.exists(report_path))
        with open(report_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            # Skip the source path line
            next(reader)
            header = next(reader)
            self.assertEqual(
                header, ["Collision #", " MD5", " Path", " Size (bytes)", " mtime"]
            )

            # Count the number of collision sets
            collisions = {}
            for row in reader:
                if row:  # handle blank lines
                    collision_num = int(row[0])
                    if collision_num not in collisions:
                        collisions[collision_num] = []
                    collisions[collision_num].append(row)

            # There should be one set of collisions (the 5 duplicates of "same_content")
            self.assertEqual(len(collisions), 1)
            self.assertEqual(len(collisions[1]), 5)

    def test_scenario_2_copy_with_restructuring(self):
        """Scenario 2: Copying with Deduplication and Path Restructuring."""
        source_dir = os.path.join(self.test_dir, "source")
        dest_dir = os.path.join(self.test_dir, "dest")
        os.makedirs(source_dir)

        # Create 10 unique files and 5 duplicates with different extensions
        make_file_tree(
            source_dir,
            file_count=10,
            use_unique_files=True,
            extensions=[".jpg", ".png", ".txt"],
        )
        make_file_tree(
            source_dir,
            file_count=5,
            use_unique_files=False,
            prefix="duplicate_",
            extensions=[".jpg", ".png"],
        )

        # Run dedupecopy with copy and path restructuring
        subprocess.run(
            [
                "dedupecopy",
                "-p",
                source_dir,
                "-c",
                dest_dir,
                "-R",
                "*:extension",
            ],
            check=True,
        )

        # Verify the destination directory
        dest_files = list(walk_tree(dest_dir))

        # Total unique files = 10 unique + 1 from the "same_content" set
        self.assertEqual(len(dest_files), 11)

        # Check for correct path restructuring (e.g., dest/jpg/file.jpg)
        for f in dest_files:
            rel_path = os.path.relpath(f, dest_dir)
            parts = rel_path.split(os.sep)
            self.assertEqual(len(parts), 2)  # [extension, filename]
            extension = os.path.splitext(parts[1])[1][1:]
            self.assertEqual(parts[0], extension)

    def test_scenario_3_delete_duplicates(self):
        """Scenario 3: Deleting Duplicates with Dry Run and Size Filter."""
        source_dir = os.path.join(self.test_dir, "source")
        os.makedirs(source_dir)

        # Create 2 sets of duplicates, one small and one large
        make_file_tree(
            source_dir,
            file_count=3,
            use_unique_files=False,
            prefix="small_dupe_",
            file_size=500,
        )
        make_file_tree(
            source_dir,
            file_count=3,
            use_unique_files=False,
            prefix="large_dupe_",
            file_size=1500,
        )
        initial_files = list(walk_tree(source_dir))
        self.assertEqual(len(initial_files), 6)

        manifest_path = os.path.join(self.test_dir, "manifest.db")

        # Dry run: ensure no files are deleted
        subprocess.run(
            [
                "dedupecopy",
                "-p",
                source_dir,
                "--delete",
                "--dry-run",
                "-m",
                manifest_path,
            ],
            check=True,
        )
        self.assertEqual(len(list(walk_tree(source_dir))), 6)

        # Actual delete with size filter
        subprocess.run(
            [
                "dedupecopy",
                "-p",
                source_dir,
                "--delete",
                "--min-delete-size",
                "1000",
                "-m",
                manifest_path,
            ],
            check=True,
        )

        # Verify deletion
        remaining_files = list(walk_tree(source_dir))
        # 3 small files + 1 original large file should remain
        self.assertEqual(len(remaining_files), 4)

        # Ensure the correct files were deleted (only large dupes)
        remaining_filenames = {os.path.basename(f) for f in remaining_files}
        self.assertTrue(any("small_dupe_" in f for f in remaining_filenames))
        self.assertTrue(any("large_dupe_" in f for f in remaining_filenames))

        # Count remaining large files
        large_files_remaining = [f for f in remaining_filenames if "large_dupe_" in f]
        self.assertEqual(len(large_files_remaining), 1)

    def test_scenario_4_incremental_backup(self):
        """Scenario 4: Incremental Backup using Manifests."""
        source_dir = os.path.join(self.test_dir, "source")
        dest_dir = os.path.join(self.test_dir, "dest")
        manifest_path = os.path.join(self.test_dir, "manifest.db")
        os.makedirs(source_dir)

        # Initial backup
        make_file_tree(source_dir, file_count=10, use_unique_files=True)
        subprocess.run(
            ["dedupecopy", "-p", source_dir, "-c", dest_dir, "-m", manifest_path],
            check=True,
        )
        dest_files_initial = list(walk_tree(dest_dir))
        self.assertEqual(len(dest_files_initial), 10)

        # Add 5 new, unique files to the source
        for i in range(5):
            fname = get_random_file_name(
                root=get_random_dir_path(source_dir), prefix="new_", extensions=[".txt"]
            )
            write_file(fname, seed=i + 10, initial=f"unique_content_{i}")

        # Incremental backup
        subprocess.run(
            [
                "dedupecopy",
                "-p",
                source_dir,
                "-c",
                dest_dir,
                "-i",
                manifest_path,
                "-m",
                manifest_path + ".new",
            ],
            check=True,
        )

        # Verify the destination directory
        dest_files_final = list(walk_tree(dest_dir))
        self.assertEqual(len(dest_files_final), 15)

        # Check that new files were copied
        new_files_in_dest = [
            f for f in dest_files_final if "new_" in os.path.basename(f)
        ]
        self.assertEqual(len(new_files_in_dest), 5)

    def test_scenario_5_multi_source_consolidation(self):
        """Scenario 5: Multi-Source Consolidation."""
        source1_dir = os.path.join(self.test_dir, "source1")
        source2_dir = os.path.join(self.test_dir, "source2")
        dest_dir = os.path.join(self.test_dir, "dest")
        manifest1_path = os.path.join(self.test_dir, "manifest1.db")
        os.makedirs(source1_dir)
        os.makedirs(source2_dir)

        # Create one file in source1
        write_file(os.path.join(source1_dir, "file1.txt"), seed=0, initial="content1")

        # Create a duplicate in source2 and a new file
        write_file(
            os.path.join(source2_dir, "file1_dupe.txt"), seed=0, initial="content1"
        )
        write_file(os.path.join(source2_dir, "file2.txt"), seed=1, initial="content2")

        # Copy source1 to dest, creating manifest1
        subprocess.run(
            ["dedupecopy", "-p", source1_dir, "-c", dest_dir, "-m", manifest1_path],
            check=True,
        )
        self.assertEqual(len(list(walk_tree(dest_dir))), 1)

        # Copy source2 to dest, comparing against manifest1
        result = subprocess.run(
            [
                "dedupecopy",
                "-p",
                source2_dir,
                "-c",
                dest_dir,
                "--compare",
                manifest1_path,
                "-m",
                os.path.join(self.test_dir, "manifest2.db"),
                "--verbose",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        # Verify destination: should contain file1.txt and file2.txt
        dest_files = list(walk_tree(dest_dir))
        self.assertEqual(
            len(dest_files), 2, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

        # Verify that the duplicate was skipped
        self.assertIn("Skipped 1", result.stdout)

    def test_scenario_6_delete_on_copy(self):
        """Scenario 6: Delete on Copy."""
        source_dir = os.path.join(self.test_dir, "source")
        dest_dir = os.path.join(self.test_dir, "dest")
        os.makedirs(source_dir)

        # Create 10 unique files and 5 duplicates
        make_file_tree(source_dir, file_count=10, use_unique_files=True)
        make_file_tree(
            source_dir, file_count=5, use_unique_files=False, prefix="duplicate_"
        )

        # Run dedupecopy with delete-on-copy
        subprocess.run(
            [
                "dedupecopy",
                "-p",
                source_dir,
                "-c",
                dest_dir,
                "--delete-on-copy",
                "-m",
                os.path.join(self.test_dir, "manifest.db"),
            ],
            check=True,
        )

        # Verify destination
        dest_files = list(walk_tree(dest_dir))
        self.assertEqual(len(dest_files), 11)

        # Verify source
        source_files = list(walk_tree(source_dir))
        # The 4 duplicates that were not copied should remain
        self.assertEqual(len(source_files), 4)
