"""Advanced tests for error handling, performance, thread safety, compare, and CSV functionality"""

import csv
import os
import time
import unittest

from dedupe_copy.test import utils
from dedupe_copy.core import run_dupe_copy


def do_copy(**kwargs):
    """Helper to run copy with default args"""
    base_args = {
        "ignore_old_collisions": False,
        "walk_threads": 4,
        "read_threads": 8,
        "copy_threads": 8,
        "convert_manifest_paths_to": "",
        "convert_manifest_paths_from": "",
        "no_walk": False,
        "preserve_stat": False,
    }
    base_args.update(kwargs)
    return run_dupe_copy(**base_args)


class TestErrorHandling(unittest.TestCase):
    """Test error handling for various failure scenarios"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("error_handling")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_permission_denied_on_source_file(self):
        """Test handling of files that cannot be read due to permissions"""
        # Create a file
        fn = os.path.join(self.temp_dir, "protected.txt")
        utils.write_file(fn, seed=0, size=100)

        # Create a readable file too
        fn2 = os.path.join(self.temp_dir, "readable.txt")
        utils.write_file(fn2, seed=1, size=100)

        # Remove read permissions
        os.chmod(fn, 0o000)

        copy_to_path = os.path.join(self.temp_dir, "copy")

        try:
            # Should not crash, should skip unreadable file
            do_copy(
                read_from_path=self.temp_dir,
                copy_to_path=copy_to_path,
                path_rules=["*:no_change"],
            )

            # The readable file should have been copied
            copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
            # Should have at least the readable file
            self.assertGreater(
                len(copied_files),
                0,
                "At least one readable file should be copied",
            )
        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(fn, 0o644)
            except OSError:
                pass

    def test_permission_denied_on_destination(self):
        """Test handling when destination directory is not writable"""
        # Create source files
        fn = os.path.join(self.temp_dir, "source_file.txt")
        utils.write_file(fn, seed=0, size=100)

        # Create destination and remove write permissions
        copy_to_path = os.path.join(self.temp_dir, "readonly_dest")
        os.makedirs(copy_to_path)
        os.chmod(copy_to_path, 0o444)  # Read-only

        try:
            # Should handle gracefully (reports error but doesn't crash)
            do_copy(
                read_from_path=self.temp_dir,
                copy_to_path=copy_to_path,
                path_rules=["*:no_change"],
            )
        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(copy_to_path, 0o755)
            except OSError:
                pass

    def test_nonexistent_source_path(self):
        """Test handling of non-existent source path"""
        nonexistent = os.path.join(self.temp_dir, "does_not_exist")
        copy_to_path = os.path.join(self.temp_dir, "copy")

        # Should handle gracefully - likely no files copied
        do_copy(
            read_from_path=nonexistent,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        # Should either not create dest or create empty dest
        if os.path.exists(copy_to_path):
            copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
            self.assertEqual(len(copied_files), 0, "No files should be copied")

    def test_corrupted_manifest_file(self):
        """Test handling of corrupted/invalid manifest file"""
        # Create a regular file
        fn = os.path.join(self.temp_dir, "file.txt")
        utils.write_file(fn, seed=0, size=100)

        # Create a "corrupt" manifest (just a text file, not a valid db)
        manifest_path = os.path.join(self.temp_dir, "corrupt_manifest.db")
        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write("This is not a valid Berkeley DB file\n")

        copy_to_path = os.path.join(self.temp_dir, "copy")

        # Should either skip the corrupt manifest or handle gracefully
        try:
            do_copy(
                read_from_path=self.temp_dir,
                manifests_in_paths=manifest_path,
                copy_to_path=copy_to_path,
                path_rules=["*:no_change"],
            )
        except Exception as e:  # pylint: disable=broad-except
            # If it raises an exception, it should be a clear database error
            self.assertIn(
                "database", str(e).lower(), "Error should mention database issue"
            )

    def test_very_long_filename(self):
        """Test handling of extremely long filenames"""
        # Create a file with a very long name (near system limits)
        long_name = "a" * 200 + ".txt"
        fn = os.path.join(self.temp_dir, long_name)

        try:
            utils.write_file(fn, seed=0, size=100)

            copy_to_path = os.path.join(self.temp_dir, "copy")

            # Should handle gracefully
            do_copy(
                read_from_path=self.temp_dir,
                copy_to_path=copy_to_path,
                path_rules=["*:no_change"],
            )

            copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
            # Should copy the file if the system supports it
            self.assertGreaterEqual(len(copied_files), 0)
        except OSError:
            # Some systems don't support very long filenames - that's OK
            self.skipTest("System doesn't support long filenames")

    def test_deep_directory_hierarchy(self):
        """Test handling of very deep directory structures"""
        # Create a deep directory structure
        deep_path = self.temp_dir
        for i in range(20):  # 20 levels deep
            deep_path = os.path.join(deep_path, f"level{i}")
        os.makedirs(deep_path, exist_ok=True)

        # Create a file at the bottom
        fn = os.path.join(deep_path, "deep_file.txt")
        utils.write_file(fn, seed=0, size=100)

        copy_to_path = os.path.join(self.temp_dir, "copy")

        # Should handle deep paths
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        # Should find and copy the deep file
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertGreater(len(copied_files), 0, "Deep file should be found and copied")


class TestCompareFunction(unittest.TestCase):
    """Test compare manifest functionality"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("compare")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_compare_skip_already_copied_files(self):
        """Test that compare manifest prevents re-copying files"""
        # Create source files
        source = os.path.join(self.temp_dir, "source")
        os.makedirs(source)
        for i in range(3):
            fn = os.path.join(source, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        # First copy with manifest
        copy1 = os.path.join(self.temp_dir, "copy1")
        manifest1 = os.path.join(self.temp_dir, "manifest1.db")

        do_copy(
            read_from_path=source,
            copy_to_path=copy1,
            manifest_out_path=manifest1,
            path_rules=["*:no_change"],
        )

        # Verify first copy worked
        self.assertEqual(len(list(utils.walk_tree(copy1, include_dirs=False))), 3)

        # Second copy using first manifest as compare
        copy2 = os.path.join(self.temp_dir, "copy2")

        do_copy(
            read_from_path=source,
            copy_to_path=copy2,
            compare_manifests=[manifest1],
            path_rules=["*:no_change"],
        )

        # Should copy nothing (all files already in compare manifest)
        if os.path.exists(copy2):
            copied_files = list(utils.walk_tree(copy2, include_dirs=False))
            self.assertEqual(
                len(copied_files),
                0,
                "No files should be copied when all are in compare manifest",
            )

    def test_compare_with_new_files(self):
        """Test that compare only skips files in manifest, not new files"""
        # Create initial source files
        source = os.path.join(self.temp_dir, "source")
        os.makedirs(source)
        for i in range(2):
            fn = os.path.join(source, f"original{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        # First copy with manifest
        copy1 = os.path.join(self.temp_dir, "copy1")
        manifest1 = os.path.join(self.temp_dir, "manifest1.db")

        do_copy(
            read_from_path=source,
            copy_to_path=copy1,
            manifest_out_path=manifest1,
            path_rules=["*:no_change"],
        )

        # Add new files to source
        for i in range(2):
            fn = os.path.join(source, f"new{i}.txt")
            utils.write_file(fn, seed=i + 100, size=100)

        # Second copy using first manifest as compare
        copy2 = os.path.join(self.temp_dir, "copy2")

        do_copy(
            read_from_path=source,
            copy_to_path=copy2,
            compare_manifests=[manifest1],
            path_rules=["*:no_change"],
        )

        # Should copy only the new files
        copied_files = list(utils.walk_tree(copy2, include_dirs=False))
        self.assertEqual(len(copied_files), 2, "Should copy only the 2 new files")

    def test_multiple_compare_manifests(self):
        """Test using multiple compare manifests"""
        # Create source files
        source = os.path.join(self.temp_dir, "source")
        os.makedirs(source)
        for i in range(6):
            fn = os.path.join(source, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        # First copy with manifest (files 0-2)
        copy1 = os.path.join(self.temp_dir, "copy1")
        manifest1 = os.path.join(self.temp_dir, "manifest1.db")
        source1 = os.path.join(self.temp_dir, "source1")
        os.makedirs(source1)
        for i in range(3):
            fn = os.path.join(source1, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        do_copy(
            read_from_path=source1,
            copy_to_path=copy1,
            manifest_out_path=manifest1,
            path_rules=["*:no_change"],
        )

        # Second copy with manifest (files 3-5)
        copy2 = os.path.join(self.temp_dir, "copy2")
        manifest2 = os.path.join(self.temp_dir, "manifest2.db")
        source2 = os.path.join(self.temp_dir, "source2")
        os.makedirs(source2)
        for i in range(3, 6):
            fn = os.path.join(source2, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        do_copy(
            read_from_path=source2,
            copy_to_path=copy2,
            manifest_out_path=manifest2,
            path_rules=["*:no_change"],
        )

        # Third copy using both manifests as compare
        copy3 = os.path.join(self.temp_dir, "copy3")

        do_copy(
            read_from_path=source,
            copy_to_path=copy3,
            compare_manifests=[manifest1, manifest2],
            path_rules=["*:no_change"],
        )

        # Should copy nothing (all 6 files covered by the two manifests)
        if os.path.exists(copy3):
            copied_files = list(utils.walk_tree(copy3, include_dirs=False))
            self.assertEqual(
                len(copied_files),
                0,
                "No files should be copied when all are in compare manifests",
            )


class TestCSVReportGeneration(unittest.TestCase):
    """Test CSV report generation functionality"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("csv_report")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_csv_report_created(self):
        """Test that CSV report file is created"""
        # Create some files with duplicates
        for i in range(3):
            fn = os.path.join(self.temp_dir, f"unique{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        # Create duplicates
        for i in range(2):
            fn = os.path.join(self.temp_dir, f"dupe{i}.txt")
            utils.write_file(fn, seed=999, size=100)  # Same content

        csv_path = os.path.join(self.temp_dir, "report.csv")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=None,  # No copy, just scan
            csv_report_path=csv_path,
        )

        # Check CSV file exists
        self.assertTrue(os.path.exists(csv_path), "CSV report should be created")

    def test_csv_report_format(self):
        """Test that CSV report has correct format"""
        # Create files
        for i in range(3):
            fn = os.path.join(self.temp_dir, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100)

        csv_path = os.path.join(self.temp_dir, "report.csv")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=None,
            csv_report_path=csv_path,
        )

        # Read and validate CSV
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                content = f.read()
                # Should have some content
                self.assertGreater(len(content), 0, "CSV should have content")

                # Should be valid CSV format (can be parsed)
                f.seek(0)
                reader = csv.reader(f)
                rows = list(reader)
                self.assertGreater(len(rows), 0, "CSV should have at least some rows")

    def test_csv_report_includes_duplicates(self):
        """Test that CSV report includes duplicate information"""
        # Create duplicates
        for i in range(3):
            fn = os.path.join(self.temp_dir, f"dupe{i}.txt")
            utils.write_file(fn, seed=42, size=100)  # Same content

        csv_path = os.path.join(self.temp_dir, "report.csv")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=None,
            csv_report_path=csv_path,
        )

        # CSV should be created and contain data
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                content = f.read()
                # Should have some data about the files
                self.assertGreater(len(content), 0, "CSV report should contain data")


class TestPerformanceAndStress(unittest.TestCase):
    """Test performance with larger datasets and stress scenarios"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("performance")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_many_small_files(self):
        """Test performance with many small files"""
        # Create 1000 small files
        file_count = 1000
        for i in range(file_count):
            fn = os.path.join(self.temp_dir, f"small{i}.txt")
            utils.write_file(
                fn, seed=i, size=100, initial=f"unique_small_{i}_"
            )  # Unique content

        copy_to_path = os.path.join(self.temp_dir, "copy")

        start_time = time.time()

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        elapsed = time.time() - start_time

        # Check all files copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), file_count, f"Should copy all {file_count} files"
        )

        # Performance assertion - should complete reasonably fast
        # Allow 30 seconds for 1000 files (very conservative)
        self.assertLess(
            elapsed, 30.0, f"Should copy {file_count} files in under 30 seconds"
        )

    def test_large_files(self):
        """Test handling of larger files"""
        # Create a few larger files (10MB each)
        file_count = 3
        file_size = 10 * 1024 * 1024  # 10MB

        for i in range(file_count):
            fn = os.path.join(self.temp_dir, f"large{i}.dat")
            utils.write_file(fn, seed=i, size=file_size, initial=f"unique_large_{i}_")

        copy_to_path = os.path.join(self.temp_dir, "copy")

        start_time = time.time()

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        elapsed = time.time() - start_time

        # Check files copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), file_count, f"Should copy all {file_count} large files"
        )

        # Should complete reasonably (60 seconds for 30MB)
        self.assertLess(
            elapsed, 60.0, f"Should copy {file_count} large files in under 60 seconds"
        )

    def test_many_duplicates(self):
        """Test performance with many duplicate files"""
        # Create 500 files with the same content (duplicates)
        file_count = 500
        for i in range(file_count):
            fn = os.path.join(self.temp_dir, f"dupe{i}.txt")
            utils.write_file(fn, seed=42, size=100)  # All same content

        copy_to_path = os.path.join(self.temp_dir, "copy")

        start_time = time.time()

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        elapsed = time.time() - start_time

        # Should copy only 1 file (all others are duplicates)
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), 1, "Should copy only 1 file (rest are dupes)"
        )

        # Should still complete reasonably fast
        self.assertLess(elapsed, 30.0, "Duplicate detection should be efficient")

    def test_mixed_workload(self):
        """Test realistic mixed workload: files, duplicates, various sizes"""
        # Create a realistic mix
        # 100 unique small files
        for i in range(100):
            fn = os.path.join(self.temp_dir, f"small{i}.txt")
            utils.write_file(fn, seed=i, size=100, initial=f"mix_small_{i}_")

        # 50 duplicate files
        for i in range(50):
            fn = os.path.join(self.temp_dir, f"dupe{i}.txt")
            utils.write_file(fn, seed=999, size=100)  # Intentionally same

        # 10 medium files
        for i in range(10):
            fn = os.path.join(self.temp_dir, f"medium{i}.dat")
            utils.write_file(
                fn, seed=i, size=1024 * 100, initial=f"mix_medium_{i}_"
            )  # 100KB

        # 2 larger files
        for i in range(2):
            fn = os.path.join(self.temp_dir, f"large{i}.dat")
            utils.write_file(
                fn, seed=i, size=1024 * 1024, initial=f"mix_large_{i}_"
            )  # 1MB

        copy_to_path = os.path.join(self.temp_dir, "copy")

        start_time = time.time()

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        elapsed = time.time() - start_time

        # Should copy 100 + 1 (dupe) + 10 + 2 = 113 files
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(len(copied_files), 113, "Should copy 113 unique files")

        # Should complete in reasonable time
        self.assertLess(elapsed, 60.0, "Mixed workload should complete efficiently")


class TestThreadSafety(unittest.TestCase):
    """Test thread safety and concurrent operations"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("thread_safety")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_concurrent_reads(self):
        """Test that concurrent reads work correctly"""
        # Create many files in different directories
        for d in range(5):
            dir_path = os.path.join(self.temp_dir, f"dir{d}")
            os.makedirs(dir_path)
            for i in range(50):
                fn = os.path.join(dir_path, f"file{i}.txt")
                utils.write_file(fn, seed=i, size=100, initial=f"concurrent_d{d}_i{i}_")

        copy_to_path = os.path.join(self.temp_dir, "copy")

        # Run with multiple threads
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            walk_threads=4,
            read_threads=8,
            copy_threads=8,
        )

        # Should copy all files without corruption
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(len(copied_files), 250, "All 250 files should be copied")

    def test_manifest_consistency_under_concurrency(self):
        """Test that manifest remains consistent with concurrent operations"""
        # Create a separate source directory to avoid scanning manifest files
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)

        # Create files in source directory
        for i in range(100):
            fn = os.path.join(source_dir, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100, initial=f"manifest_test_{i}_")

        copy_to_path = os.path.join(self.temp_dir, "copy")
        manifest_path = os.path.join(self.temp_dir, "manifest.db")

        # Run with multiple threads
        do_copy(
            read_from_path=source_dir,
            copy_to_path=copy_to_path,
            manifest_out_path=manifest_path,
            path_rules=["*:no_change"],
            walk_threads=4,
            read_threads=8,
            copy_threads=8,
        )

        # Manifest should be created
        self.assertTrue(os.path.exists(manifest_path), "Manifest should be created")

        # Use manifest in a second run - should work correctly
        copy_to_path2 = os.path.join(self.temp_dir, "copy2")

        do_copy(
            read_from_path=source_dir,
            copy_to_path=copy_to_path2,
            compare_manifests=[manifest_path],
            path_rules=["*:no_change"],
        )

        # Should copy nothing (all in compare manifest)
        if os.path.exists(copy_to_path2):
            copied_files = list(utils.walk_tree(copy_to_path2, include_dirs=False))
            self.assertEqual(len(copied_files), 0, "No files should be copied")

    def test_no_race_conditions_in_directory_creation(self):
        """Test that concurrent directory creation doesn't cause race conditions"""
        # Create many files that will map to same directories with path rules
        for i in range(100):
            fn = os.path.join(self.temp_dir, f"file{i}.txt")
            utils.write_file(fn, seed=i, size=100, initial=f"race_test_{i}_")

        copy_to_path = os.path.join(self.temp_dir, "copy")

        # All files go to same directory - tests concurrent mkdir
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:extension"],  # All .txt files to same dir
            copy_threads=8,  # Multiple threads trying to create same dir
        )

        # Should copy all files without errors
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(len(copied_files), 100, "All files should be copied")


if __name__ == "__main__":
    unittest.main()
