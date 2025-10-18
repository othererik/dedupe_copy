"""Enhanced tests for copy operations covering all user-facing features"""

import os
import unittest
import datetime
import time

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


def do_copy_with_rules(path_rule_strings, **kwargs):
    """Helper to convert path rule strings to callable and run copy"""
    base_args = {
        "path_rules": path_rule_strings,
        "ignore_old_collisions": False,
        "walk_threads": 4,
        "read_threads": 8,
        "copy_threads": 8,
        "convert_manifest_paths_to": "",
        "convert_manifest_paths_from": "",
        "no_walk": False,
    }
    base_args.update(kwargs)
    return run_dupe_copy(**base_args)


class TestPathRules(unittest.TestCase):
    """Test all path rule functionality"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("path_rules")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_mtime_rule_only(self):
        """Test organizing files by modification time (YYYY_MM)"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=5, extensions=[".jpg"], file_size=100
        )
        copy_to_path = os.path.join(self.temp_dir, "mtime_copy")

        do_copy_with_rules(
            ["*.jpg:mtime"],
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
        )

        # Verify files are organized by mtime
        for file_info in self.file_data:
            osrc = file_info[0]
            mtime = file_info[2]
            timestamp = datetime.datetime.fromtimestamp(mtime)
            year_month = f"{timestamp.year}_{timestamp.month:0>2}"
            expected_path = os.path.join(
                copy_to_path, year_month, os.path.basename(osrc)
            )
            self.assertTrue(
                os.path.exists(expected_path),
                f"File not found at expected mtime location: {expected_path}",
            )

    def test_extension_rule_only(self):
        """Test organizing files by extension"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir,
            file_spec=10,
            extensions=[".jpg", ".png", ".mp3"],
            file_size=100,
        )
        copy_to_path = os.path.join(self.temp_dir, "ext_copy")

        do_copy_with_rules(
            ["*:extension"],
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
        )

        # Verify files are organized by extension
        jpg_count = 0
        png_count = 0
        mp3_count = 0
        for file_info in self.file_data:
            osrc = file_info[0]
            ext = os.path.splitext(osrc)[1][1:]  # Remove leading dot
            expected_path = os.path.join(copy_to_path, ext, os.path.basename(osrc))
            self.assertTrue(
                os.path.exists(expected_path),
                f"File not found at expected extension location: {expected_path}",
            )
            if ext == "jpg":
                jpg_count += 1
            elif ext == "png":
                png_count += 1
            elif ext == "mp3":
                mp3_count += 1

        listing = list(utils.walk_tree(copy_to_path, include_dirs=True))

        # Verify directories exist for each extension
        self.assertTrue(
            jpg_count > 0 or os.path.exists(os.path.join(copy_to_path, "jpg")),
            f"failed to find jpg dir: {listing}",
        )
        self.assertTrue(
            png_count > 0 or os.path.exists(os.path.join(copy_to_path, "png")),
            f"failed to find png dir: {listing}",
        )
        self.assertTrue(
            mp3_count > 0 or os.path.exists(os.path.join(copy_to_path, "mp3")),
            f"failed to find mp3 dir: {listing}",
        )

    def test_combined_rules_extension_then_mtime(self):
        """Test combining extension and mtime rules"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=5, extensions=[".jpg"], file_size=100
        )
        copy_to_path = os.path.join(self.temp_dir, "combined_copy")

        do_copy_with_rules(
            ["*.jpg:extension", "*.jpg:mtime"],
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
        )

        # Verify structure is extension/mtime/filename
        for file_info in self.file_data:
            osrc = file_info[0]
            mtime = file_info[2]
            timestamp = datetime.datetime.fromtimestamp(mtime)
            year_month = f"{timestamp.year}_{timestamp.month:0>2}"
            expected_path = os.path.join(
                copy_to_path, "jpg", year_month, os.path.basename(osrc)
            )
            self.assertTrue(
                os.path.exists(expected_path),
                f"File not found at expected combined path: {expected_path}",
            )

    def test_extension_specific_rules(self):
        """Test different rules for different extensions"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=10, extensions=[".jpg", ".pdf"], file_size=100
        )
        copy_to_path = os.path.join(self.temp_dir, "specific_copy")

        do_copy_with_rules(
            ["*.jpg:mtime", "*.pdf:extension"],
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
        )

        # Verify jpg files use mtime, pdf files use extension
        for file_info in self.file_data:
            osrc = file_info[0]
            ext = os.path.splitext(osrc)[1]
            if ext == ".jpg":
                mtime = file_info[2]
                timestamp = datetime.datetime.fromtimestamp(mtime)
                year_month = f"{timestamp.year}_{timestamp.month:0>2}"
                expected_path = os.path.join(
                    copy_to_path, year_month, os.path.basename(osrc)
                )
            elif ext == ".pdf":
                expected_path = os.path.join(
                    copy_to_path, "pdf", os.path.basename(osrc)
                )
            else:
                continue

            self.assertTrue(
                os.path.exists(expected_path),
                f"File not found at expected path: {expected_path}",
            )


class TestExtensionFiltering(unittest.TestCase):
    """Test extension filtering functionality"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("ext_filter")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_single_extension_filter(self):
        """Test copying only files with specific extension"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir,
            file_spec=15,
            extensions=[".jpg", ".png", ".mp3"],
            file_size=100,
        )
        copy_to_path = os.path.join(self.temp_dir, "jpg_only")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            extensions=["jpg"],
            path_rules=["*:no_change"],
        )

        # Count copied files
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))

        # Verify only jpg files were copied
        for copied_file in copied_files:
            self.assertTrue(
                copied_file.endswith(".jpg"), f"Non-jpg file was copied: {copied_file}"
            )

        # Verify some jpg files exist
        self.assertGreater(len(copied_files), 0, "No files were copied")

    def test_multiple_extension_filters(self):
        """Test copying multiple specific extensions"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir,
            file_spec=20,
            extensions=[".jpg", ".png", ".mp3", ".txt"],
            file_size=100,
        )
        copy_to_path = os.path.join(self.temp_dir, "images_only")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            extensions=["jpg", "png"],
            path_rules=["*:no_change"],
        )

        # Verify only jpg and png files were copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        for copied_file in copied_files:
            self.assertTrue(
                copied_file.endswith(".jpg") or copied_file.endswith(".png"),
                f"Unexpected file extension copied: {copied_file}",
            )

        self.assertGreater(len(copied_files), 0, "No files were copied")

    def test_wildcard_extension_filter(self):
        """Test wildcard patterns in extension filtering"""
        # Create files with similar extensions
        test_files = []
        for i, ext in enumerate([".jpg", ".jpeg", ".jpng"]):
            fn = os.path.join(self.temp_dir, f"file{i}{ext}")
            check, mtime = utils.write_file(fn, i, size=100, initial=str(i))
            test_files.append([fn, check, mtime])

        copy_to_path = os.path.join(self.temp_dir, "wildcard_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            extensions=["*.jp*"],
            path_rules=["*:no_change"],
        )

        # Should match .jpg, .jpeg, .jpng
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), 3, f"Expected 3 files, got {len(copied_files)}"
        )


class TestDuplicateDetection(unittest.TestCase):
    """Test duplicate file detection and handling"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("duplicates")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_duplicate_content_files(self):
        """Test that files with identical content are detected as duplicates"""
        # Create two files with identical content
        file1 = os.path.join(self.temp_dir, "subdir1", "file1.txt")
        file2 = os.path.join(self.temp_dir, "subdir2", "file2.txt")

        # Use same seed and initial data for identical content
        check1, _ = utils.write_file(file1, seed=5, size=1000, initial="identical")
        check2, _ = utils.write_file(file2, seed=5, size=1000, initial="identical")

        # Verify they have the same checksum
        self.assertEqual(check1, check2, "Files should have identical checksums")

        copy_to_path = os.path.join(self.temp_dir, "no_dupes_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        # Should only copy one file
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files),
            1,
            f"Expected 1 file (duplicates removed), got {len(copied_files)}",
        )

    def test_multiple_duplicates(self):
        """Test handling multiple copies of the same file"""
        # Create 5 files with identical content
        files = []
        for i in range(5):
            fn = os.path.join(self.temp_dir, f"subdir{i}", f"file{i}.txt")
            check, mtime = utils.write_file(fn, seed=10, size=500, initial="same")
            files.append([fn, check, mtime])

        # Verify all have same checksum
        checksums = [f[1] for f in files]
        self.assertEqual(len(set(checksums)), 1, "All files should have same checksum")

        copy_to_path = os.path.join(self.temp_dir, "multi_dupe_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        # Should only copy one file
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files),
            1,
            f"Expected 1 file (all duplicates removed), got {len(copied_files)}",
        )

    def test_unique_files_all_copied(self):
        """Test that unique files are all copied"""
        # Create files with unique content
        files = []
        for i in range(10):
            fn = os.path.join(self.temp_dir, f"file{i}.txt")
            check, mtime = utils.write_file(fn, seed=i, size=500, initial=f"unique{i}")
            files.append([fn, check, mtime])

        copy_to_path = os.path.join(self.temp_dir, "unique_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        # All files should be copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), 10, f"Expected 10 unique files, got {len(copied_files)}"
        )


class TestIgnorePatterns(unittest.TestCase):
    """Test file ignore pattern functionality"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("ignore_test")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_simple_ignore_pattern(self):
        """Test ignoring files with specific pattern"""
        # Create files with various names and UNIQUE content
        test_files = []
        for i, name in enumerate(["keep1.txt", "temp.tmp", "keep2.txt", "temp2.tmp"]):
            fn = os.path.join(self.temp_dir, name)
            check, mtime = utils.write_file(fn, seed=i, size=100, initial=f"file{i}")
            test_files.append([fn, check, mtime])

        copy_to_path = os.path.join(self.temp_dir, "ignore_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            ignored_patterns=["*.tmp"],
            path_rules=["*:no_change"],
        )

        # Only .txt files should be copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), 2, f"Expected 2 files, got {len(copied_files)}"
        )

        for copied_file in copied_files:
            self.assertTrue(
                copied_file.endswith(".txt"), f"Ignored file was copied: {copied_file}"
            )

    def test_multiple_ignore_patterns(self):
        """Test multiple ignore patterns"""
        # Create files with various names and UNIQUE content
        test_files = []
        for i, name in enumerate(["keep.txt", "temp.tmp", "cache.cache", "data.txt"]):
            fn = os.path.join(self.temp_dir, name)
            check, mtime = utils.write_file(fn, seed=i, size=100, initial=f"content{i}")
            test_files.append([fn, check, mtime])

        copy_to_path = os.path.join(self.temp_dir, "multi_ignore_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            ignored_patterns=["*.tmp", "*.cache"],
            path_rules=["*:no_change"],
        )

        # Only .txt files should be copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), 2, f"Expected 2 .txt files, got {len(copied_files)}"
        )


class TestMultipleSourcePaths(unittest.TestCase):
    """Test copying from multiple source paths"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("multi_source")
        self.source1 = os.path.join(self.temp_dir, "source1")
        self.source2 = os.path.join(self.temp_dir, "source2")
        os.makedirs(self.source1)
        os.makedirs(self.source2)

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_multiple_sources_unique_files(self):
        """Test copying unique files from multiple sources"""
        # Create unique files in each source with unique content
        files1 = []
        for i in range(5):
            fn = utils.get_random_file_name(root=self.source1, extensions=[".jpg"])
            check, mtime = utils.write_file(
                fn, seed=i, size=100, initial=f"source1_file{i}"
            )
            files1.append([fn, check, mtime])

        files2 = []
        for i in range(5):
            fn = utils.get_random_file_name(root=self.source2, extensions=[".png"])
            check, mtime = utils.write_file(
                fn, seed=i + 100, size=100, initial=f"source2_file{i}"
            )
            files2.append([fn, check, mtime])

        copy_to_path = os.path.join(self.temp_dir, "multi_copy")

        do_copy_with_rules(
            ["*:no_change"],
            read_from_path=[self.source1, self.source2],
            copy_to_path=copy_to_path,
        )

        # All 10 unique files should be copied
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files),
            10,
            f"Expected 10 files from both sources, got {len(copied_files)}",
        )

    def test_multiple_sources_with_duplicates(self):
        """Test that duplicates across sources are handled"""
        # Create a file in source1
        file1 = os.path.join(self.source1, "duplicate.txt")
        check1, _ = utils.write_file(file1, seed=5, size=500, initial="same")

        # Create identical file in source2
        file2 = os.path.join(self.source2, "duplicate.txt")
        check2, _ = utils.write_file(file2, seed=5, size=500, initial="same")

        self.assertEqual(check1, check2, "Files should have same checksum")

        # Add UNIQUE files to each source
        for i in range(3):
            fn = utils.get_random_file_name(root=self.source1, extensions=[".jpg"])
            utils.write_file(fn, seed=i + 10, size=100, initial=f"s1_unique{i}")

        for i in range(3):
            fn = utils.get_random_file_name(root=self.source2, extensions=[".png"])
            utils.write_file(fn, seed=i + 200, size=100, initial=f"s2_unique{i}")

        copy_to_path = os.path.join(self.temp_dir, "dupe_across_sources")

        do_copy_with_rules(
            ["*:no_change"],
            read_from_path=[self.source1, self.source2],
            copy_to_path=copy_to_path,
        )

        # Should have 7 files: 1 duplicate (copied once) + 6 unique
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files),
            7,
            f"Expected 7 files (1 dupe + 6 unique), got {len(copied_files)}",
        )


class TestManifestIntegration(unittest.TestCase):
    """Test manifest save/load functionality during copy"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("manifest_copy")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_save_manifest_during_copy(self):
        """Test that manifest is saved during copy operation"""
        # pylint: disable=attribute-defined-outside-init
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=10, file_size=100
        )
        copy_to_path = os.path.join(self.temp_dir, "copy_with_manifest")
        manifest_path = os.path.join(self.temp_dir, "test_manifest.db")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            manifest_out_path=manifest_path,
        )

        # Verify manifest files were created
        self.assertTrue(
            os.path.exists(manifest_path), f"Manifest file not created: {manifest_path}"
        )
        self.assertTrue(
            os.path.exists(f"{manifest_path}.read"),
            f"Manifest read file not created: {manifest_path}.read",
        )

    def test_skip_already_processed_files(self):
        """Test that files in loaded manifest are skipped"""
        # Create a source directory separate from temp_dir to avoid scanning manifest files
        source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(source_dir)

        # First pass: create files and manifest
        files1 = []
        for i in range(5):
            fn = utils.get_random_file_name(root=source_dir, extensions=[".jpg"])
            check, mtime = utils.write_file(fn, seed=i, size=100, initial=f"first_{i}")
            files1.append([fn, check, mtime])

        copy_to_path = os.path.join(self.temp_dir, "first_copy")
        manifest_path = os.path.join(self.temp_dir, "skip_manifest.db")

        do_copy_with_rules(
            ["*:no_change"],
            read_from_path=source_dir,
            copy_to_path=copy_to_path,
            manifests_in_paths=manifest_path,
            manifest_out_path=manifest_path,
        )

        first_count = len(list(utils.walk_tree(copy_to_path, include_dirs=False)))

        assert first_count == 5, f"Expected 5 files in first copy, got {first_count}"

        # Add more UNIQUE files
        files2 = []
        for i in range(5):
            fn = utils.get_random_file_name(root=source_dir, extensions=[".png"])
            check, mtime = utils.write_file(
                fn, seed=i + 100, size=100, initial=f"second_{i}"
            )
            files2.append([fn, check, mtime])

        # Second pass: use existing manifest
        do_copy_with_rules(
            ["*:no_change"],
            read_from_path=source_dir,
            copy_to_path=copy_to_path,
            manifests_in_paths=manifest_path,
            manifest_out_path=manifest_path,
        )

        # Should now have 10 files total
        final_count = len(list(utils.walk_tree(copy_to_path, include_dirs=False)))
        self.assertEqual(
            final_count, 10, f"Expected 10 files (5 old + 5 new), got {final_count}"
        )


class TestMetadataPreservation(unittest.TestCase):
    """Test file metadata preservation"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("metadata_test")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_preserve_stat_flag(self):
        """Test that preserve_stat flag preserves modification times"""
        # Create a file with specific mtime
        fn = os.path.join(self.temp_dir, "timed_file.txt")
        _, mtime = utils.write_file(fn, seed=0, size=100)

        # Wait a moment to ensure time difference
        time.sleep(0.1)

        copy_to_path = os.path.join(self.temp_dir, "preserved_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            preserve_stat=True,
        )

        # Check copied file mtime
        copied_file = os.path.join(copy_to_path, "timed_file.txt")
        copied_mtime = os.path.getmtime(copied_file)

        # Should be very close (within 1 second for file system precision)
        time_diff = abs(copied_mtime - mtime)
        self.assertLess(
            time_diff, 1.0, f"Modification time not preserved: diff={time_diff}s"
        )

    def test_no_preserve_stat_flag(self):
        """Test that without preserve_stat, mtime is current time"""
        # Create a file with old mtime
        fn = os.path.join(self.temp_dir, "old_file.txt")
        check, old_mtime = utils.write_file(fn, seed=0, size=100)

        assert old_mtime is not None, "Old mtime should not be None"
        assert check is not None, "Checksum should not be None"

        # Set mtime to 1 day ago
        one_day_ago = time.time() - 86400
        os.utime(fn, (one_day_ago, one_day_ago))

        copy_to_path = os.path.join(self.temp_dir, "new_mtime_copy")

        # Record current time
        before_copy = time.time()

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            preserve_stat=False,  # Default, but explicit
        )

        after_copy = time.time()

        # Check copied file mtime
        copied_file = os.path.join(copy_to_path, "old_file.txt")
        copied_mtime = os.path.getmtime(copied_file)

        # New mtime should be recent (between before and after copy)
        self.assertGreater(
            copied_mtime, before_copy - 1, "Copied file mtime should be recent"
        )
        self.assertLess(
            copied_mtime, after_copy + 1, "Copied file mtime should be recent"
        )


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and special scenarios"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("edge_cases")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_files_without_extension(self):
        """Test handling files with no extension"""
        # Create files without extensions
        for i in range(3):
            fn = os.path.join(self.temp_dir, f"noext{i}")
            utils.write_file(fn, seed=i, size=100)

        copy_to_path = os.path.join(self.temp_dir, "noext_copy")

        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:extension"],
        )

        # Should be in a "no_extension" folder (from CopyThread logic)
        copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(
            len(copied_files), 3, f"Expected 3 files, got {len(copied_files)}"
        )

    def test_empty_source_directory(self):
        """Test copying from empty directory"""
        empty_source = os.path.join(self.temp_dir, "empty")
        os.makedirs(empty_source)
        copy_to_path = os.path.join(self.temp_dir, "empty_copy")

        # Should not raise an error
        do_copy(
            read_from_path=empty_source,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )

        # Copy destination might not even exist or be empty
        if os.path.exists(copy_to_path):
            copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
            self.assertEqual(
                len(copied_files), 0, "No files should be copied from empty dir"
            )


if __name__ == "__main__":
    unittest.main()
