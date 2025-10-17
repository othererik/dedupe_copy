import os
import unittest
import tempfile
import shutil
from dedupe_copy.core import run_dupe_copy
from dedupe_copy.test.utils import make_temp_dir, remove_dir, make_file_tree, write_file

class TestEmptyFiles(unittest.TestCase):
    """Tests for handling of empty files."""

    def setUp(self):
        """Set up a temporary directory for testing."""
        self.source_dir = make_temp_dir("source")
        self.dest_dir = make_temp_dir("dest")
        self.manifest_dir = make_temp_dir("manifest")

    def tearDown(self):
        """Clean up temporary directories."""
        remove_dir(self.source_dir)
        remove_dir(self.dest_dir)
        remove_dir(self.manifest_dir)

    def test_default_copies_all_empty_files(self):
        """Verify that by default, all empty files are copied."""
        make_file_tree(self.source_dir, file_count=5, file_size=100, use_unique_files=True)
        for i in range(3):
            write_file(os.path.join(self.source_dir, f"empty_{i}.txt"), seed=i, size=0)

        run_dupe_copy(
            read_from_path=self.source_dir,
            copy_to_path=self.dest_dir,
            path_rules=["*:no_change"]
        )

        source_files = set(os.listdir(self.source_dir))
        dest_files = set(os.listdir(self.dest_dir))
        self.assertEqual(source_files, dest_files)

    def test_dedupe_empty_copies_one_empty_file(self):
        """Verify that with --dedupe-empty, only one empty file is copied."""
        make_file_tree(self.source_dir, file_count=5, file_size=100, use_unique_files=True)
        for i in range(3):
            write_file(os.path.join(self.source_dir, f"empty_{i}.txt"), seed=i, size=0)

        run_dupe_copy(
            read_from_path=self.source_dir,
            copy_to_path=self.dest_dir,
            dedupe_empty=True,
            path_rules=["*:no_change"]
        )

        dest_files = os.listdir(self.dest_dir)
        empty_files_in_dest = [f for f in dest_files if f.startswith("empty_")]
        self.assertEqual(len(empty_files_in_dest), 1)
        self.assertEqual(len(dest_files), 5 + 1)

    def test_default_delete_keeps_empty_files(self):
        """Verify that by default, --delete does not remove empty files."""
        write_file(os.path.join(self.source_dir, "file1.txt"), seed=1, size=100)
        write_file(os.path.join(self.source_dir, "file2.txt"), seed=1, size=100) # dupe
        write_file(os.path.join(self.source_dir, "empty1.txt"), seed=2, size=0)
        write_file(os.path.join(self.source_dir, "empty2.txt"), seed=3, size=0) # empty dupe

        manifest_path = os.path.join(self.manifest_dir, "manifest.db")
        run_dupe_copy(read_from_path=self.source_dir, manifest_out_path=manifest_path)

        run_dupe_copy(
            manifests_in_paths=[manifest_path],
            manifest_out_path=f"{manifest_path}.new",
            delete_duplicates=True,
            no_walk=True
        )

        remaining_files = os.listdir(self.source_dir)
        self.assertIn("file1.txt", remaining_files)
        self.assertNotIn("file2.txt", remaining_files)
        self.assertIn("empty1.txt", remaining_files)
        self.assertIn("empty2.txt", remaining_files)

    def test_dedupe_empty_delete_removes_empty_files(self):
        """Verify that with --dedupe-empty, --delete removes duplicate empty files."""
        write_file(os.path.join(self.source_dir, "file1.txt"), seed=1, size=100)
        write_file(os.path.join(self.source_dir, "file2.txt"), seed=1, size=100) # dupe
        write_file(os.path.join(self.source_dir, "empty1.txt"), seed=2, size=0)
        write_file(os.path.join(self.source_dir, "empty2.txt"), seed=3, size=0) # empty dupe

        manifest_path = os.path.join(self.manifest_dir, "manifest.db")
        run_dupe_copy(read_from_path=self.source_dir, manifest_out_path=manifest_path, dedupe_empty=True)

        run_dupe_copy(
            manifests_in_paths=[manifest_path],
            manifest_out_path=f"{manifest_path}.new",
            delete_duplicates=True,
            no_walk=True,
            dedupe_empty=True,
        )

        remaining_files = os.listdir(self.source_dir)
        self.assertIn("file1.txt", remaining_files)
        self.assertNotIn("file2.txt", remaining_files)

        empty_files_remaining = [f for f in remaining_files if f.startswith("empty")]
        self.assertEqual(len(empty_files_remaining), 1)

if __name__ == '__main__':
    unittest.main()