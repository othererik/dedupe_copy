"""End-to-end tests for complex user scenarios."""
import os
import unittest
import shutil
import platform
import hashlib
from dedupe_copy.core import run_dupe_copy
from dedupe_copy.test.utils import make_temp_dir, get_hash, walk_tree


def create_complex_file_tree(root_path: str, file_defs: dict) -> dict:
    """
    Creates a complex file and directory structure for testing from a dictionary.
    Adds hardlinks if not on Windows.
    Returns a dictionary of paths to expected content.
    """
    paths = {}
    os.makedirs(root_path, exist_ok=True)

    # Create files and directories
    for path, content in file_defs.items():
        full_path = os.path.join(root_path, path)
        if content is None:  # Represents a directory
            os.makedirs(full_path, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(content)
            paths[full_path] = content

    # Create a hardlink, not on Windows
    if platform.system() != "Windows":
        link_source_path = os.path.join(root_path, "unique_file.txt")
        if os.path.exists(link_source_path):
            link_target_path = os.path.join(root_path, "nested/hardlink.txt")
            os.makedirs(os.path.dirname(link_target_path), exist_ok=True)
            os.link(link_source_path, link_target_path)
            paths[link_target_path] = file_defs["unique_file.txt"]

    return paths


class TestEndToEndScenario(unittest.TestCase):
    """A single, large, end-to-end test to cover complex user scenarios."""

    def setUp(self):
        self.test_dir = make_temp_dir("e2e_test")
        self.base_source_dir = os.path.join(self.test_dir, "base_source")
        self.op_source_dir = os.path.join(self.test_dir, "operation_source")
        self.target_dir = os.path.join(self.test_dir, "target")
        self.manifest_dir = os.path.join(self.test_dir, "manifests")
        os.makedirs(self.base_source_dir, exist_ok=True)
        os.makedirs(self.op_source_dir, exist_ok=True)
        os.makedirs(self.target_dir, exist_ok=True)
        os.makedirs(self.manifest_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_complex_copy_and_delete_scenario(self):
        """
        Tests a complex multi-stage scenario involving comparison, copying, and deletion.
        """
        # === Stage 1: Create base source and its manifest ===
        base_source_files = {
            "unique_file.txt": "This is a unique file in the base source.",
            "duplicate_target.txt": "This content will be duplicated.",
            "empty.txt": "",
            "nested/another_file.txt": "Some content.",
        }
        create_complex_file_tree(self.base_source_dir, base_source_files)
        base_manifest_path = os.path.join(self.manifest_dir, "base.db")
        initial_base_files = set(walk_tree(self.base_source_dir))

        run_dupe_copy(
            read_from_path=[self.base_source_dir], manifest_out_path=base_manifest_path
        )
        self.assertTrue(os.path.exists(base_manifest_path))

        # === Stage 2: Create the operational source directory ===
        op_source_files = {
            "op_unique.txt": "This is a unique file in the operational source.",
            "dupe_of_base.txt": "This content will be duplicated.",
            "internal_dupe_1.txt": "Internal duplicate content.",
            "subdir/internal_dupe_2.txt": "Internal duplicate content.",
            "another_empty.txt": "",
        }
        create_complex_file_tree(self.op_source_dir, op_source_files)
        expected_internal_dupe_hash = hashlib.md5(
            op_source_files["internal_dupe_1.txt"].encode("utf-8")
        ).hexdigest()

        # === Stage 3: Run the main copy/dedupe operation ===
        op_manifest_path = os.path.join(self.manifest_dir, "op.db")
        run_dupe_copy(
            read_from_path=[self.op_source_dir],
            copy_to_path=self.target_dir,
            compare_manifests=[base_manifest_path],
            manifest_out_path=op_manifest_path,
            delete_on_copy=True,
            dedupe_empty=True,
        )

        # === Stage 4: Verify the final state of all directories ===
        op_source_files_after = set(walk_tree(self.op_source_dir))
        target_files_after = set(walk_tree(self.target_dir))
        base_source_files_after = set(walk_tree(self.base_source_dir))

        # Base source should be untouched
        self.assertEqual(len(base_source_files_after), len(initial_base_files))

        # Operational source directory should be empty
        self.assertEqual(
            len(op_source_files_after),
            0,
            f"Operational source should be empty, but contains: {op_source_files_after}",
        )

        # Target directory should contain the correct files
        self.assertEqual(
            len(target_files_after),
            2,
            f"Target should have 2 files, but contains: {target_files_after}",
        )

        expected_unique_path = os.path.join(self.target_dir, "op_unique.txt")
        self.assertTrue(os.path.exists(expected_unique_path))
        with open(expected_unique_path, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), op_source_files["op_unique.txt"])

        found_internal_dupe = False
        for f in target_files_after:
            if get_hash(f) == expected_internal_dupe_hash:
                with open(f, "r", encoding="utf-8") as fh:
                    self.assertEqual(fh.read(), "Internal duplicate content.")
                found_internal_dupe = True
                break
        self.assertTrue(
            found_internal_dupe,
            "One of the internal duplicates should have been moved to target.",
        )
