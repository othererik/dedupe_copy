"""Interactive CLI for exploring manifest files."""

import cmd
import logging
import os
import tempfile
from typing import Optional

from dedupe_copy.manifest import Manifest

logger = logging.getLogger(__name__)


class ManifestExplorer(cmd.Cmd):
    """Interactive command-line tool for exploring manifest files."""

    intro = "Welcome to the Manifest Explorer. Type help or ? to list commands.\\n"
    prompt = "(manifest_explorer) "

    def __init__(self) -> None:
        super().__init__()
        self.manifest: Optional[Manifest] = None
        # Temporary directory lives across the interactive REPL session and is
        # cleaned up in do_exit(), so a context manager block cannot be used here.
        self.temp_dir = (
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )

    def do_load(self, arg: str) -> None:
        """Load a manifest file. Usage: load <path_to_manifest>"""
        if not arg:
            print("Please provide the path to the manifest file.")
            return
        if not os.path.exists(arg):
            print(f"Error: Manifest file not found at '{arg}'")
            return
        if self.manifest:
            self.manifest.close()
        try:
            self.manifest = Manifest(
                manifest_paths=arg, temp_directory=self.temp_dir.name
            )
            print(f"Manifest '{arg}' loaded successfully.")
        # Catch all exceptions in the interactive REPL command handler so a
        # corrupt or incompatible manifest file does not crash the user's shell.
        except Exception as e:  # pylint: disable=broad-except
            print(f"An error occurred while loading the manifest: {e}")
            self.manifest = None

    def do_info(self, _arg: str) -> None:
        """Display information about the loaded manifest."""
        if not self.manifest:
            print("No manifest loaded. Use 'load <path>' to load a manifest.")
            return
        print(f"  Hashes: {len(self.manifest.md5_data)}")
        print(f"  Files: {len(self.manifest.read_sources)}")

    def do_list(self, arg: str) -> None:
        """List contents of the manifest. Usage: list [limit]"""
        if not self.manifest:
            print("No manifest loaded. Use 'load <path>' to load a manifest.")
            return
        try:
            limit = int(arg) if arg else 10
        except ValueError:
            print("Invalid limit. Please provide an integer.")
            return

        count = 0
        for hash_val, files in self.manifest.items():
            if count >= limit:
                break
            print(f"Hash: {hash_val}")
            for file_info in files:
                print(f"  - {file_info[0]}")
            count += 1

    def do_find(self, arg: str) -> None:
        """Find a file path or hash in the manifest. Usage: find <query>"""
        if not self.manifest:
            print("No manifest loaded. Use 'load <path>' to load a manifest.")
            return
        if not arg:
            print("Please provide a search query.")
            return

        found = False
        # Search by hash
        if arg in self.manifest:
            print(f"Found hash: {arg}")
            for file_info in self.manifest[arg]:
                print(f"  - {file_info[0]}")
            found = True

        # Search by file path
        for hash_val, files in self.manifest.items():
            for file_info in files:
                if arg in file_info[0]:
                    print(f"Found file: {file_info[0]} (Hash: {hash_val})")
                    found = True

        if not found:
            print("No matching hash or file found.")

    def do_exit(self, _arg: str) -> bool:
        """Exit the manifest explorer."""
        if self.manifest:
            self.manifest.close()
        self.temp_dir.cleanup()
        return True


def main() -> None:
    """Main function to run the Manifest Explorer."""
    try:
        ManifestExplorer().cmdloop()
    except KeyboardInterrupt:
        print("\\nExiting.")


if __name__ == "__main__":
    main()
