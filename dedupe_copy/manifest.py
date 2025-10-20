"""Manifest file handling for dedupe_copy.
Manifests store a mapping of hash -> list of files with that hash
They are the core data structure use to track duplicates
and what files have been read.
"""

import logging
import os
import random
import threading
from typing import Any, List, Optional, Set, Tuple, Union

from .disk_cache_dict import CacheDict, DefaultCacheDict

logger = logging.getLogger(__name__)


class Manifest:
    """Manages the storage and retrieval of file hash and metadata.

    This class provides a dictionary-like interface for accessing file data,
    while also tracking which files have been read in a separate structure.
    It supports loading from and saving to disk, as well as combining
    multiple manifests.

    Attributes:
        path: The primary path for the manifest file.
        md5_data: A dictionary-like object mapping hashes to file metadata.
        read_sources: A set-like object tracking files that have been read.
        save_event: An optional event to signal save operations.
    """

    cache_size = 10000

    def __init__(
        self,
        manifest_paths: Optional[Union[str, List[str]]],
        save_path: Optional[str] = None,
        temp_directory: Optional[str] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        """Initializes the Manifest.

        Args:
            manifest_paths: A path or list of paths to load manifests from.
            save_path: The default path for saving the manifest.
            temp_directory: A directory for temporary files. Required if
                            `save_path` is not provided.
            save_event: An optional event to coordinate save operations.
        """
        self.temp_directory = temp_directory
        if save_path:
            self.path = save_path
        else:
            assert (
                temp_directory is not None
            ), "temp_directory must be provided if save_path is not"
            self.path = os.path.join(
                temp_directory, f"temporary_{random.getrandbits(16)}.dict"
            )
        self.md5_data: Any = {}
        self.read_sources: Any = {}
        self.save_event = save_event
        if manifest_paths:
            if not isinstance(manifest_paths, list):
                self.load(manifest_paths)
            else:
                self._load_manifest_list(manifest_paths)
        else:
            sources_path = f"{self.path}.read"
            # no data yet
            if os.path.exists(self.path):
                logger.info("Removing old manifest file at: %r", self.path)
                os.unlink(self.path)
            if os.path.exists(sources_path):
                logger.info("Removing old manifest sources file at: %r", sources_path)
                os.unlink(sources_path)
            logger.info("creating manifests %s / %s", self.path, sources_path)
            self.md5_data = DefaultCacheDict(
                list, db_file=self.path, max_size=self.cache_size
            )
            self.read_sources = CacheDict(
                db_file=sources_path, max_size=self.cache_size
            )

    def __contains__(self, key: str) -> bool:
        """Check if key exists in manifest."""
        return key in self.md5_data

    def __getitem__(self, key: str) -> Any:
        """Get value for key from manifest."""
        return self.md5_data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set value for key in manifest."""
        self.md5_data[key] = value

    def __delitem__(self, key: str) -> None:
        """Delete key from manifest."""
        del self.md5_data[key]

    def __len__(self) -> int:
        """Return number of hash keys in manifest."""
        return len(self.md5_data)

    def save(
        self,
        path: Optional[str] = None,
        no_walk: bool = False,
        rebuild_sources: bool = True,
    ) -> None:
        """Saves the manifest to disk.

        This method writes the main hash data and the set of read sources to
        their respective files.

        Args:
            path: The path to save the manifest to. If None, the default
                  path is used.
            no_walk: If True, the read sources are not repopulated from the
                     main data before saving.
            rebuild_sources: If True, the read sources are repopulated from
                             the main data.
        """
        if self.save_event:
            self.save_event.set()
        path = path or self.path
        try:
            # Save the main data first, then populate and save sources.
            # This ensures that if a read happens during save, the main
            # data is consistent.
            logger.info("Writing manifest of %d hashes to %s", len(self.md5_data), path)
            self.md5_data.save(db_file=path)

            if rebuild_sources and not no_walk:
                self._populate_read_sources()

            logger.info(
                "Writing sources of %d files to %s.read", len(self.read_sources), path
            )
            self.read_sources.save(db_file=f"{path}.read")
        finally:
            if self.save_event:
                self.save_event.clear()

    def close(self) -> None:
        """Closes the underlying database files for the manifest."""
        if hasattr(self, "md5_data") and hasattr(self.md5_data, "close"):
            self.md5_data.close()
        if hasattr(self, "read_sources") and hasattr(self.read_sources, "close"):
            self.read_sources.close()

    def load(self, path: Optional[str] = None) -> None:
        """Loads a manifest from disk.

        This method closes any existing manifest data and loads new data from
        the specified path.

        Args:
            path: The path to the manifest file to load. If None, the default
                  path is used.
        """
        path = path or self.path
        self.close()
        self.md5_data, self.read_sources = self._load_manifest(path=path)

    def items(self) -> Any:
        """Return items view of manifest data."""
        return self.md5_data.items()

    def iteritems(self) -> Any:
        """Deprecated: Use items() instead"""
        return self.md5_data.items()

    def remove_files(self, files_to_remove: List[str]) -> None:
        """Removes a list of specified files from the manifest.

        This method updates both the main hash data and the read sources to
        remove all traces of the specified files.

        Args:
            files_to_remove: A list of file paths to be removed.
        """
        # Create a reverse lookup from path to hash
        files_to_remove_set = set(files_to_remove)
        hashes_to_modify = {}

        # First, find which hashes are affected without creating a full reverse map
        for hash_val, file_list in self.md5_data.items():
            # Check if any file associated with this hash needs to be removed
            if any(file_info[0] in files_to_remove_set for file_info in file_list):
                hashes_to_modify[hash_val] = file_list

        # Now, process only the affected hashes
        for hash_val, file_list in hashes_to_modify.items():
            new_file_list = [
                file_info
                for file_info in file_list
                if file_info[0] not in files_to_remove_set
            ]

            if new_file_list:
                self.md5_data[hash_val] = new_file_list
            else:
                # If no files are left for this hash, remove the hash key entirely
                del self.md5_data[hash_val]

        # Update read_sources separately for efficiency
        for path in files_to_remove_set:
            if path in self.read_sources:
                del self.read_sources[path]

    def update_paths(self, moved_files: List[Tuple[str, str]]) -> None:
        """Updates file paths in the manifest after a move operation.

        Args:
            moved_files: A list of tuples, where each tuple contains the
                         (source_path, destination_path) of a moved file.
        """
        if not moved_files:
            return

        # Create a mapping of old paths to new paths for quick lookup
        path_map = dict(moved_files)
        hashes_to_modify = {}

        # Find which hashes are affected
        for hash_val, file_list in self.md5_data.items():
            if any(file_info[0] in path_map for file_info in file_list):
                hashes_to_modify[hash_val] = file_list

        # Update the paths for the affected hashes
        for hash_val, file_list in hashes_to_modify.items():
            new_file_list = []
            for file_info in file_list:
                old_path = file_info[0]
                if old_path in path_map:
                    new_path = path_map[old_path]
                    new_file_list.append([new_path] + list(file_info[1:]))
                    # Update read_sources as well
                    if old_path in self.read_sources:
                        del self.read_sources[old_path]
                    self.read_sources[new_path] = None
                else:
                    new_file_list.append(file_info)
            self.md5_data[hash_val] = new_file_list

    def _populate_read_sources(self) -> None:
        """Populate the read_sources list from the md5_data."""
        # Clear existing sources to prevent duplication when re-populating
        self.read_sources.clear()
        dict_iter = self.md5_data.items()
        for _, info in dict_iter:
            for file_data in info:
                src = file_data[0]
                if src not in self.read_sources:
                    self.read_sources[src] = None

    def hash_set(self) -> Set[str]:
        """Returns a set of all hash keys present in the manifest.

        Returns:
            A set containing all the unique hash keys.
        """
        return set(self.md5_data.keys())

    def _load_manifest(self, path: Optional[str] = None) -> Tuple[Any, Any]:
        path = path or self.path
        logger.info("Reading manifest from %r...", path)
        # Would be nice to just get the fd, but backends require a path
        md5_data = DefaultCacheDict(list, db_file=path, max_size=self.cache_size)
        md5_data.load()
        logger.info("... read %d hashes", len(md5_data))
        read_sources = CacheDict(db_file=f"{path}.read", max_size=self.cache_size)
        read_sources.load()
        logger.info("... in %d files", len(read_sources))
        return md5_data, read_sources

    @staticmethod
    def _combine_manifests(
        manifests: List[Tuple[Any, Any]], temp_directory: Optional[str]
    ) -> Tuple[Any, Any]:
        """Combine multiple manifest data structures into one.
        Args:
            manifests: List of (md5_data, read_sources) tuples to combine
            temp_directory: The directory to use for temporary files.
        Returns:
            Tuple of (combined_md5_data, combined_read_sources)
        """
        assert temp_directory is not None
        combined_md5_path = os.path.join(
            temp_directory, f"combined_md5_{random.getrandbits(16)}.dict"
        )
        combined_read_path = f"{combined_md5_path}.read"
        combined_md5 = DefaultCacheDict(list, db_file=combined_md5_path)
        combined_read = CacheDict(db_file=combined_read_path)

        for m, r in manifests:
            for key, files in m.items():
                current_files = combined_md5[key]
                # Use a set for faster lookups
                existing_files = {tuple(f) for f in current_files}
                new_files_added = False
                for info in files:
                    info_tuple = tuple(info)
                    if info_tuple not in existing_files:
                        current_files.append(info)
                        existing_files.add(info_tuple)
                        new_files_added = True

                if new_files_added:
                    combined_md5[key] = current_files
            for key in r:
                combined_read[key] = None
        return combined_md5, combined_read

    def _load_manifest_list(self, manifests: List[str]) -> None:
        if not isinstance(manifests, list):
            raise TypeError("manifests must be a list")

        # If there's only one manifest, load it directly without combining.
        if len(manifests) == 1:
            self.load(manifests[0])
            return

        # Close existing manifests before replacing them
        self.close()

        # This will become the new manifest data. Start with empty.
        self.md5_data, self.read_sources = self._combine_manifests(
            [], self.temp_directory
        )

        # Iteratively load and combine manifests, closing each one after use.
        for src in manifests:
            m, r = self._load_manifest(src)
            try:
                for key, files in m.items():
                    current_files = self.md5_data[key]
                    # Use a set for faster lookups
                    existing_files = {tuple(f) for f in current_files}
                    new_files_added = False
                    for info in files:
                        info_tuple = tuple(info)
                        if info_tuple not in existing_files:
                            current_files.append(info)
                            existing_files.add(info_tuple)
                            new_files_added = True

                    if new_files_added:
                        self.md5_data[key] = current_files

                for key in r:
                    self.read_sources[key] = None
            finally:
                # Ensure the temporary manifest is closed to release file handles
                if hasattr(m, "close"):
                    m.close()
                if hasattr(r, "close"):
                    r.close()

    def convert_manifest_paths(
        self, paths_from: str, paths_to: str, temp_directory: Optional[str] = None
    ) -> None:
        """Converts file paths in the manifest by replacing a prefix.

        This method is useful for adapting a manifest to a new directory
        structure, for example, when moving files to a different location.

        Args:
            paths_from: The prefix to be replaced in the file paths.
            paths_to: The new prefix to substitute.
            temp_directory: A directory for temporary files during the
                            conversion.
        """
        temp_directory = temp_directory or self.temp_directory
        for key, val in self.md5_data.items():
            new_values = []
            for file_data in val:
                new_values.append(
                    [file_data[0].replace(paths_from, paths_to, 1)]
                    + list(file_data[1:])
                )
            self.md5_data[key] = new_values
        # build a new set of values and move into place
        # Note: Using CacheDict for read_sources (could optimize with a persistent
        # set implementation)
        db_file = self.read_sources.db_file_path()
        assert temp_directory is not None, "temp_directory must be provided"
        new_sources = CacheDict(
            db_file=os.path.join(temp_directory, "temp_convert.dict"),
            max_size=self.cache_size,
        )
        for key in self.read_sources:
            new_sources[key.replace(paths_from, paths_to, 1)] = None
        del self.read_sources
        new_sources.save(db_file=db_file)
        self.read_sources = new_sources
        self.md5_data.save()
        self.read_sources.save()
