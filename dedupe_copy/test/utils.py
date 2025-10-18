"""Utilities to support tests"""

from collections import deque
import datetime
import hashlib
import os
import random
import shutil
import string
import tempfile
import time
from typing import Generator, List, Optional, Set, Tuple, Dict, Union
import uuid


RANDOM_DATA = "ThisdatamaybebutithasnoduplicatebigrAms."


def walk_tree(root_dir: str, include_dirs: bool = False) -> Generator[str, None, None]:
    """Walk a directory tree yielding file paths.

    :param root_dir: Root directory to start walking from
    :type root_dir: str
    :param include_dirs: Whether to include directories in results
    :type include_dirs: bool

    :yields: File paths (and directory paths if include_dirs=True)
    """
    for root, dirs, files in os.walk(root_dir):
        if include_dirs:
            for dir_name in dirs:
                yield os.path.join(root, dir_name)
        for name in files:
            yield os.path.join(root, name)


def make_temp_dir(description: str = "test_temp") -> str:
    """Return the path to a temporary directory"""
    abs_path = tempfile.mkdtemp(suffix=description)
    print(f"Made temporary directory: {abs_path}")
    return abs_path


def remove_dir(root: Optional[str]) -> None:
    """Remove a directory"""
    if root is None:
        return
    try:
        shutil.rmtree(root)
    except OSError as err:
        print(f"Failed to remove dir: {root}, will retry once ({err})")
        time.sleep(1)
        shutil.rmtree(root)
    print(f"Removed temporary directory: {root}")


def write_file(
    src: str, seed: int, size: int = 1000, initial: Optional[Union[str, bytes]] = None
) -> Tuple[str, float]:
    """Write a file that is reproduce-able given size, seed, and initial data

    :param src: Path to file that will be created / truncated and re-written
    :type src: str
    :param seed: integer up to len of RANDOM_DATA - 1
    :type seed: int
    :param size: file size in bytes
    :type size: int
    :param initial: string to write to the file first
    :type initial: str or bytes or None

    Returns a tuple of (checksum, mtime)
    """
    written = 0
    check = hashlib.md5()
    data_chunk_size = len(RANDOM_DATA)
    if seed >= data_chunk_size:
        print("Warning: data uniqueness not guaranteed")
    data_deque = deque(RANDOM_DATA)
    data_deque.rotate(seed)
    data = "".join(data_deque)
    dirs = os.path.dirname(src)
    if not os.path.exists(dirs):
        try:
            os.makedirs(dirs)
        except (OSError, FileExistsError):
            pass  # might be a threading race if making lots in threads
    with open(src, "wb") as fh:
        if initial:
            # Ensure initial is bytes
            if isinstance(initial, str):
                initial = initial.encode("utf-8")
            fh.write(initial[:size])
            written += len(initial)
            check.update(initial[:size])
        while written < size:
            if written + data_chunk_size <= size:
                chunk = data.encode("utf-8") if isinstance(data, str) else data
            else:
                chunk_str = data[: size - written]
                chunk = (
                    chunk_str.encode("utf-8")
                    if isinstance(chunk_str, str)
                    else chunk_str
                )
            fh.write(chunk)
            check.update(chunk)
            written += len(chunk)
    return check.hexdigest(), os.path.getmtime(src)


def get_random_file_name(
    root: str = "",
    prefix: Optional[str] = None,
    name_len: int = 10,
    extensions: Optional[List[str]] = None,
) -> str:
    """Return a random file name. If extensions is supplied, one will be chosen
    from the list. Will try to only return new names. If a root is suppled, a
    full path to the file will be formed.

    :param root: Root directory path
    :type root: str
    :param prefix: Optional prefix for the filename
    :type prefix: str or None
    :param name_len: Length of the random filename
    :type name_len: int
    :param extensions: List of possible extensions
    :type extensions: list or None

    :returns: A random file path
    """
    while True:
        name = []
        if prefix:
            name.append(prefix)
            name_len -= len(prefix)
        for _ in range(name_len):
            name.append(random.choice(string.ascii_letters))
        if extensions:
            name.extend(random.choice(extensions))
        name_str = "".join(name)
        full_path = os.path.join(root, name_str)
        if not os.path.exists(full_path):
            return full_path


def get_random_dir_path(
    root: str,
    max_depth: int = 4,
    existing_dirs: Optional[Set[str]] = None,
    new_dir_chance: float = 0.3,
    new_path_only: bool = False,
) -> str:
    """Generate a random directory path.

    :param root: Root directory path
    :type root: str
    :param max_depth: Maximum depth of subdirectories
    :type max_depth: int
    :param existing_dirs: Set of existing directory names to potentially reuse
    :type existing_dirs: set or None
    :param new_dir_chance: Probability of creating a new directory vs reusing existing
    :type new_dir_chance: float
    :param new_path_only: If True, only return paths that don't exist
    :type new_path_only: bool

    :returns: A random directory path
    """
    if existing_dirs is None:
        existing_dirs = set()
    while True:
        dir_count = random.randint(0, max_depth)
        dir_parts = [root]
        for _ in range(dir_count):
            if existing_dirs and random.random() <= new_dir_chance:
                dir_parts.append(random.choice(list(existing_dirs)))
            else:
                dir_parts.append(get_random_file_name())
            existing_dirs.add(dir_parts[-1])
        final_path = os.path.join(*dir_parts)
        if not new_path_only or not os.path.exists(final_path):
            return final_path


def make_file_tree(
    root: str,
    file_spec: Union[int, Dict[str, str]] = 10,
    extensions: Optional[List[str]] = None,
    file_size: int = 1000,
    prefix: Optional[str] = None,
    use_unique_files: bool = True,
) -> List[List[Union[str, float]]]:
    """Create a tree of files with various extensions off of root.
    If file_spec is an integer, it creates that many random files.
    If file_spec is a dictionary, it creates files with the given name and content.
    Returns a list of lists such as [[item, hash, mtime], [item, hash, mtime]]
    """
    file_list: List[List[Union[str, float]]] = []
    if isinstance(file_spec, dict):
        for filename, content in file_spec.items():
            path = os.path.join(root, filename)
            # Ensure the directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # write_file can handle the content
            check, mtime = write_file(path, 0, size=len(content), initial=content)
            file_list.append([path, check, mtime])
        return file_list

    # Original logic for when file_spec is an integer
    file_count = file_spec
    if not extensions:
        extensions = [".mov", ".mp3", ".png", ".jpg"]
    # this set is grown by the get_random_dir_path function
    existing_dirs: Set[str] = set()
    for i in range(file_count):
        # if given extensions, round-robin them
        ext = extensions[i % len(extensions)]
        fn = get_random_file_name(
            root=get_random_dir_path(root, existing_dirs=existing_dirs),
            extensions=[ext],
            prefix=prefix,
        )
        src = os.path.join(root, fn)
        initial_content = str(i) if use_unique_files else "same_content"
        check, mtime = write_file(src, 0, size=file_size, initial=initial_content)
        file_list.append([src, check, mtime])
    return file_list


def get_hash(src: str) -> str:
    """Calculate MD5 hash of a file.

    :param src: Path to the file
    :type src: str

    :returns: MD5 hexdigest string
    """
    check = hashlib.md5()
    with open(src, "rb") as fh:
        d = fh.read(64000)
        while d:
            check.update(d)
            d = fh.read(64000)
    return check.hexdigest()


def apply_path_rules(
    osrc: str, nsrc: str, _oroot: str, nroot: str, rules: List[str]
) -> str:
    """Apply path transformation rules to construct a new path.

    :param osrc: Original source path
    :type osrc: str
    :param nsrc: New source path
    :type nsrc: str
    :param nroot: New root path
    :type nroot: str
    :param rules: List of transformation rules to apply
    :type rules: list

    :returns: Transformed path string
    """
    new_path = [nroot]
    for rule in rules:
        if rule == "mtime":
            time_stamp = datetime.datetime.fromtimestamp(os.path.getmtime(osrc))
            yyyy_mm = f"{time_stamp.year}_{time_stamp.month:0>2}"
            new_path.append(yyyy_mm)
        if rule == "extension":
            new_path.append(os.path.splitext(osrc)[-1][1:])
    nsrc = nsrc.replace(nroot, "", 1)
    if nsrc.startswith(os.sep):
        nsrc = nsrc[1:]
    if rules and "no_change" not in rules:
        new_path.append(os.path.basename(osrc))
    else:
        new_path.append(nsrc)
    return os.sep.join(new_path)


def verify_files(file_list: List[List[Union[str, float]]]) -> Tuple[bool, str]:
    """Inspect a list of the form [[item, hash, mtime], [item, hash, mtime]]
    and return a tuple of (True|False, Message). True in the tuple indicates
    a match, if False, the message will contain the mismatches.
    """
    print(f"Verifying {len(file_list)} items")
    success = True
    message: List[str] = []
    for item in file_list:
        src = str(item[0])
        check = str(item[1])
        mtime = float(item[2])
        print(f" ... {src}")
        try:
            new_check = get_hash(src)
            new_mtime = os.path.getmtime(src)
            if check != new_check:
                success = False
                message.append(
                    f"Hash mismatch on {src}. Actual: {new_check} Expected: {check}"
                )
            if int(mtime) != int(new_mtime):
                success = False
                message.append(
                    f"Mtime mismatch on {src}. Actual: {new_mtime} Expected: {mtime}"
                )
        except (OSError, IOError) as err:
            msg = f"Failed to read {src}: {err}"
            message.append(msg)
            print(msg)
            success = False
    return success, "\n".join(message)


def gen_fake_manifest() -> (
    Tuple[Dict[str, List[List[Union[str, int, float]]]], Dict[str, None]]
):
    """Imitate the type of data in a manifest"""
    manifest: Dict[str, List[List[Union[str, int, float]]]] = {}
    file_a = f"/a/b/c/{random.getrandbits(16)}.file"
    file_b = f"/c/b/{random.getrandbits(16)}.file"
    sources: Dict[str, None] = {file_a: None, file_b: None}
    manifest[str(uuid.uuid1())] = [
        [file_a, random.randint(0, 10000), 1.0 * random.randint(0, 10000)]
    ]
    manifest[str(uuid.uuid1())] = [
        [file_b, random.randint(0, 10000), 1.0 * random.randint(0, 10000)]
    ]
    return manifest, sources
