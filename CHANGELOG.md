# Changelog

## [1.2.1] - 2026-01-15
- Performance improvements
- Thread safety fix for CacheDict usage
- Improved user feedback


## [1.2.0] - 2025-11-21
- Memory improvement
- Improved UI experience


## [1.1.10] - 2025-11-02
- More Manifest save performance tweaks


## [1.1.9] - 2025-10-31
- Manifest save performance tweaks


## [1.1.8] - 2025-10-22
- Cleanups, coverage, and fixing edge cases.


## [1.1.7] - 2025-10-21
- Bugfix where tool failed to correctly synchronize files from a source to a target directory under specific conditions (--no-walk, --delete-on-copy, and --compare).


## [1.1.6] - 2025-10-20
- Correct documentation for empty file handling. The default is to treat empty files as unique, and `--dedupe-empty` is used to treat them as duplicates.


## [1.1.5] - 2025-10-17
- Add option to `--delete-on-copy`
- Require output manifest when deleting objects, input remains unchanged
- Minor performance improvements
- Default to keeping empty files, can override with `--dedupe-empty`
- Minor bug fixes


## [1.1.4] - 2025-10-13
- Improve manifest save performance
- Add manifest viewer tool
- Add manifest validation option (confirm files exist and are the same size)


## [1.1.3] - 2025-10-11
- Bugfix: clear manifest sources before reuse with walk to avoid duplicate entries
- More DCD performance measurments and optional charts
- Improve test coverage
- Remove entries from manifest when using --delete


## [1.1.2] - 2025-10-10
- Bugfix: race condition on copy
- Bugfix: close manifests on combine to prevent resource leak
- Bugfix: prevent doubling of source files when using --no-walk
- Bugfix: better feedback when trying to use xxhash but it isn't installed
- Bugfix: fix flaky extension test


## [1.1.1] - 2025-10-09

### Documentation
- Added Architecture section to README explaining the multi-threaded pipeline design
- Documented thread stages (Walk, Read, Result Processing, Copy/Delete, Progress)
- Added data structure explanations (Manifest, Disk Cache Dictionary)

### Notes
This is a documentation and code quality release with no functional changes.


## [1.1.0] - 2025-10-09
- Add new optional hashing algorithm choice
- Add delete and dry-run options
- Other performance improvements
- Resolved an issue where using the `--no-walk` flag would prevent `--delete` and `-r` (report) operations from executing. The tool now correctly performs these actions on the loaded manifest without requiring a new filesystem scan.
- Various correctness fixes and additional tests


## [1.0.1] - 2025-10-07
- Drop support for anything earlier than 3.11
- Lots of internal cleanup, CI related stuff


## [1.0.0] - 2025-10-04

### Major Changes
- **Python 3 Migration**: Fully updated codebase for Python 3.8+ compatibility, including Python 3.13 support
- **Version Bump**: Updated from 0.4.1 to 1.0.0 to reflect stability and major modernization

### Added
- Modern `pyproject.toml` packaging configuration replacing old `setup.py`
- `.gitignore` file for better repository management  
- `MANIFEST.in` for proper package distribution
- Version information in `__init__.py` module

### Changed
#### Python 3 Compatibility
- Replaced `Queue` module with `queue` (Python 3 naming)
- Converted all `print` statements to `print()` function calls
- Updated dictionary iteration methods:
  - `dict.iteritems()` → `dict.items()`
  - `dict.iterkeys()` → `dict.keys()`
  - `dict.itervalues()` → `dict.values()`
  - `dict.viewkeys()` → `dict.keys()`
  - `dict.viewvalues()` → `dict.values()`
- Replaced `xrange()` with `range()`
- Removed Python 2 `unicode` string handling (`.decode('utf-8')`)
- Updated `cPickle` import to use `pickle` directly
- Removed Python 2 long integer literals (`0L`)
- Fixed `collections.deque` initialization for Python 3 (`maxlen` parameter)

#### Modernizations
- Updated `collections.MutableMapping` to `collections.abc.MutableMapping`
- Fixed pickle serialization for Python 3 (bytes handling)
- Improved error handling: replaced bare `except Exception` with specific exception types
- Added f-string formatting in key areas for better performance
- Fixed `assert isinstance()` to use proper error handling
- Updated CLI shebang from `python` to `python3`

#### Package Structure
- Converted from distutils to modern setuptools with PEP 518 compliance
- Added proper classifiers for Python 3.8-3.13 in `pyproject.toml`
- Improved package metadata and URLs

### Fixed
- Fixed `collections.deque` initialization syntax errors
- Fixed `pickle.loads()` bytes handling for Python 3
- Fixed integer division to use `//` operator where needed
- Fixed `dict.keys()` subscripting issues in tests
- Improved thread-safe directory creation with proper `OSError` handling
- Fixed deprecated test assertions and methods

### Deprecated
- Python 2 support fully removed
- `has_key()` method marked as deprecated (use `in` operator instead)
- Added `iteritems()` compatibility method (use `items()` instead)

### Development
- Tests updated for Python 3 compatibility
- All lint errors resolved for Python 3.13
- Maintained backward-compatible API where possible

### Migration Notes
- **Breaking**: This version drops Python 2.x support entirely
- **Breaking**: Requires Python 3.8 or higher
- Most APIs remain compatible, but internal implementations updated
- Users should test thoroughly before upgrading from 0.x versions

### Technical Debt Resolved
- Removed all Python 2 vs 3 compatibility hacks
- Simplified import statements
- Modernized string formatting
- Improved code clarity and maintainability
- Much better test coverage
