"""Pytest collection configuration for optional test dependencies."""

import importlib.util
from typing import List

collect_ignore: List[str] = []

if importlib.util.find_spec("hypothesis") is None:
    collect_ignore.append("test_hypothesis.py")
