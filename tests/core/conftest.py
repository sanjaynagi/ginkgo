"""Fixtures for core tests that load workflow modules from disk."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


@pytest.fixture
def restore_imports(tmp_path: Path):
    """Undo what loading a workflow from ``tmp_path`` does to the interpreter.

    Loading puts the workflow's import roots on ``sys.path`` and registers its
    modules, so a later test that scaffolds a same-named package would get the
    stale one. Only modules loaded from ``tmp_path`` are dropped: ginkgo's own
    lazily imported modules must keep their identity.
    """
    saved_path = list(sys.path)
    yield
    sys.path[:] = saved_path
    root = str(tmp_path.resolve())
    for name, module in list(sys.modules.items()):
        location = getattr(module, "__file__", None) or ""
        if location and str(Path(location).resolve()).startswith(root):
            del sys.modules[name]
