"""Shared fixtures for ginkgo tests."""

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_working_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run each test in an isolated working directory.

    This keeps phase 3 cache entries scoped to a single test and avoids
    cross-test interference from ``.ginkgo/cache``.
    """
    monkeypatch.chdir(tmp_path)
