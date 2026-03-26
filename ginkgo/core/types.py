"""Ginkgo type markers for task argument and return type annotations.

These are marker classes used as type annotations on task parameters and return
values. They drive validation (Phase 2) and caching (Phase 3). In Phase 1 they
carry no runtime behaviour.
"""

from __future__ import annotations


class file(str):
    """A path to a single file.

    Validated to exist on disk before task execution.  Return values declared
    as ``file`` are validated to exist after execution.  Cache key contribution
    is the BLAKE3 digest of file contents.
    """


class folder(str):
    """A path to a directory.

    Validated to exist and be a directory before execution.  Cache key
    contribution is the BLAKE3 digest of sorted recursive contents.
    """


class tmp_dir(str):
    """A ginkgo-managed scratch directory, unique per task execution.

    Created automatically before task execution and deleted on success.
    Kept on failure for debugging.  Does not participate in the cache key.
    """
