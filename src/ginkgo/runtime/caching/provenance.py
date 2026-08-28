"""Run identity and log tails.

What used to be the provenance recorder now lives in :mod:`ginkgo.store`: the
ledger records what happened, and :class:`~ginkgo.runtime.rundir.RunDir` owns
the bytes on disk. What is left here is the naming of a run and the reading of
its logs. Phase 5 of the ledger work moves both into ``rundir`` and deletes
this module.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from pathlib import Path


def make_run_id(*, workflow_path: str | Path | None = None) -> str:
    """Return a timestamped run identifier."""
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    token_source = str(Path(workflow_path).resolve()) if workflow_path is not None else timestamp
    discriminator = secrets.token_hex(4)
    suffix = abs(hash((token_source, timestamp, discriminator))) % (16**8)
    return f"{timestamp}_{suffix:08x}"


def tail_text(path: Path, *, lines: int = 50) -> list[str]:
    """Return the last *lines* lines from a text file."""
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8").splitlines()
    return content[-lines:]


def combined_log_tail(
    *,
    run_dir: Path,
    stdout_log: object,
    stderr_log: object,
    lines: int,
) -> list[str]:
    """Combine stdout and stderr tails for failure display.

    Each log argument is the relative path stored on a task record; it
    may be a string path, ``None``, or any other value depending on the
    caller's task representation. Non-string values are ignored, so
    callers can pass either mapping ``.get(...)`` results or dataclass
    attributes without an extra ``isinstance`` check.
    """
    combined: list[str] = []
    if isinstance(stdout_log, str):
        combined.extend(tail_text(run_dir / stdout_log, lines=lines))
    if isinstance(stderr_log, str):
        combined.extend(tail_text(run_dir / stderr_log, lines=lines))
    return combined[-lines:]
