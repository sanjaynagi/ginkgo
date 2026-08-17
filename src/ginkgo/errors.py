"""Ginkgo's error taxonomy, and where a failure happened in the user's code.

Two kinds of failure reach the CLI, and they deserve different reports:

* A :class:`GinkgoError` is a mistake ginkgo detected and can explain — a
  missing environment, an undeclared parameter, a refused mount. Its message
  *is* the report, so the CLI prints one line and nothing else.
* Anything else is unexpected: a bug in the workflow being run, or in ginkgo.
  The message alone is rarely enough, so the CLI adds the location of the
  innermost frame in code the user wrote, and can print the whole traceback.

:func:`failure_location` supplies that location, and is shared by the CLI's
top-level handler and by ``ginkgo doctor``, so both point at the same line.
"""

from __future__ import annotations

import sysconfig
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

__all__ = ["FailureLocation", "GinkgoError", "failure_location"]


class GinkgoError(Exception):
    """A failure ginkgo raises deliberately, whose message is the whole report.

    Raise this — or one of its subclasses — when ginkgo has found a mistake it
    can describe in a sentence. The CLI prints ``✖ <message>`` for it, with no
    location and no traceback, because neither would tell the user anything the
    message does not.
    """


@dataclass(frozen=True)
class FailureLocation:
    """A single frame: the file, line, and function a failure came from."""

    path: Path
    lineno: int
    function: str

    def __str__(self) -> str:
        return f"{self.path}:{self.lineno} in {self.function}"


def failure_location(exc: BaseException) -> FailureLocation | None:
    """Locate *exc* in code the user wrote.

    Returns the innermost frame that is neither ginkgo's own source nor a
    library (the standard library, site-packages, or any other installed
    location). Chained exceptions are followed, so a failure ginkgo re-raised
    still reports the user's line rather than the re-raise site.

    Parameters
    ----------
    exc : BaseException
        The exception to locate.

    Returns
    -------
    FailureLocation | None
        The frame to report, or ``None`` when no user code is on the stack —
        which means the failure came from one of ginkgo's own checks.
    """
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop(0)
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        located = _innermost_user_frame(current.__traceback__)
        if located is not None:
            return located
        pending.extend(
            candidate
            for candidate in (current.__cause__, current.__context__)
            if candidate is not None
        )
    return None


def _innermost_user_frame(tb: TracebackType | None) -> FailureLocation | None:
    """Return the deepest frame of *tb* that lives in code the user wrote."""
    found: FailureLocation | None = None
    while tb is not None:
        frame = tb.tb_frame
        if _is_user_code(frame.f_code.co_filename):
            found = FailureLocation(
                path=Path(frame.f_code.co_filename),
                lineno=tb.tb_lineno,
                function=frame.f_code.co_name,
            )
        tb = tb.tb_next
    return found


def _library_roots() -> tuple[Path, ...]:
    """Directories holding installed code: the standard library and packages."""
    roots: list[Path] = []
    for key in ("stdlib", "platstdlib", "purelib", "platlib"):
        location = sysconfig.get_paths().get(key)
        if location:
            roots.append(Path(location).resolve())
    return tuple(roots)


_GINKGO_ROOT = Path(__file__).resolve().parent
_LIBRARY_ROOTS = _library_roots()


def _is_user_code(filename: str) -> bool:
    """Whether *filename* is source the user wrote rather than installed code."""
    if not filename or filename.startswith("<"):
        return False
    path = Path(filename)
    if not path.is_absolute():
        path = Path.cwd() / path
    resolved = path.resolve()
    return not any(_is_within(resolved, root) for root in (_GINKGO_ROOT, *_LIBRARY_ROOTS))


def _is_within(path: Path, root: Path) -> bool:
    """Whether *path* sits at or below *root*."""
    return path == root or root in path.parents
