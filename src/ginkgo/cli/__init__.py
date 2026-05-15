"""CLI entrypoint and compatibility re-exports."""

from __future__ import annotations

from ginkgo.cli.renderers.common import (
    _core_unit_label,
    _environment_label,
    _time_of_day_spinner,
    _truncate_task_label,
)


def main(argv: list[str] | None = None) -> int:
    """Run the CLI entrypoint.

    Parameters
    ----------
    argv : list[str] | None
        Optional argument vector passed to the CLI parser.

    Returns
    -------
    int
        Process exit code returned by the CLI application.
    """
    from ginkgo.cli.app import main as app_main

    return app_main(argv)


__all__ = [
    "main",
    "_core_unit_label",
    "_environment_label",
    "_time_of_day_spinner",
    "_truncate_task_label",
]
