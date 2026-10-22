"""CLI entrypoint.

This module stays free of heavy imports so that an interrupt while the CLI is
still importing is caught by :func:`main` and exits like any other Ctrl-C.
"""

from __future__ import annotations

#: Exit status conventionally used for termination by SIGINT (128 + 2).
INTERRUPT_EXIT_CODE = 130


def main(argv: list[str] | None = None) -> int:
    """Run the CLI entrypoint.

    Parameters
    ----------
    argv : list[str] | None
        Optional argument vector passed to the CLI parser.

    Returns
    -------
    int
        Process exit code returned by the CLI application, or
        :data:`INTERRUPT_EXIT_CODE` for an interrupt before a command starts.
    """
    # The application reports an interrupt during a command itself; this
    # catches one that lands while it is still importing or parsing.
    try:
        from ginkgo.cli.app import main as app_main

        return app_main(argv)
    except KeyboardInterrupt:
        return INTERRUPT_EXIT_CODE
    except ImportError as exc:
        # An extension module interrupted mid-initialisation reports the
        # interrupt as the cause of an ImportError.
        if isinstance(exc.__cause__, KeyboardInterrupt):
            return INTERRUPT_EXIT_CODE
        raise


__all__ = ["INTERRUPT_EXIT_CODE", "main"]
