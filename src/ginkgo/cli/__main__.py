"""Module entrypoint for ``python -m ginkgo.cli``."""

from __future__ import annotations

import sys

from ginkgo.cli import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
