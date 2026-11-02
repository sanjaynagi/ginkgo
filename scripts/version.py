"""Set or check the version that ginkgo and its plugins release at.

Core and every first-party plugin release together at one version. The version
appears in each package's ``[project] version``, in every ``ginkgo-*==X`` pin
between them, and in the docs. ``set`` rewrites all of them and relocks;
``check`` fails unless all of them match: CI runs it against core's version,
and the release workflow against the pushed tag.

    python scripts/version.py set 0.5.0
    python scripts/version.py check [0.5.0]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

#: Files that carry the release version, as a project version or as a pin.
FILES = [
    ROOT / "pyproject.toml",
    *sorted(ROOT.glob("plugins/*/pyproject.toml")),
    ROOT / "docs/architecture/plugins.md",
]

#: Pixi workspaces whose lock records the editable ginkgo's metadata.
LOCKED_WORKSPACES = [ROOT, ROOT / "plugins/ginkgo-pixi/tests/envs/test_env"]

_PROJECT_VERSION = re.compile(r'^(version = ")([^"]+)(")', re.MULTILINE)
_PIN = re.compile(r"(ginkgo-[a-z]+==)([0-9][^\s\"';,`)]*)")


def _versions(path: Path) -> list[str]:
    """Return every release version one file states."""
    text = path.read_text()
    found = [match.group(2) for match in _PIN.finditer(text)]
    project = _PROJECT_VERSION.search(text) if path.suffix == ".toml" else None
    return found + ([project.group(2)] if project else [])


def check(version: str) -> list[str]:
    """Return a ``path: version`` line for each release version that is not ``version``."""
    return [
        f"{path.relative_to(ROOT)}: {found}"
        for path in FILES
        for found in _versions(path)
        if found != version
    ]


def set_version(version: str) -> None:
    """Rewrite every release version to ``version``, then relock."""
    for path in FILES:
        text = _PIN.sub(rf"\g<1>{version}", path.read_text())
        if path.suffix == ".toml":
            text = _PROJECT_VERSION.sub(rf"\g<1>{version}\g<3>", text, count=1)
        path.write_text(text)
    for workspace in LOCKED_WORKSPACES:
        subprocess.run(["pixi", "lock"], cwd=workspace, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["set", "check"])
    parser.add_argument(
        "version",
        nargs="?",
        help="for example 0.5.0, and a leading v is ignored; check defaults to core's version",
    )
    args = parser.parse_args()
    if args.version is None:
        if args.action == "set":
            parser.error("set needs a version")
        args.version = _versions(ROOT / "pyproject.toml")[-1]
    version = args.version.removeprefix("v")
    if args.action == "set":
        set_version(version)
    wrong = check(version)
    if wrong:
        print(f"Not at {version}:", *wrong, sep="\n  ", file=sys.stderr)
        return 1
    print(f"Every package and pin is at {version}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
