"""Set or check the version ginkgo and its plugins release at.

They release together, so the version appears in every package's
``[project] version`` and in every ``ginkgo-*==`` pin between them.

    python scripts/version.py set 0.5.0    # rewrite them all, then relock
    python scripts/version.py check 0.5.0  # fail unless they all match
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECTS = [ROOT / "pyproject.toml", *sorted(ROOT.glob("plugins/*/pyproject.toml"))]
VERSIONS = re.compile(r'(^version = "|ginkgo-[a-z]+==)([^"]+)', re.MULTILINE)


def main(action: str, version: str) -> int:
    version = version.removeprefix("v")
    if action == "set":
        for path in PYPROJECTS:
            path.write_text(VERSIONS.sub(rf"\g<1>{version}", path.read_text()))
        for workspace in [ROOT, ROOT / "plugins/ginkgo-pixi/tests/envs/test_env"]:
            subprocess.run(["pixi", "lock"], cwd=workspace, check=True)
    wrong = [
        f"{path.relative_to(ROOT)}: {match.group(2)}"
        for path in PYPROJECTS
        for match in VERSIONS.finditer(path.read_text())
        if match.group(2) != version
    ]
    if wrong:
        sys.exit(f"Not at {version}:\n  " + "\n  ".join(wrong))
    print(f"Every package and pin is at {version}.")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
