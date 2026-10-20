"""Task arguments, rendered as a notebook's command line, read back through mo.cli_args()."""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

from ginkgo.runtime.task_runners.shell import render_repeated_cli_tokens


def test_mo_cli_args_reads_the_rendering_back(tmp_path: Path) -> None:
    reader = tmp_path / "reader.py"
    # ``mo.cli_args()`` only works inside a marimo app, which is exactly
    # how a marimo notebook task runs: ``python notebook.py --name value``.
    reader.write_text(
        "import marimo\n"
        "app = marimo.App()\n"
        "@app.cell\n"
        "def _():\n"
        "    import json\n"
        "    import marimo as mo\n"
        "    args = mo.cli_args()\n"
        "    print(json.dumps({\n"
        "        'one': args.get_all('one'),\n"
        "        'many': args.get_all('many'),\n"
        "        'none': args.get_all('none'),\n"
        "        'flag': args.get('flag'),\n"
        "    }))\n"
        "    return\n"
        "if __name__ == '__main__':\n"
        "    app.run()\n",
        encoding="utf-8",
    )
    tokens = [
        *render_repeated_cli_tokens(option="--one", value=["only.tsv"]),
        *render_repeated_cli_tokens(option="--many", value=["a b.tsv", "c.tsv"]),
        *render_repeated_cli_tokens(option="--none", value=[]),
        *render_repeated_cli_tokens(option="--flag", value=True),
    ]
    cmd = " ".join([shlex.quote(sys.executable), shlex.quote(str(reader)), *tokens])
    completed = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, check=True, cwd=tmp_path
    )
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == {
        "one": ["only.tsv"],
        "many": ["a b.tsv", "c.tsv"],
        "none": [],
        "flag": True,
    }
