"""Preparation tasks for the starter workflow."""

from __future__ import annotations

import shlex
from pathlib import Path

from ginkgo import file, shell, task


@task()
def write_seed_card(*, item: str, output_path: str) -> file:
    """Write a tiny text artifact for one item.

    Parameters
    ----------
    item : str
        Synthetic item identifier.
    output_path : str
        Destination path for the seed artifact.

    Returns
    -------
    file
        Seed text artifact path.
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        f"item={item}\nlabel={item}\n",
        encoding="utf-8",
    )
    return file(str(output))


@task(kind="shell")
def normalize_seed_card(*, seed_card: file, output_path: str) -> file:
    """Normalize one seed artifact with a local shell command.

    Parameters
    ----------
    seed_card : file
        Seed text artifact.
    output_path : str
        Destination path for the normalized artifact.

    Returns
    -------
    file
        Upper-cased normalized artifact.
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    quoted_input = shlex.quote(str(seed_card))
    quoted_output = shlex.quote(str(output))
    return shell(
        cmd=f"tr '[:lower:]' '[:upper:]' < {quoted_input} > {quoted_output}",
        output=str(output),
    )
