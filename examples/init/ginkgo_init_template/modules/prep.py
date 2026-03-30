"""Preparation tasks for the starter workflow."""

from __future__ import annotations

import shlex
from pathlib import Path

from ginkgo import AssetRef, asset, file, shell, task


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
    return asset(
        output,
        name=f"starter/seed_cards/{item}",
        metadata={"item": item, "stage": "seed"},
    )


@task(kind="shell")
def normalize_seed_card(
    *, seed_card: file | AssetRef, output_path: str, check_path: str
) -> list[file]:
    """Normalize one seed artifact and produce a validation checksum.

    Parameters
    ----------
    seed_card : file
        Seed text artifact.
    output_path : str
        Destination path for the normalized artifact.
    check_path : str
        Destination path for a checksum validation file.

    Returns
    -------
    list[file]
        ``[normalized_card, checksum_file]``.
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    check = Path(check_path)
    check.parent.mkdir(parents=True, exist_ok=True)
    input_path = (
        Path(seed_card.artifact_path) if isinstance(seed_card, AssetRef) else Path(str(seed_card))
    )
    quoted_input = shlex.quote(str(input_path))
    quoted_output = shlex.quote(str(output))
    quoted_check = shlex.quote(str(check))
    cmd = (
        f"tr '[:lower:]' '[:upper:]' < {quoted_input} > {quoted_output} && "
        f"shasum {quoted_output} > {quoted_check}"
    )
    return shell(cmd=cmd, output=[str(output), str(check)])
