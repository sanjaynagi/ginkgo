"""Preparation tasks for the starter workflow."""

from __future__ import annotations

import shlex
from pathlib import Path

from ginkgo import Out, asset, file, shell, task


def _seed_card_has_content(payload: object) -> bool:
    """Return whether a seed-card file exists and has content."""
    return isinstance(payload, Path) and payload.is_file() and payload.stat().st_size > 0


@task()
def write_seed_card(item: str, output_path: Out[file]) -> file:
    """Write a tiny text artifact for one item.

    Parameters
    ----------
    item : str
        Synthetic item identifier.
    output_path : Out[file]
        Destination path for the seed artifact. This task writes it; its
        parent directory is created automatically.

    Returns
    -------
    file
        Seed text artifact path.
    """
    output = Path(output_path)
    output.write_text(
        f"item={item}\nlabel={item}\n",
        encoding="utf-8",
    )
    return asset(
        output,
        name=f"starter/seed_cards/{item}",
        metadata={"item": item, "stage": "seed"},
        checks=[_seed_card_has_content],
    )


@task(kind="shell")
def normalize_seed_card(
    seed_card: file, output_path: Out[file], check_path: Out[file]
) -> list[file]:
    """Normalize one seed artifact and produce a validation checksum.

    Parameters
    ----------
    seed_card : file
        Seed text artifact.
    output_path : Out[file]
        Destination path for the normalized artifact.
    check_path : Out[file]
        Destination path for a checksum validation file.

    Returns
    -------
    list[file]
        ``[normalized_card, checksum_file]``.
    """
    quoted_input = shlex.quote(str(seed_card))
    quoted_output = shlex.quote(str(output_path))
    quoted_check = shlex.quote(str(check_path))
    cmd = (
        f"tr '[:lower:]' '[:upper:]' < {quoted_input} > {quoted_output} && "
        f"shasum {quoted_output} > {quoted_check}"
    )
    return shell(cmd=cmd)
