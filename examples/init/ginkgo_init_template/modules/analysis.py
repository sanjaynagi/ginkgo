"""Analysis tasks for the starter workflow."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

from ginkgo import file, script, shell, task


_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@task("script", env="analysis_tools")
def build_brief(
    *,
    item: str,
    variant: str,
    normalized_card: file,
    output_path: str,
) -> file:
    """Build one Markdown brief in a Pixi-backed script task.

    Parameters
    ----------
    item : str
        Synthetic item identifier.
    variant : str
        Synthetic variant label.
    normalized_card : file
        Normalized text artifact.
    output_path : str
        Destination path for the Markdown brief.

    Returns
    -------
    file
        Markdown brief written by the script.
    """
    return script(_SCRIPTS_DIR / "build_brief.py", outputs=output_path)


@task(kind="shell", env="docker://ubuntu:24.04")
def package_brief(*, brief: file, output_path: str) -> file:
    """Package one Markdown brief in a Docker-backed shell task.

    Parameters
    ----------
    brief : file
        Markdown brief to package.
    output_path : str
        Destination path for the packaging report.

    Returns
    -------
    file
        Text packaging report.
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    quoted_brief = shlex.quote(str(brief))
    quoted_output = shlex.quote(str(output))
    cmd = (
        f"printf 'brief={quoted_brief}\\n' > {quoted_output} && "
        f"printf 'word_count=' >> {quoted_output} && "
        f"wc -w < {quoted_brief} >> {quoted_output}"
    )
    return shell(cmd=cmd, output=str(output))


@task()
def write_summary(
    *,
    items: list[str],
    variants: list[str],
    seed_cards: list[file],
    normalized_cards: list[file],
    briefs: list[file],
    packages: list[file],
) -> file:
    """Assemble a compact JSON summary across all fan-out artifacts.

    Parameters
    ----------
    items : list[str]
        Item identifiers for each fan-out branch.
    variants : list[str]
        Variant identifiers for each fan-out branch.
    seed_cards : list[file]
        Seed text artifacts.
    normalized_cards : list[file]
        Normalized text artifacts.
    briefs : list[file]
        Pixi-built Markdown briefs.
    packages : list[file]
        Docker-built packaging reports.

    Returns
    -------
    file
        JSON summary path.
    """
    rows = []
    for item, variant, seed_card, normalized_card, brief, package in zip(
        items,
        variants,
        seed_cards,
        normalized_cards,
        briefs,
        packages,
        strict=True,
    ):
        rows.append(
            {
                "item": item,
                "variant": variant,
                "seed_card": str(seed_card),
                "normalized_card": str(normalized_card),
                "brief": str(brief),
                "package": str(package),
            }
        )

    output = Path("results/summary.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "row_count": len(rows),
                "rows": rows,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return file(str(output))
