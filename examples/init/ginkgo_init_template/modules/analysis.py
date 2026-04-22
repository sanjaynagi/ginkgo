"""Analysis tasks for the starter workflow."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

from ginkgo import AssetRef, file, script, shell, task


_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@task("script", env="analysis_tools")
def build_brief(
    item: str,
    normalized_card: file,
    output_path: str,
) -> file:
    """Build one Markdown brief in a Pixi-backed script task.

    Parameters
    ----------
    item : str
        Synthetic item identifier.
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
def package_brief(brief: file | AssetRef, output_path: str) -> file:
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
    brief_path = Path(brief.artifact_path) if isinstance(brief, AssetRef) else Path(str(brief))
    quoted_brief = shlex.quote(str(brief_path))
    quoted_output = shlex.quote(str(output))
    cmd = (
        f"printf 'brief={quoted_brief}\\n' > {quoted_output} && "
        f"printf 'word_count=' >> {quoted_output} && "
        f"wc -w < {quoted_brief} >> {quoted_output}"
    )
    return shell(cmd=cmd, output=str(output))


@task()
def write_summary(
    items: list[str],
    seed_paths: list[str],
    normalized_cards: list[file],
    checksums: list[file],
    briefs: list[file],
    packages: list[file],
) -> file:
    """Assemble a compact JSON summary across all fan-out artifacts.

    Parameters
    ----------
    items : list[str]
        Item identifiers for each fan-out branch.
    seed_paths : list[str]
        Seed text artifact paths.
    normalized_cards : list[file]
        Normalized text artifacts.
    checksums : list[file]
        Checksum validation files from normalization.
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
    for item, seed_path, normalized_card, checksum, brief, package in zip(
        items,
        seed_paths,
        normalized_cards,
        checksums,
        briefs,
        packages,
        strict=True,
    ):
        rows.append(
            {
                "item": item,
                "seed_card": seed_path,
                "normalized_card": str(normalized_card),
                "checksum": str(checksum),
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
