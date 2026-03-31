"""Reporting tasks for the starter workflow."""

from __future__ import annotations

import json
from pathlib import Path

from ginkgo import expand, file, notebook, task


_NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent / "notebooks"


@task("notebook")
def render_overview_notebook(*, summary_path: file, run_label: str) -> file:
    """Render an HTML overview notebook for the starter run.

    Parameters
    ----------
    summary_path : file
        JSON summary artifact.
    run_label : str
        Short run label shown in the notebook.

    Returns
    -------
    file
        Rendered HTML notebook path.
    """
    return notebook(_NOTEBOOKS_DIR / "overview.ipynb")


@task()
def write_delivery_manifest(
    *,
    summary_path: file,
    notebook_html: file,
    package_reports: list[file],
) -> file:
    """Write the final delivery manifest for the starter workflow.

    Parameters
    ----------
    summary_path : file
        JSON summary artifact.
    notebook_html : file
        Rendered notebook HTML.
    package_reports : list[file]
        Packaging reports produced by the Docker task.

    Returns
    -------
    file
        Markdown manifest path.
    """
    summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    lines = [
        "# Delivery Manifest",
        "",
        f"- Summary: {summary_path}",
        f"- Notebook: {notebook_html}",
        f"- Packaged branches: {summary['row_count']}",
        "",
        "## Package Reports",
        "",
        *expand("- {path}", path=[str(path) for path in package_reports]),
    ]

    output = Path("results/delivery_manifest.md")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))
