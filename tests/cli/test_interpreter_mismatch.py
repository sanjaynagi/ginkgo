"""When the running interpreter cannot import what the project declares.

Cover for issue #221. Python and notebook task bodies execute in the
interpreter the CLI runs from, so a project's Pixi manifest describes the
environment they need. A globally installed CLI run inside such a project
reports only ``ModuleNotFoundError``; these tests pin the three places that
now explain it, and the good paths that stay quiet.
"""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from rich.console import Console
from rich.spinner import Spinner

from ginkgo.cli.errors import report_failure
from ginkgo.cli.renderers.models import CliRunSummary, FailureDetails
from ginkgo.cli.renderers.run import _RunEventState, _RunLayoutRenderer
from ginkgo.envs.interpreter import (
    detect_import_mismatch,
    import_failure_mismatch,
    workflow_import_roots,
)

ABSENT = "totally_absent_lib"


def _manifest(*, tasks: str = 'run = "ginkgo run"\n') -> Path:
    """Write a project ``pixi.toml`` in the current directory."""
    path = Path("pixi.toml")
    path.write_text(
        '[workspace]\nname = "demo"\n\n[dependencies]\npython = ">=3.11"\n\n[tasks]\n' + tasks,
        encoding="utf-8",
    )
    return path


def _workflow(*, imports: str = "") -> Path:
    """Write a one-task workflow, optionally importing *imports* at module scope."""
    path = Path("workflow.py")
    path.write_text(
        f"{imports}from ginkgo import flow, task\n"
        "\n\n"
        '@task("shell")\n'
        "def greet() -> str:\n"
        '    return "echo hello"\n'
        "\n\n"
        "@flow\n"
        "def main():\n"
        "    return greet()\n",
        encoding="utf-8",
    )
    return path


def _notebook(*, imports: str) -> Path:
    """Write a notebook whose single code cell carries *imports*."""
    path = Path("notebooks") / "overview.ipynb"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {"cell_type": "markdown", "source": [f"# not code: import {ABSENT}\n"]},
                    {"cell_type": "code", "source": [imports]},
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )
    return path


class TestImportScan:
    def test_stdlib_and_project_packages_are_not_environment_dependencies(self) -> None:
        Path("modules").mkdir()
        (Path("modules") / "prep.py").write_text("import json\n", encoding="utf-8")
        _workflow(imports="import json\nfrom modules import prep\nimport rich\n")

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo", "rich")

    def test_notebook_code_cells_count_but_prose_does_not(self) -> None:
        _workflow()
        _notebook(imports=f"import {ABSENT}\n")

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo", ABSENT)

    def test_env_and_script_directories_are_left_out(self) -> None:
        """Those bodies run under a declared env, not in the CLI's interpreter."""
        for directory in ("envs", "scripts", "tests"):
            Path(directory).mkdir()
            (Path(directory) / "thing.py").write_text(f"import {ABSENT}\n", encoding="utf-8")
        _workflow()

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo",)


class TestDetectMismatch:
    def test_missing_import_beside_a_manifest_is_reported(self) -> None:
        _manifest()
        _workflow(imports=f"import {ABSENT}\n")

        mismatch = detect_import_mismatch(
            workflow_path=Path("workflow.py"),
            project_root=Path.cwd(),
        )

        assert mismatch is not None
        assert mismatch.missing == (ABSENT,)
        assert mismatch.code == "interpreter_env_mismatch"
        assert mismatch.severity == "error"
        assert "cannot import: " + ABSENT in mismatch.message
        assert "pixi.toml" in mismatch.message
        assert str(mismatch.interpreter) in mismatch.suggestion
        assert "pixi run run" in mismatch.suggestion

    def test_no_manifest_means_no_check(self) -> None:
        _workflow(imports=f"import {ABSENT}\n")

        assert (
            detect_import_mismatch(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_an_interpreter_that_imports_everything_is_silent(self) -> None:
        """The ``pixi run`` case: the interpreter *is* the declared environment."""
        _manifest()
        _workflow(imports="import ginkgo\n")

        assert (
            detect_import_mismatch(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_a_manifest_without_a_plain_run_task_falls_back_to_the_full_command(self) -> None:
        _manifest(tasks='check = "ginkgo run --dry-run"\n')
        _workflow(imports=f"import {ABSENT}\n")

        mismatch = detect_import_mismatch(
            workflow_path=Path("workflow.py"),
            project_root=Path.cwd(),
        )

        assert mismatch is not None
        assert mismatch.project_command == "pixi run ginkgo run"


class TestImportFailureMismatch:
    def test_a_missing_module_message_is_explained(self) -> None:
        _manifest()

        mismatch = import_failure_mismatch(
            message=f"No module named '{ABSENT}'",
            project_root=Path.cwd(),
        )

        assert mismatch is not None
        assert mismatch.missing == (ABSENT,)

    def test_an_importable_module_is_not_explained_away(self) -> None:
        """A recorded failure from some other interpreter is not this mismatch."""
        _manifest()

        assert (
            import_failure_mismatch(message="No module named 'ginkgo'", project_root=Path.cwd())
            is None
        )

    def test_other_failures_are_left_alone(self) -> None:
        _manifest()

        assert import_failure_mismatch(message="exit code 1", project_root=Path.cwd()) is None
        assert import_failure_mismatch(message=None, project_root=Path.cwd()) is None


def _failure_panel_text(*, error: str) -> str:
    """Render one run failure panel and return its plain text."""
    console = Console(file=StringIO(), width=200, force_terminal=False)
    renderer = _RunLayoutRenderer(
        console=console,
        summary=CliRunSummary(run_id="run_1", mode="default", run_dir=Path.cwd(), cores=1),
        resources=None,
        state=_RunEventState(),
        activity_spinner=Spinner("dots"),
        time_spinner=Spinner("dots"),
    )
    details = FailureDetails(
        task_label="load_frame",
        exit_code=1,
        log_path=None,
        log_tail=[],
        error=error,
        failure_kind="import_error",
    )
    console.print(renderer.render_failure_panel(details))
    return console.file.getvalue()


class TestRenderedFailure:
    def test_the_hint_is_attached_to_a_task_import_failure(self) -> None:
        _manifest()

        text = _failure_panel_text(error=f"No module named '{ABSENT}'")

        assert "cannot import: " + ABSENT in text
        assert "pixi run run" in text

    def test_no_manifest_leaves_the_failure_panel_alone(self) -> None:
        text = _failure_panel_text(error=f"No module named '{ABSENT}'")

        assert "cannot import" not in text
        assert "pixi run" not in text

    def test_a_top_level_import_failure_is_explained(self) -> None:
        _manifest()
        stream = StringIO()

        report_failure(
            exc=ModuleNotFoundError(f"No module named '{ABSENT}'"),
            stream=stream,
            show_traceback=False,
        )

        text = stream.getvalue()
        assert "cannot import: " + ABSENT in text
        assert "Try: pixi run run" in text

    def test_a_top_level_import_failure_outside_a_project_stays_bare(self) -> None:
        stream = StringIO()

        report_failure(
            exc=ModuleNotFoundError(f"No module named '{ABSENT}'"),
            stream=stream,
            show_traceback=False,
        )

        assert "pixi run" not in stream.getvalue()
