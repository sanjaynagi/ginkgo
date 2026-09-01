"""When the running interpreter cannot import what the project declares.

Cover for issue #221. Python task bodies execute in the interpreter the CLI
runs from, so a project's Pixi manifest describes the environment they need. A
globally installed CLI run inside such a project reports only
``ModuleNotFoundError``; these tests pin the places that now explain it, the
good paths that stay quiet, and the two ways a missing import can be reported
— the wrong interpreter, or a manifest that is genuinely short.
"""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from rich.console import Console
from rich.spinner import Spinner

from ginkgo.cli.errors import report_failure
from ginkgo.cli.renderers.models import CliRunSummary, FailureDetails
from ginkgo.cli.renderers.run import _RunEventState, _RunLayoutRenderer
from ginkgo.envs.interpreter import (
    InterpreterMismatch,
    MissingDependency,
    detect_import_problem,
    explain_import_failure,
    source_import_roots,
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


def _notebook(*, code: str, markdown: str = "") -> Path:
    """Write a notebook with one markdown cell and one code cell."""
    path = Path("notebooks") / "overview.ipynb"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {"cell_type": "markdown", "source": [markdown]},
                    {"cell_type": "code", "source": [code]},
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )
    return path


def _inside_project_env(monkeypatch: pytest.MonkeyPatch, *, manifest: Path) -> None:
    """Make the running interpreter look like *manifest*'s own Pixi environment."""
    monkeypatch.setenv("PIXI_PROJECT_MANIFEST", str(manifest.resolve()))


def _outside_project_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make sure no inherited Pixi environment claims this interpreter.

    The suite itself usually runs under ``pixi run``, which exports
    ``PIXI_PROJECT_MANIFEST`` for *ginkgo's own* manifest. That never matches a
    test's temporary one, but clearing it keeps these tests independent of how
    they were launched.
    """
    monkeypatch.delenv("PIXI_PROJECT_MANIFEST", raising=False)


class TestImportScan:
    def test_stdlib_and_project_packages_are_not_environment_dependencies(self) -> None:
        Path("modules").mkdir()
        (Path("modules") / "prep.py").write_text("import json\n", encoding="utf-8")
        _workflow(imports="import json\nfrom modules import prep\nimport rich\n")

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo", "rich")

    def test_notebook_code_cells_count_but_prose_does_not(self) -> None:
        _workflow()
        _notebook(code=f"import {ABSENT}\n", markdown=f"# an example\nimport {ABSENT}_prose\n")

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo", ABSENT)

    def test_env_and_script_directories_are_left_out(self) -> None:
        """Those bodies run under a declared env, not in the CLI's interpreter."""
        for directory in ("envs", "scripts", "tests"):
            Path(directory).mkdir()
            (Path(directory) / "thing.py").write_text(f"import {ABSENT}\n", encoding="utf-8")
        _workflow()

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo",)

    def test_a_file_that_does_not_parse_contributes_nothing(self) -> None:
        _workflow()
        Path("broken.py").write_text(f"import {ABSENT}\ndef (\n", encoding="utf-8")

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo",)


class TestImportsThatDoNotRun:
    """Imports the environment does not have to supply, so they must not count.

    Each of these is a shape that reads like an import in raw text but either
    never executes or is optional by construction. The scan parses instead of
    matching lines, which is what keeps them out.
    """

    def test_an_import_inside_a_docstring_is_not_an_import(self) -> None:
        assert source_import_roots(text='"""Example:\n\nimport seaborn\n"""\n') == frozenset()

    def test_an_import_inside_a_string_literal_is_not_an_import(self) -> None:
        assert source_import_roots(text='SNIPPET = "import seaborn"\n') == frozenset()

    def test_a_type_checking_import_is_not_a_runtime_import(self) -> None:
        text = (
            "from typing import TYPE_CHECKING\n\n"
            "if TYPE_CHECKING:\n"
            "    import pandas\n"
            "    from numpy import ndarray\n"
        )

        assert source_import_roots(text=text) == {"typing"}

    def test_a_qualified_type_checking_guard_is_recognised_too(self) -> None:
        assert source_import_roots(
            text="import typing\nif typing.TYPE_CHECKING:\n    import pandas\n"
        ) == {"typing"}

    def test_the_else_branch_of_a_type_checking_guard_still_counts(self) -> None:
        text = "if TYPE_CHECKING:\n    import pandas\nelse:\n    import polars\n"

        assert source_import_roots(text=text) == {"polars"}

    def test_an_optional_dependency_guarded_by_import_error_does_not_count(self) -> None:
        text = "try:\n    import seaborn\nexcept ImportError:\n    seaborn = None\n"

        assert source_import_roots(text=text) == frozenset()

    def test_a_module_not_found_guard_counts_the_same_way(self) -> None:
        text = "try:\n    import seaborn\nexcept (ModuleNotFoundError, OSError):\n    pass\n"

        assert source_import_roots(text=text) == frozenset()

    def test_a_try_guarding_something_else_still_declares_its_imports(self) -> None:
        text = "try:\n    import pandas\nexcept ValueError:\n    pass\n"

        assert source_import_roots(text=text) == {"pandas"}

    def test_a_deferred_import_inside_a_function_body_still_counts(self) -> None:
        assert source_import_roots(text="def load():\n    import pandas\n") == {"pandas"}

    def test_a_relative_import_is_the_project_satisfying_itself(self) -> None:
        assert source_import_roots(text="from .modules import prep\n") == frozenset()

    def test_a_docstring_example_does_not_trip_the_scan(self) -> None:
        """The end-to-end version: doctor's finding, not just the extractor."""
        _manifest()
        _workflow(imports='"""Docs.\n\nExample::\n\n    import seaborn\n"""\n')

        assert (
            detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_an_optional_import_does_not_trip_the_scan(self) -> None:
        _manifest()
        _workflow()
        Path("optional.py").write_text(
            f"try:\n    import {ABSENT}\nexcept ImportError:\n    {ABSENT} = None\n",
            encoding="utf-8",
        )

        assert (
            detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_a_type_checking_import_does_not_trip_the_scan(self) -> None:
        _manifest()
        _workflow()
        Path("typed.py").write_text(
            f"from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    import {ABSENT}\n",
            encoding="utf-8",
        )

        assert (
            detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_a_markdown_notebook_cell_does_not_trip_the_scan(self) -> None:
        _manifest()
        _workflow()
        _notebook(code="import json\n", markdown=f"Install it first:\n\nimport {ABSENT}\n")

        assert (
            detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )


class TestDetectImportProblem:
    def test_missing_import_beside_a_manifest_is_reported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _outside_project_env(monkeypatch)
        _manifest()
        _workflow(imports=f"import {ABSENT}\n")

        finding = detect_import_problem(
            workflow_path=Path("workflow.py"),
            project_root=Path.cwd(),
        )

        assert isinstance(finding, InterpreterMismatch)
        assert finding.missing == (ABSENT,)
        assert finding.code == "interpreter_env_mismatch"
        assert finding.severity == "error"
        assert "cannot import: " + ABSENT in finding.message
        assert "pixi.toml" in finding.message
        assert str(finding.interpreter) in finding.suggestion
        assert "pixi run run" in finding.suggestion

    def test_no_manifest_means_no_check(self) -> None:
        _workflow(imports=f"import {ABSENT}\n")

        assert (
            detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_an_interpreter_that_imports_everything_is_silent(self) -> None:
        """The ``pixi run`` case: the interpreter *is* the declared environment."""
        _manifest()
        _workflow(imports="import ginkgo\n")

        assert (
            detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())
            is None
        )

    def test_a_manifest_without_a_plain_run_task_falls_back_to_the_full_command(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _outside_project_env(monkeypatch)
        _manifest(tasks='check = "ginkgo run --dry-run"\n')
        _workflow(imports=f"import {ABSENT}\n")

        finding = detect_import_problem(
            workflow_path=Path("workflow.py"),
            project_root=Path.cwd(),
        )

        assert isinstance(finding, InterpreterMismatch)
        assert finding.project_command == "pixi run ginkgo run"


class TestInsideTheProjectEnvironment:
    """Running from the manifest's own environment: right interpreter, short manifest.

    ``pixi run`` advice would be a no-op here, so the finding has to say
    something else — and must never claim the interpreter is wrong.
    """

    def test_a_missing_import_is_a_missing_dependency_not_a_wrong_interpreter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest = _manifest()
        _inside_project_env(monkeypatch, manifest=manifest)
        _workflow(imports=f"import {ABSENT}\n")

        finding = detect_import_problem(
            workflow_path=Path("workflow.py"),
            project_root=Path.cwd(),
        )

        assert isinstance(finding, MissingDependency)
        assert finding.code == "missing_dependency"
        assert finding.missing == (ABSENT,)
        assert "pixi run" not in "\n".join(finding.hint_lines)
        assert "pixi install" in finding.suggestion
        assert "pixi.toml" in finding.suggestion

    def test_a_prefix_under_the_manifest_pixi_directory_counts_as_the_project_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The other signal: no env var, but ``sys.prefix`` sits in ``.pixi/envs``."""
        _outside_project_env(monkeypatch)
        manifest = _manifest()
        prefix = manifest.parent.resolve() / ".pixi" / "envs" / "default"
        prefix.mkdir(parents=True)
        monkeypatch.setattr("ginkgo.envs.interpreter.sys.prefix", str(prefix))
        _workflow(imports=f"import {ABSENT}\n")

        finding = detect_import_problem(
            workflow_path=Path("workflow.py"),
            project_root=Path.cwd(),
        )

        assert isinstance(finding, MissingDependency)

    def test_a_reported_failure_is_explained_without_the_interpreter_framing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest = _manifest()
        _inside_project_env(monkeypatch, manifest=manifest)

        finding = explain_import_failure(
            message=f"No module named '{ABSENT}'",
            project_root=Path.cwd(),
        )

        assert isinstance(finding, MissingDependency)


class TestExplainImportFailure:
    def test_a_missing_module_message_is_explained(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _outside_project_env(monkeypatch)
        _manifest()

        finding = explain_import_failure(
            message=f"No module named '{ABSENT}'",
            project_root=Path.cwd(),
        )

        assert isinstance(finding, InterpreterMismatch)
        assert finding.missing == (ABSENT,)

    def test_an_importable_module_is_not_explained_away(self) -> None:
        """A recorded failure from some other interpreter is not this mismatch."""
        _manifest()

        assert (
            explain_import_failure(message="No module named 'ginkgo'", project_root=Path.cwd())
            is None
        )

    def test_other_failures_are_left_alone(self) -> None:
        _manifest()

        assert explain_import_failure(message="exit code 1", project_root=Path.cwd()) is None
        assert explain_import_failure(message=None, project_root=Path.cwd()) is None


def _renderer() -> _RunLayoutRenderer:
    """Build a run renderer writing into a string buffer."""
    console = Console(file=StringIO(), width=200, force_terminal=False)
    return _RunLayoutRenderer(
        console=console,
        summary=CliRunSummary(run_id="run_1", mode="default", run_dir=Path.cwd(), cores=1),
        resources=None,
        state=_RunEventState(),
        activity_spinner=Spinner("dots"),
        time_spinner=Spinner("dots"),
    )


def _failure(*, error: str, kind: str = "python", env_label: str = "local") -> FailureDetails:
    """One failed task, defaulting to the in-process case."""
    return FailureDetails(
        task_label="load_frame",
        exit_code=1,
        log_path=None,
        log_tail=[],
        error=error,
        failure_kind="import_error",
        task_kind=kind,
        env_label=env_label,
    )


def _failures_text(details: list[FailureDetails]) -> str:
    """Render a run's failure panels and return their plain text."""
    renderer = _renderer()
    renderer._console.print(renderer.render_failure_details(details))
    return renderer._console.file.getvalue()


class TestRenderedFailure:
    def test_the_hint_is_attached_to_a_python_task_import_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _outside_project_env(monkeypatch)
        _manifest()

        text = _failures_text([_failure(error=f"No module named '{ABSENT}'")])

        assert "cannot import: " + ABSENT in text
        assert "pixi run run" in text

    def test_a_shell_task_gets_no_interpreter_advice(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Its body ran in a subprocess, where this interpreter is not the issue."""
        _outside_project_env(monkeypatch)
        _manifest()

        text = _failures_text([_failure(error=f"No module named '{ABSENT}'", kind="shell")])

        assert "cannot import" not in text
        assert "pixi run" not in text

    def test_a_python_task_in_a_declared_environment_gets_no_advice_either(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _outside_project_env(monkeypatch)
        _manifest()

        text = _failures_text(
            [_failure(error=f"No module named '{ABSENT}'", env_label="analysis_tools")]
        )

        assert "cannot import" not in text

    def test_the_hint_is_rendered_once_however_many_tasks_failed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _outside_project_env(monkeypatch)
        _manifest()

        text = _failures_text([_failure(error=f"No module named '{ABSENT}'") for _ in range(3)])

        assert text.count("Try: pixi run run") == 1

    def test_no_manifest_leaves_the_failure_panel_alone(self) -> None:
        text = _failures_text([_failure(error=f"No module named '{ABSENT}'")])

        assert "cannot import" not in text
        assert "pixi run" not in text

    def test_a_top_level_import_failure_is_explained(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _outside_project_env(monkeypatch)
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
