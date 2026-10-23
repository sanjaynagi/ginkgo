"""When the running interpreter cannot import what the project declares.

Cover for issue #221. Python task bodies execute in the interpreter the CLI
runs from, so a project's Pixi manifest describes the environment they need. A
globally installed CLI run inside such a project reports only
``ModuleNotFoundError``; these tests pin the places that now explain it, the
good paths that stay quiet, and the two ways a missing import can be reported
— the wrong interpreter, or a manifest that is genuinely short.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pytest
from rich.console import Console
from rich.spinner import Spinner

from ginkgo.cli.errors import report_failure
from ginkgo.cli.renderers.models import CliRunSummary, FailureDetails
from ginkgo.cli.renderers.run import _RunEventState, _RunLayoutRenderer
from ginkgo.runtime.diagnostics import collect_workflow_diagnostics
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

    def test_env_and_script_directories_are_left_out(self) -> None:
        """Those bodies run under a declared env, not in the CLI's interpreter."""
        for directory in ("envs", "scripts", "tests"):
            Path(directory).mkdir()
            (Path(directory) / "thing.py").write_text(f"import {ABSENT}\n", encoding="utf-8")
        _workflow()

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo",)

    def test_a_source_run_in_a_tasks_own_env_is_left_out(self) -> None:
        """A notebook a task runs in its ``env=`` imports from that env."""
        Path("notebooks").mkdir()
        notebook = Path("notebooks") / "overview.py"
        notebook.write_text(f"import {ABSENT}\n", encoding="utf-8")
        _workflow()

        assert workflow_import_roots(
            workflow_path=Path("workflow.py"), excluded_sources=frozenset({notebook.resolve()})
        ) == ("ginkgo",)

    def test_doctor_learns_which_sources_run_in_a_tasks_env(self) -> None:
        Path("tool.py").write_text(f"import {ABSENT}\n", encoding="utf-8")
        Path("workflow.py").write_text(
            "from pathlib import Path\n\n"
            "from ginkgo import flow, script, task\n\n"
            "_HERE = Path(__file__).resolve().parent\n\n\n"
            '@task("script", env="analysis")\n'
            "def tool():\n"
            '    return script(_HERE / "tool.py")\n\n\n'
            "@flow\n"
            "def main():\n"
            "    return tool()\n",
            encoding="utf-8",
        )

        check = collect_workflow_diagnostics(
            workflow_path=Path("workflow.py").resolve(), config_paths=[], secret_resolver=None
        )

        assert check.env_sources == frozenset({Path("tool.py").resolve()})

    def test_a_file_that_does_not_parse_contributes_nothing(self) -> None:
        _workflow()
        Path("broken.py").write_text(f"import {ABSENT}\ndef (\n", encoding="utf-8")

        assert workflow_import_roots(workflow_path=Path("workflow.py")) == ("ginkgo",)


@pytest.mark.parametrize(
    ("text", "roots"),
    [
        # Shapes that read like an import in raw text but never execute, or are
        # optional by construction. The scan parses instead of matching lines.
        ('"""Example:\n\nimport seaborn\n"""\n', set()),
        ('SNIPPET = "import seaborn"\n', set()),
        (
            "from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n"
            "    import pandas\n    from numpy import ndarray\n",
            {"typing"},
        ),
        ("import typing\nif typing.TYPE_CHECKING:\n    import pandas\n", {"typing"}),
        ("if TYPE_CHECKING:\n    import pandas\nelse:\n    import polars\n", {"polars"}),
        ("try:\n    import seaborn\nexcept ImportError:\n    seaborn = None\n", set()),
        ("try:\n    import seaborn\nexcept (ModuleNotFoundError, OSError):\n    pass\n", set()),
        ("try:\n    import pandas\nexcept ValueError:\n    pass\n", {"pandas"}),
        ("def load():\n    import pandas\n", {"pandas"}),
        ("from .modules import prep\n", set()),
    ],
)
def test_source_import_roots(text: str, roots: set[str]) -> None:
    assert source_import_roots(text=text) == roots


def _detect():
    return detect_import_problem(workflow_path=Path("workflow.py"), project_root=Path.cwd())


@pytest.mark.parametrize(
    ("manifest", "source"),
    [
        (True, f'"""Docs.\n\nExample::\n\n    import {ABSENT}\n"""\n'),
        (True, f"try:\n    import {ABSENT}\nexcept ImportError:\n    {ABSENT} = None\n"),
        (True, f"from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    import {ABSENT}\n"),
        # The ``pixi run`` case: the interpreter *is* the declared environment.
        (True, "import ginkgo\n"),
        # No manifest means no check.
        (False, f"import {ABSENT}\n"),
    ],
    ids=["docstring", "optional", "type-checking", "importable", "no-manifest"],
)
def test_nothing_is_reported_when_nothing_is_missing(manifest: bool, source: str) -> None:
    if manifest:
        _manifest()
    _workflow()
    Path("extra.py").write_text(source, encoding="utf-8")

    assert _detect() is None


@pytest.mark.parametrize(
    ("tasks", "command"),
    [
        ('run = "ginkgo run"\n', "pixi run run"),
        # Without a plain run task, the advice falls back to the full command.
        ('check = "ginkgo run --dry-run"\n', "pixi run ginkgo run"),
    ],
)
def test_missing_import_beside_a_manifest_is_reported(
    monkeypatch: pytest.MonkeyPatch, tasks: str, command: str
) -> None:
    _outside_project_env(monkeypatch)
    _manifest(tasks=tasks)
    _workflow(imports=f"import {ABSENT}\n")

    finding = _detect()

    assert isinstance(finding, InterpreterMismatch)
    assert (finding.missing, finding.code, finding.severity) == (
        (ABSENT,),
        "interpreter_env_mismatch",
        "error",
    )
    assert "cannot import: " + ABSENT in finding.message and "pixi.toml" in finding.message
    assert str(finding.interpreter) in finding.suggestion
    assert finding.project_command == command
    # A recorded ModuleNotFoundError is explained the same way.
    explained = explain_import_failure(
        message=f"No module named '{ABSENT}'", project_root=Path.cwd()
    )
    assert isinstance(explained, InterpreterMismatch) and explained.missing == (ABSENT,)


def test_other_failures_are_not_explained_away() -> None:
    """A recorded failure from some other interpreter is not this mismatch."""
    _manifest()

    for message in ("No module named 'ginkgo'", "exit code 1", None):
        assert explain_import_failure(message=message, project_root=Path.cwd()) is None


@pytest.mark.parametrize("signal", ["env-var", "prefix"])
def test_inside_the_project_env_a_missing_import_is_a_missing_dependency(
    monkeypatch: pytest.MonkeyPatch, signal: str
) -> None:
    """Running from the manifest's own environment: right interpreter, short manifest.

    ``pixi run`` advice would be a no-op here, so the finding must never claim
    the interpreter is wrong. Either ``PIXI_PROJECT_MANIFEST`` names the
    manifest, or ``sys.prefix`` sits in its ``.pixi/envs``.
    """
    _outside_project_env(monkeypatch)
    manifest = _manifest()
    if signal == "env-var":
        _inside_project_env(monkeypatch, manifest=manifest)
    else:
        prefix = manifest.parent.resolve() / ".pixi" / "envs" / "default"
        prefix.mkdir(parents=True)
        monkeypatch.setattr("ginkgo.envs.interpreter.sys.prefix", str(prefix))
    _workflow(imports=f"import {ABSENT}\n")

    finding = _detect()

    assert isinstance(finding, MissingDependency)
    assert (finding.code, finding.missing) == ("missing_dependency", (ABSENT,))
    assert "pixi run" not in "\n".join(finding.hint_lines)
    assert "pixi install" in finding.suggestion and "pixi.toml" in finding.suggestion
    explained = explain_import_failure(
        message=f"No module named '{ABSENT}'", project_root=Path.cwd()
    )
    assert isinstance(explained, MissingDependency)


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


def _failure(
    *,
    error: str,
    kind: str = "python",
    env_label: str = "local",
    label: str = "load_frame",
    ignored: bool = False,
) -> FailureDetails:
    """One failed task, defaulting to the in-process case."""
    return FailureDetails(
        task_label=label,
        exit_code=1,
        log_path=None,
        log_tail=[],
        error=error,
        failure_kind="import_error",
        task_kind=kind,
        env_label=env_label,
        ignored=ignored,
    )


def _failures_text(details: list[FailureDetails]) -> str:
    """Render a run's failure panels and return their plain text."""
    renderer = _renderer()
    renderer._console.print(renderer.render_failure_details(details))
    return renderer._console.file.getvalue()


class TestRenderedFailure:
    @pytest.mark.parametrize(
        ("manifest", "kind", "env_label", "hinted"),
        [
            (True, "python", "local", True),
            # A shell body ran in a subprocess, where this interpreter is not the issue.
            (True, "shell", "local", False),
            (True, "python", "analysis_tools", False),
            (False, "python", "local", False),
        ],
    )
    def test_the_hint_goes_only_to_an_in_process_import_failure(
        self, monkeypatch: pytest.MonkeyPatch, manifest: bool, kind: str, env_label: str, hinted
    ) -> None:
        _outside_project_env(monkeypatch)
        if manifest:
            _manifest()
        failure = _failure(error=f"No module named '{ABSENT}'", kind=kind, env_label=env_label)

        # However many tasks failed, the hint is rendered once.
        text = _failures_text([failure] * 3)

        assert ("cannot import: " + ABSENT in text) is hinted
        assert text.count("Try: pixi run run") == int(hinted)

    def test_the_hinted_failure_keeps_its_panel_past_the_ignored_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A keep-going run caps its panels; the hint must not be capped away."""
        _outside_project_env(monkeypatch)
        _manifest()
        shell_failures = [
            _failure(error="exit status 1", kind="shell", label=f"step_{index}", ignored=True)
            for index in range(11)
        ]

        text = _failures_text(
            [*shell_failures, _failure(error=f"No module named '{ABSENT}'", ignored=True)]
        )

        assert "Failure Details: load_frame" in text
        assert "cannot import: " + ABSENT in text
        assert "and 2 more" in text

    @pytest.mark.parametrize("manifest", [True, False])
    def test_a_top_level_import_failure_is_explained_inside_a_project(
        self, monkeypatch: pytest.MonkeyPatch, manifest: bool
    ) -> None:
        _outside_project_env(monkeypatch)
        if manifest:
            _manifest()
        stream = StringIO()

        report_failure(
            exc=ModuleNotFoundError(f"No module named '{ABSENT}'"),
            stream=stream,
            show_traceback=False,
        )

        assert ("Try: pixi run run" in stream.getvalue()) is manifest
        assert ("cannot import: " + ABSENT in stream.getvalue()) is manifest
