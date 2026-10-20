"""Predicting a driver task's directive without running its body (R2)."""

from __future__ import annotations

import types
from pathlib import Path

import pytest

import ginkgo
from ginkgo import script, subworkflow
from ginkgo.core.hashing import hash_file
from ginkgo.core.script import ScriptDirective
from ginkgo.core.subworkflow import SubWorkflowDirective
from ginkgo.runtime.static_directive import static_directive

SCRIPTS = "scripts"


def literal_path(out: str):
    return script("scripts/s.py", output=out)


def formatted_path(name: str, out: str):
    """A docstring is allowed before the return."""
    return script(f"{SCRIPTS}/{name}.py", output=[out], log=f"logs/{name}.log")


def path_arithmetic(out: str):
    return ginkgo.script(Path(SCRIPTS).resolve() / "s.py", output=out)


def defaulted(out: str, name: str = "s"):
    return script(Path(SCRIPTS).joinpath(name).with_suffix(".py"), output=out)


def child(n: int):
    return subworkflow("child/flow.py", params={"n": n})


def two_statements(out: str):
    path = "scripts/s.py"
    return script(path, output=out)


def calls_user_code(out: str):
    return script(pick_script(), output=out)


# User code records its calls: the evaluator swallows any exception it raises.
CALLS: list[str] = []


def pick_script() -> str:
    CALLS.append("pick_script")
    return "scripts/s.py"


def real_script(out: str):
    return ginkgo.script("scripts/s.py", output=out)


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / SCRIPTS).mkdir()
    (tmp_path / SCRIPTS / "s.py").write_text("print('hi')\n")
    return tmp_path


def _predict(fn, **args):
    directive_type = SubWorkflowDirective if fn is child else ScriptDirective
    return static_directive(fn=fn, resolved_args=args, directive_type=directive_type)


class TestEvaluatedShapes:
    @pytest.mark.parametrize(
        "fn", [literal_path, real_script, formatted_path, path_arithmetic, defaulted]
    )
    def test_script_directive_is_predicted(self, project: Path, fn) -> None:
        args = {"out": "o.txt"} | ({"name": "s"} if fn is formatted_path else {})

        directive = _predict(fn, **args)

        assert isinstance(directive, ScriptDirective)
        assert directive.source_hash == hash_file(project / SCRIPTS / "s.py")

    def test_formatted_arguments_are_evaluated(self, project: Path) -> None:
        directive = _predict(formatted_path, name="s", out="o.txt")

        assert directive.output == ["o.txt"]
        assert directive.log == "logs/s.log"

    def test_subworkflow_directive_is_predicted(self, project: Path) -> None:
        (project / "child").mkdir()
        (project / "child" / "flow.py").write_text("")

        directive = _predict(child, n=3)

        assert isinstance(directive, SubWorkflowDirective)
        assert directive.params == {"n": 3}


class TestFallback:
    @pytest.mark.parametrize("fn", [two_statements, calls_user_code])
    def test_unsupported_bodies_are_not_evaluated(self, project: Path, fn) -> None:
        assert _predict(fn, out="o.txt") is None
        assert CALLS == []

    def test_a_user_function_named_script_is_not_called(self, project: Path) -> None:
        def fake_script(path, **_):
            CALLS.append("script")
            return script(path, **_)

        shadowed = types.FunctionType(
            literal_path.__code__, {**literal_path.__globals__, "script": fake_script}
        )

        assert _predict(shadowed, out="o.txt") is None
        assert CALLS == []

    def test_a_missing_script_is_not_predicted(self, project: Path) -> None:
        (project / SCRIPTS / "s.py").unlink()

        assert _predict(real_script, out="o.txt") is None

    def test_an_unresolved_parameter_is_not_looked_up_globally(self, project: Path) -> None:
        assert _predict(formatted_path, out="o.txt") is None
