"""Tests for how the CLI reports a failure that reached the top level."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.app import main
from ginkgo.errors import GinkgoError, failure_location


def _write(name: str, body: str) -> Path:
    """Write *body* as a workflow module in the current directory."""
    path = Path(name)
    path.write_text(body.strip() + "\n", encoding="utf-8")
    return path


def _line_of(path: Path, needle: str) -> int:
    """Return the 1-based line number of the first line containing *needle*."""
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if needle in line:
            return index
    raise AssertionError(f"{needle!r} not found in {path}")


NAME_ERROR_WORKFLOW = """
from ginkgo import flow, task


@task("shell")
def greet(text: str) -> str:
    return f"echo {text}"


@flow
def main():
    return greet(text=undefined_symbol)
"""

BAD_PARAM_WORKFLOW = """
from ginkgo import flow, param, task

label = param("not-a-valid-name")


@task("shell")
def greet(text: str) -> str:
    return f"echo {text}"


@flow
def main():
    return greet(text=label)
"""


class TestUnexpectedFailures:
    """A mistake in the user's own code is reported with its location."""

    def test_flow_body_error_reports_the_file_and_line(self, capsys) -> None:
        path = _write("wf_nameerror.py", NAME_ERROR_WORKFLOW)

        status = main(["run", str(path), "--dry-run"])

        captured = capsys.readouterr().err
        assert status == 1
        assert "✖ name 'undefined_symbol' is not defined" in captured
        assert "NameError at " in captured
        assert f"{path.name}:{_line_of(path, 'undefined_symbol')} in main" in captured

    def test_location_is_reported_without_any_flag(self, capsys) -> None:
        """The escape hatch is offered, but finding the line must not need it."""
        path = _write("wf_nameerror.py", NAME_ERROR_WORKFLOW)

        main(["run", str(path), "--dry-run"])

        captured = capsys.readouterr().err
        assert "GINKGO_TRACEBACK=1" in captured
        assert "Traceback" not in captured

    def test_traceback_env_var_adds_the_traceback(self, capsys, monkeypatch) -> None:
        path = _write("wf_nameerror.py", NAME_ERROR_WORKFLOW)
        monkeypatch.setenv("GINKGO_TRACEBACK", "1")

        main(["run", str(path), "--dry-run"])

        captured = capsys.readouterr().err
        assert "Traceback" in captured
        assert "wf_nameerror.py" in captured

    def test_verbose_adds_the_traceback(self, capsys) -> None:
        path = _write("wf_nameerror.py", NAME_ERROR_WORKFLOW)

        main(["run", str(path), "--dry-run", "--verbose"])

        captured = capsys.readouterr().err
        assert "Traceback" in captured


class TestDeliberateFailures:
    """Ginkgo's own messages stay one clean line."""

    def test_ginkgo_error_raised_from_user_code_stays_a_bare_message(self, capsys) -> None:
        """A ``GinkgoError`` explains itself, even with user code on the stack."""
        path = _write("wf_param.py", BAD_PARAM_WORKFLOW)

        status = main(["run", str(path), "--dry-run"])

        captured = capsys.readouterr().err
        assert status == 1
        assert captured.startswith("✖ Invalid parameter name")
        assert "ParamError at" not in captured
        assert "Traceback" not in captured
        assert "GINKGO_TRACEBACK" not in captured

    def test_failure_from_a_ginkgo_check_stays_a_bare_message(self, capsys) -> None:
        status = main(["run", "definitely_not_here.py", "--dry-run"])

        captured = capsys.readouterr().err
        assert status == 1
        assert captured.startswith("✖ ")
        assert " at " not in captured
        assert "Traceback" not in captured
        assert "GINKGO_TRACEBACK" not in captured


class TestInterruptAndExit:
    def test_keyboard_interrupt_is_not_a_crash(self, capsys, monkeypatch) -> None:
        def interrupt(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr("ginkgo.cli.app.command_notebooks", interrupt)

        status = main(["notebooks"])

        captured = capsys.readouterr().err
        assert status == 130
        assert "Interrupted" in captured
        assert "✖" not in captured
        assert "Traceback" not in captured

    def test_system_exit_propagates(self, monkeypatch) -> None:
        def exit_now(*args, **kwargs):
            raise SystemExit(3)

        monkeypatch.setattr("ginkgo.cli.app.command_notebooks", exit_now)

        with pytest.raises(SystemExit) as excinfo:
            main(["notebooks"])

        assert excinfo.value.code == 3


class TestFailureLocation:
    def test_a_failure_with_no_traceback_has_no_location(self) -> None:
        assert failure_location(GinkgoError("never raised")) is None

    def test_the_innermost_user_frame_wins(self) -> None:
        path = _write("helper.py", "def boom():\n    raise ValueError('inner')\n")
        namespace: dict[str, object] = {}
        exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)

        try:
            namespace["boom"]()  # type: ignore[operator]
        except ValueError as exc:
            located = failure_location(exc)

        assert located is not None
        assert located.function == "boom"
        assert located.path.name == "helper.py"

    def test_a_cause_is_followed_when_the_re_raise_has_no_user_frame(self) -> None:
        """Ginkgo re-raises some failures; the user's line must survive that."""
        path = _write("helper.py", "def boom():\n    raise ValueError('inner')\n")
        namespace: dict[str, object] = {}
        exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)

        try:
            namespace["boom"]()  # type: ignore[operator]
        except ValueError as inner:
            wrapper = RuntimeError("re-raised inside ginkgo")
            wrapper.__cause__ = inner

        located = failure_location(wrapper)

        assert located is not None
        assert located.function == "boom"
