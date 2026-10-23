"""Tests for how the CLI reports a failure that reached the top level."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from ginkgo.cli.app import main
from ginkgo.cli.errors import UsageError, report_failure
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
        # The escape hatch is offered, but finding the line must not need it.
        assert "GINKGO_TRACEBACK=1" in captured
        assert "Traceback" not in captured

    @pytest.mark.parametrize("ask", ["env", "verbose"])
    def test_a_requested_traceback_is_added(self, capsys, monkeypatch, ask: str) -> None:
        path = _write("wf_nameerror.py", NAME_ERROR_WORKFLOW)
        if ask == "env":
            monkeypatch.setenv("GINKGO_TRACEBACK", "1")

        main(["run", str(path), "--dry-run", *(["--verbose"] if ask == "verbose" else [])])

        captured = capsys.readouterr().err
        assert "Traceback" in captured and "wf_nameerror.py" in captured


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


class TestRequestedTracebackAlwaysWins:
    """The flag overrides the default judgement, so nothing may veto it."""

    def test_a_failure_with_no_user_frame_still_gets_its_traceback(
        self,
        capsys,
        monkeypatch,
    ) -> None:
        monkeypatch.setenv("GINKGO_TRACEBACK", "1")

        status = main(["run", "definitely_not_here.py", "--dry-run"])

        captured = capsys.readouterr().err
        assert status == 1
        assert "Traceback" in captured
        assert "FileNotFoundError" in captured

    def test_a_ginkgo_error_still_gets_its_traceback(self) -> None:
        stream = io.StringIO()
        try:
            raise GinkgoError("deliberate")
        except GinkgoError as exc:
            status = report_failure(exc=exc, stream=stream, show_traceback=True)

        printed = stream.getvalue()
        assert status == 1
        assert "✖ deliberate" in printed
        assert "Traceback" in printed

    @pytest.mark.parametrize(
        ("exc", "status"),
        [
            # The hint rides with the location; the flag works with or without it.
            (GinkgoError("deliberate"), 1),
            # A path in the message must stay whole, so it can be copied.
            (GinkgoError("No such workflow: " + "/deep" * 60 + "/flow.py"), 1),
            (UsageError("say which"), 2),
        ],
    )
    def test_a_bare_message_is_one_line(self, exc: Exception, status: int) -> None:
        stream = io.StringIO()
        assert report_failure(exc=exc, stream=stream, show_traceback=False) == status
        assert stream.getvalue().splitlines() == [f"✖ {exc}"]


class TestInterruptAndExit:
    def test_keyboard_interrupt_is_not_a_crash(self, capsys, monkeypatch) -> None:
        def interrupt(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr("ginkgo.cli.app.command_plugins", interrupt)

        status = main(["plugins"])

        captured = capsys.readouterr().err
        assert status == 130
        assert "Interrupted" in captured
        assert "✖" not in captured
        assert "Traceback" not in captured

    def test_system_exit_propagates(self, monkeypatch) -> None:
        def exit_now(*args, **kwargs):
            raise SystemExit(3)

        monkeypatch.setattr("ginkgo.cli.app.command_plugins", exit_now)

        with pytest.raises(SystemExit) as excinfo:
            main(["plugins"])

        assert excinfo.value.code == 3


def _raise_from_user_code() -> ValueError:
    """Raise and return a ``ValueError`` from a function defined in ``helper.py``."""
    path = _write("helper.py", "def boom():\n    raise ValueError('inner')\n")
    namespace: dict[str, object] = {}
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)
    try:
        namespace["boom"]()  # type: ignore[operator]
    except ValueError as exc:
        return exc
    raise AssertionError("boom did not raise")


class TestFailureLocation:
    def test_a_failure_with_no_traceback_has_no_location(self) -> None:
        assert failure_location(GinkgoError("never raised")) is None

    def test_the_innermost_user_frame_wins(self) -> None:
        located = failure_location(_raise_from_user_code())

        assert located is not None
        assert (located.function, located.path.name) == ("boom", "helper.py")

    def test_a_cause_is_followed_when_the_re_raise_has_no_user_frame(self) -> None:
        """Ginkgo re-raises some failures; the user's line must survive that."""
        wrapper = RuntimeError("re-raised inside ginkgo")
        wrapper.__cause__ = _raise_from_user_code()

        located = failure_location(wrapper)

        assert located is not None
        assert located.function == "boom"
