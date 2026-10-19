"""Tests for machine-readable failure classification (issue #139)."""

import pytest

from ginkgo.envs.container import ContainerPrepareError
from ginkgo.envs.pixi import PixiEnvNotFoundError
from ginkgo.runtime.task_runners import classify_failure
from ginkgo.runtime.task_runners.shell import ShellTaskError


class CustomAnalysisError(Exception):
    """Stand-in for a user-defined exception raised inside a task body."""


class TestUserCodeClassification:
    @pytest.mark.parametrize(
        "exc",
        [
            RuntimeError("boom"),
            KeyError("sample_id"),
            AssertionError("expected 3 rows"),
            CustomAnalysisError("bad input"),
            TypeError("unsupported operand"),
            ValueError("invalid literal"),
        ],
    )
    def test_task_body_exceptions_classify_as_user_code(self, exc: Exception) -> None:
        failure = classify_failure(exc=exc)
        assert failure["kind"] == "user_code_error"
        assert failure["code"] == exc.__class__.__name__
        assert failure["retryable"] is False


class TestFrameworkClassification:
    @pytest.mark.parametrize(
        ("exc", "kind"),
        [
            (PixiEnvNotFoundError(env="analysis", searched=None), "env_mismatch"),
            (ContainerPrepareError(image="img:1", output="bad image"), "env_mismatch"),
            (ModuleNotFoundError("no module named x"), "import_error"),
            (ImportError("cannot import name"), "import_error"),
            (
                ShellTaskError(
                    task_name="run_bwa", cmd="bwa", exit_code=1, output="failed", log=None
                ),
                "shell_command_error",
            ),
            (PermissionError("denied"), "invalid_path"),
            (FileNotFoundError("missing.txt"), "missing_input"),
            (FileNotFoundError("task did not create out.txt"), "output_validation_error"),
        ],
    )
    def test_framework_exceptions_keep_their_kind(self, exc: Exception, kind: str) -> None:
        assert classify_failure(exc=exc)["kind"] == kind

    def test_an_exception_declaring_its_kind_classifies_itself(self) -> None:
        class PluginEnvError(Exception):
            failure_kind = "env_mismatch"

        assert classify_failure(exc=PluginEnvError("failed"))["kind"] == "env_mismatch"

    def test_a_framework_sounding_name_alone_is_user_code(self) -> None:
        exc = type("EnvResolutionError", (Exception,), {})("failed")
        assert classify_failure(exc=exc)["kind"] == "user_code_error"
