"""Tests for machine-readable failure classification (issue #139)."""

import pytest

import ginkgo

from ginkgo.envs.container import ContainerPrepareError
from ginkgo.runtime.task_runners import classify_failure
from ginkgo.runtime.task_runners.shell import ShellTaskError
from ginkgo.runtime.task_validation import DeclaredOutputNotWrittenError


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
        # Whether a failure is retried is the task's retry policy, which the
        # retrying/failed events already show; the summary makes no claim.
        assert set(failure) == {"kind", "message", "code"}

    @pytest.mark.parametrize(
        ("exc", "message"),
        [
            # N5: a bare 'PRODUCT_TAG' does not say a key was missing.
            (KeyError("PRODUCT_TAG"), "KeyError: 'PRODUCT_TAG'"),
            (DeclaredOutputNotWrittenError("t.out was not written"), "t.out was not written"),
        ],
    )
    def test_the_message(self, exc: Exception, message: str) -> None:
        assert classify_failure(exc=exc)["message"] == message

    def test_a_key_error_from_a_worker_keeps_its_message(self) -> None:
        """The KeyError rebuilt from a worker must not quote its key twice."""
        with pytest.raises(KeyError) as raised:
            ginkgo.evaluate(read_product_tag())

        assert str(raised.value) == "'PRODUCT_TAG'"


@ginkgo.task()
def read_product_tag() -> str:
    return {}["PRODUCT_TAG"]


class TestFrameworkClassification:
    @pytest.mark.parametrize(
        ("exc", "kind"),
        [
            (ContainerPrepareError(image="img:1", output="bad image"), "env_mismatch"),
            (ModuleNotFoundError("no module named x"), "import_error"),
            (ImportError("cannot import name"), "import_error"),
            (
                ShellTaskError(
                    task_kind="shell",
                    task_name="run_bwa",
                    cmd="bwa",
                    exit_code=1,
                    output="failed",
                    log=None,
                ),
                "shell_command_error",
            ),
            (PermissionError("denied"), "invalid_path"),
            (FileNotFoundError("missing.txt"), "missing_input"),
            (FileNotFoundError("task did not create out.txt"), "output_validation_error"),
            (
                DeclaredOutputNotWrittenError("t.out is declared `Out[file]` but 'o' was not"),
                "output_validation_error",
            ),
        ],
    )
    def test_framework_exceptions_keep_their_kind(self, exc: Exception, kind: str) -> None:
        assert classify_failure(exc=exc)["kind"] == kind

    def test_a_declared_kind_classifies_itself_but_a_name_alone_does_not(self) -> None:
        class PluginEnvError(Exception):
            failure_kind = "env_mismatch"

        assert classify_failure(exc=PluginEnvError("failed"))["kind"] == "env_mismatch"
        lookalike = type("EnvResolutionError", (Exception,), {})("failed")
        assert classify_failure(exc=lookalike)["kind"] == "user_code_error"
