"""Tests for the phase 3 ``Out[...]`` ergonomics (issue #307).

Covers automatic parent-directory creation, return-value inference from
``Out[...]`` parameters, named ``.output["name"]`` access, and inferring
``shell(output=...)`` from declared ``Out[...]`` parameters. Builds on the
``Out[...]`` marker itself, tested in ``test_out_annotation.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, shell, task
from tests.conftest import EventCollector


# ---------------------------------------------------------------------------
# Module-level tasks (importable, as ``@task`` requires for process execution)
# ---------------------------------------------------------------------------


@task()
def write_nested(payload: str, dest: Out[file]):
    """Writes without creating its own parent directories."""
    Path(dest).write_text(payload, encoding="utf-8")


@task(kind="shell")
def shell_write_nested(payload: str, dest: Out[file]):
    return shell(cmd=f"echo -n '{payload}' > {dest}")


@task()
def write_single_inferred(payload: str, dest: Out[file]):
    Path(dest).write_text(payload, encoding="utf-8")


@task()
def write_multi_inferred(payload: str, dest: Out[file], check: Out[file]):
    Path(dest).write_text(payload, encoding="utf-8")
    Path(check).write_text(f"checked:{payload}", encoding="utf-8")


@task()
def write_list_inferred(payload: str, parts: Out[list[file]]):
    for part in parts:
        Path(part).write_text(payload, encoding="utf-8")


@task()
def write_optional_inferred(payload: str | None, dest: Out[file | None]):
    if dest is None:
        return
    Path(dest).write_text(payload or "", encoding="utf-8")


@task()
def explicit_none_return(payload: str, dest: Out[file]) -> None:
    Path(dest).write_text(payload, encoding="utf-8")


@task()
def non_none_under_inference(payload: str, dest: Out[file]):
    Path(dest).write_text(payload, encoding="utf-8")
    return "not none"


@task()
def consume_file(f: file) -> str:
    return Path(f).read_text(encoding="utf-8")


@task(kind="shell")
def shell_inferred_single(payload: str, dest: Out[file]):
    return shell(cmd=f"echo -n '{payload}' > {dest}")


@task(kind="shell")
def shell_inferred_multi(payload: str, dest: Out[file], check: Out[file]):
    return shell(cmd=f"echo -n '{payload}' > {dest} && echo -n 'checked' > {check}")


@task(kind="shell")
def shell_explicit_output_matching(payload: str, dest: Out[file]):
    return shell(cmd=f"echo -n '{payload}' > {dest}", output=dest)


@task(kind="shell")
def shell_explicit_output_mismatch(payload: str, dest: Out[file], other: str):
    return shell(cmd=f"echo -n '{payload}' > {dest}", output=other)


@task(kind="shell")
def shell_no_out_no_output(payload: str):
    return shell(cmd=f"echo -n '{payload}'")


@task()
def named_access_producer(payload: str, dest: Out[file], check: Out[file]) -> file:
    Path(dest).write_text(payload, encoding="utf-8")
    Path(check).write_text(f"checked:{payload}", encoding="utf-8")
    return file(dest)


@task()
def named_access_reader(path: file) -> str:
    return Path(path).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Parent directory auto-creation
# ---------------------------------------------------------------------------


class TestOutParentDirCreation:
    def test_python_task_parent_dirs_are_created(self, tmp_path: Path):
        dest = tmp_path / "results" / "deep" / "nested" / "x.txt"
        assert not dest.parent.exists()

        result = evaluate(write_nested(payload="hi", dest=str(dest)))
        assert Path(str(result)).read_text(encoding="utf-8") == "hi"

    def test_shell_task_parent_dirs_are_created(self, tmp_path: Path):
        dest = tmp_path / "results" / "deep" / "nested" / "shell.txt"
        assert not dest.parent.exists()

        result = evaluate(shell_write_nested(payload="ho", dest=str(dest)))
        assert Path(str(result)).read_text(encoding="utf-8") == "ho"

    def test_dry_run_does_not_create_directories(self, tmp_path: Path):
        from ginkgo.core.expr import record_constructed_calls
        from ginkgo.runtime.dry_run import build_dry_run_plan
        from ginkgo.runtime.evaluator import ConcurrentEvaluator

        dest = tmp_path / "should" / "not" / "exist.txt"
        with record_constructed_calls() as calls:
            expr = write_nested(payload="hi", dest=str(dest))
        evaluator = ConcurrentEvaluator(constructed_calls=tuple(calls))
        evaluator.build_and_validate(expr)
        build_dry_run_plan(evaluator=evaluator, workflow_label="test")
        assert not dest.parent.exists()


# ---------------------------------------------------------------------------
# 2a. Inferred return value
# ---------------------------------------------------------------------------


class TestInferredReturn:
    def test_single_out_param_infers_file_return(self, tmp_path: Path):
        dest = tmp_path / "single.txt"
        result = evaluate(write_single_inferred(payload="v", dest=str(dest)))
        assert isinstance(result, file)
        assert Path(str(result)).read_text(encoding="utf-8") == "v"

    def test_multiple_out_params_infer_tuple_return(self, tmp_path: Path):
        dest = tmp_path / "d.txt"
        check = tmp_path / "c.txt"
        result = evaluate(write_multi_inferred(payload="v", dest=str(dest), check=str(check)))
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(item, file) for item in result)
        assert str(result[0]) == str(dest)
        assert str(result[1]) == str(check)

    def test_list_out_param_infers_list_return(self, tmp_path: Path):
        parts = [str(tmp_path / f"p{i}.txt") for i in range(3)]
        result = evaluate(write_list_inferred(payload="v", parts=parts))
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(item, file) for item in result)

    def test_absent_optional_out_infers_none(self, tmp_path: Path):
        result = evaluate(write_optional_inferred(payload=None, dest=None))
        assert result is None

    def test_present_optional_out_infers_file(self, tmp_path: Path):
        dest = tmp_path / "opt.txt"
        result = evaluate(write_optional_inferred(payload="present", dest=str(dest)))
        assert isinstance(result, file)
        assert Path(str(result)).read_text(encoding="utf-8") == "present"

    def test_explicit_none_return_is_not_inferred(self, tmp_path: Path):
        dest = tmp_path / "explicit_none.txt"
        result = evaluate(explicit_none_return(payload="v", dest=str(dest)))
        assert result is None
        # The Out[...] path was still written and checked normally.
        assert dest.read_text(encoding="utf-8") == "v"

    def test_non_none_return_under_inference_raises(self, tmp_path: Path):
        dest = tmp_path / "bad_return.txt"
        with pytest.raises(TypeError, match="returned .*instead of None"):
            evaluate(non_none_under_inference(payload="v", dest=str(dest)))

    def test_downstream_consumer_gets_content_tracked_file(self, tmp_path: Path):
        dest = tmp_path / "producer.txt"
        result = evaluate(
            consume_file(f=write_single_inferred(payload="downstream-value", dest=str(dest)))
        )
        assert result == "downstream-value"

    def test_second_run_is_a_cache_hit(self, tmp_path: Path):
        dest = tmp_path / "cache_me.txt"
        evaluate(write_single_inferred(payload="v1", dest=str(dest)))

        collector = EventCollector()
        result = evaluate(
            write_single_inferred(payload="v1", dest=str(dest)), event_bus=collector.bus
        )
        assert collector.cached()
        assert isinstance(result, file)

    def test_deleting_output_forces_rerun_and_restores_it(self, tmp_path: Path):
        """An inferred ``-> file``-shaped return restores content on a cache hit.

        Unlike a bare ``Out[...]`` path (checked for existence only — see
        ``test_out_annotation.TestOutEndToEndCaching``), the *inferred*
        return is a real ``file`` value, so the same artifact-restoration
        machinery an explicit ``-> file`` return gets heals it back onto
        disk from the artifact store on a cache hit, rather than forcing a
        re-run.
        """
        dest = tmp_path / "restore_me.txt"
        evaluate(write_single_inferred(payload="v1", dest=str(dest)))

        dest.unlink()
        assert not dest.exists()

        collector = EventCollector()
        result = evaluate(
            write_single_inferred(payload="v1", dest=str(dest)), event_bus=collector.bus
        )
        assert collector.cached()
        assert dest.exists()
        assert Path(str(result)).read_text(encoding="utf-8") == "v1"


# ---------------------------------------------------------------------------
# 2b. Named ``.output["name"]`` access
# ---------------------------------------------------------------------------


class TestNamedOutputAccess:
    def test_named_access_on_single_call(self, tmp_path: Path):
        dest = tmp_path / "n1.txt"
        check = tmp_path / "n1.check.txt"
        call = named_access_producer(payload="v", dest=str(dest), check=str(check))

        check_value = call.output["check"]
        result = evaluate(named_access_reader(path=check_value))
        assert result == "checked:v"

    def test_named_access_is_not_the_return_value(self, tmp_path: Path):
        """``.output["dest"]`` resolves the arg, independent of what the task returns."""
        dest = tmp_path / "n2.txt"
        check = tmp_path / "n2.check.txt"
        call = named_access_producer(payload="v2", dest=str(dest), check=str(check))

        dest_value = call.output["dest"]
        result = evaluate(named_access_reader(path=dest_value))
        assert result == "v2"

    def test_named_access_on_map_result(self, tmp_path: Path):
        dests = [str(tmp_path / f"m{i}.txt") for i in range(3)]
        checks = [str(tmp_path / f"m{i}.check.txt") for i in range(3)]
        calls = named_access_producer().map(payload=["a", "b", "c"], dest=dests, check=checks)
        check_list = calls.output["check"]
        paths = evaluate(check_list)
        assert [str(p) for p in paths] == checks

        contents = [Path(p).read_text(encoding="utf-8") for p in paths]
        assert contents == ["checked:a", "checked:b", "checked:c"]

    def test_bad_name_raises_clear_error(self, tmp_path: Path):
        dest = tmp_path / "n3.txt"
        check = tmp_path / "n3.check.txt"
        call = named_access_producer(payload="v", dest=str(dest), check=str(check))
        with pytest.raises(KeyError, match="no Out\\[\\.\\.\\.\\] parameter named 'bogus'"):
            call.output["bogus"]

    def test_integer_indexing_still_works(self, tmp_path: Path):
        dest = tmp_path / "n4.txt"
        check = tmp_path / "n4.check.txt"
        call = write_multi_inferred(payload="v", dest=str(dest), check=str(check))
        first = evaluate(call.output[0])
        assert str(first) == str(dest)


# ---------------------------------------------------------------------------
# 3. Inferred ``shell(output=...)``
# ---------------------------------------------------------------------------


class TestShellOutputInference:
    def test_shell_infers_output_single(self, tmp_path: Path):
        dest = tmp_path / "shell_single.txt"
        result = evaluate(shell_inferred_single(payload="v", dest=str(dest)))
        assert Path(str(result)).read_text(encoding="utf-8") == "v"

    def test_shell_infers_output_multi(self, tmp_path: Path):
        dest = tmp_path / "shell_multi.txt"
        check = tmp_path / "shell_multi_check.txt"
        result = evaluate(shell_inferred_multi(payload="v", dest=str(dest), check=str(check)))
        assert isinstance(result, tuple)
        assert Path(str(result[0])).read_text(encoding="utf-8") == "v"
        assert Path(str(result[1])).read_text(encoding="utf-8") == "checked"

    def test_shell_explicit_output_matching_out_paths_ok(self, tmp_path: Path):
        dest = tmp_path / "shell_explicit.txt"
        result = evaluate(shell_explicit_output_matching(payload="v", dest=str(dest)))
        assert Path(str(result)).read_text(encoding="utf-8") == "v"

    def test_shell_explicit_output_mismatch_errors(self, tmp_path: Path):
        dest = tmp_path / "shell_mismatch_dest.txt"
        other = str(tmp_path / "shell_mismatch_other.txt")
        with pytest.raises(ValueError, match="Out\\[\\.\\.\\.\\] parameters"):
            evaluate(shell_explicit_output_mismatch(payload="v", dest=str(dest), other=other))

    def test_shell_with_no_out_params_and_no_output_still_errors(self):
        with pytest.raises(ValueError, match="output"):
            evaluate(shell_no_out_no_output(payload="v"))
