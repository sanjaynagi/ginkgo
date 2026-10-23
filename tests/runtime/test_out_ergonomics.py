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
    return shell(cmd=f"printf '%s' '{payload}' > {dest}")


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


@task(kind="shell")
def shell_mixed_container_explicit_return(
    dest: Out[file], parts: Out[list[file]]
) -> tuple[file, list[file]]:
    targets = " ".join([dest, *parts])
    return shell(cmd=f"for p in {targets}; do echo x > $p; done")


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
    return shell(cmd=f"printf '%s' '{payload}' > {dest}")


@task(kind="shell")
def shell_inferred_multi(payload: str, dest: Out[file], check: Out[file]):
    return shell(cmd=f"printf '%s' '{payload}' > {dest} && printf '%s' 'checked' > {check}")


@task(kind="shell")
def shell_explicit_output_matching(payload: str, dest: Out[file]):
    return shell(cmd=f"printf '%s' '{payload}' > {dest}", output=dest)


@task(kind="shell")
def shell_explicit_output_mismatch(payload: str, dest: Out[file], other: str):
    return shell(cmd=f"printf '%s' '{payload}' > {dest}", output=other)


@task(kind="shell")
def shell_no_out_no_output(payload: str):
    return shell(cmd=f"printf '%s' '{payload}'")


@task()
def named_access_producer(payload: str, dest: Out[file], check: Out[file]) -> file:
    Path(dest).write_text(payload, encoding="utf-8")
    Path(check).write_text(f"checked:{payload}", encoding="utf-8")
    return file(dest)


@task()
def named_access_reader(path: file) -> str:
    return Path(path).read_text(encoding="utf-8")


@pytest.mark.parametrize("make", [write_nested, shell_write_nested])
def test_parent_dirs_of_an_out_path_are_created(tmp_path: Path, make):
    dest = tmp_path / "results" / "deep" / "x.txt"
    result = evaluate(make(payload="hi", dest=str(dest)))
    assert Path(str(result)).read_text(encoding="utf-8") == "hi"


def test_dry_run_does_not_create_directories(tmp_path: Path):
    from ginkgo.runtime.dry_run import build_dry_run_plan
    from ginkgo.runtime.evaluator import ConcurrentEvaluator

    dest = tmp_path / "should" / "not" / "exist.txt"
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph(write_nested(payload="hi", dest=str(dest)))
    evaluator.validate_graph()
    build_dry_run_plan(evaluator=evaluator, workflow_label="test")
    assert not dest.parent.exists()


class TestInferredReturn:
    def test_the_return_is_inferred_from_the_out_params(self, tmp_path: Path):
        d, c = str(tmp_path / "d.txt"), str(tmp_path / "c.txt")
        parts = [str(tmp_path / f"p{i}.txt") for i in range(3)]
        result = evaluate(
            {
                "single": write_single_inferred(payload="v", dest=d),
                "multi": write_multi_inferred(payload="v", dest=d + ".m", check=c),
                "list": write_list_inferred(payload="v", parts=parts),
                "absent": write_optional_inferred(payload=None, dest=None),
                "present": write_optional_inferred(payload="p", dest=str(tmp_path / "o.txt")),
            }
        )

        assert isinstance(result["single"], file)
        assert Path(str(result["single"])).read_text(encoding="utf-8") == "v"
        assert isinstance(result["multi"], tuple)
        assert [str(item) for item in result["multi"]] == [d + ".m", c]
        assert isinstance(result["list"], list) and len(result["list"]) == 3
        assert all(isinstance(item, file) for item in [*result["multi"], *result["list"]])
        assert result["absent"] is None
        assert Path(str(result["present"])).read_text(encoding="utf-8") == "p"

    def test_explicit_none_return_is_not_inferred(self, tmp_path: Path):
        dest = tmp_path / "explicit_none.txt"
        assert evaluate(explicit_none_return(payload="v", dest=str(dest))) is None
        # The Out[...] path was still written and checked normally.
        assert dest.read_text(encoding="utf-8") == "v"

    def test_non_none_return_under_inference_raises(self, tmp_path: Path):
        with pytest.raises(TypeError, match="returned .*instead of None"):
            evaluate(non_none_under_inference(payload="v", dest=str(tmp_path / "bad.txt")))

    def test_an_inferred_file_feeds_downstream_and_is_restored_on_a_hit(self, tmp_path: Path):
        """The inferred return is a real ``file``, so a hit heals a deleted output."""
        dest = tmp_path / "producer.txt"

        def build():
            return consume_file(f=write_single_inferred(payload="value", dest=str(dest)))

        assert evaluate(build()) == "value"
        dest.unlink()

        collector = EventCollector()
        assert evaluate(build(), event_bus=collector.bus) == "value"
        assert collector.cached()
        assert dest.read_text(encoding="utf-8") == "value"


class TestNamedOutputAccess:
    def test_named_and_indexed_access_resolve_the_out_args(self, tmp_path: Path):
        """``.output["name"]`` resolves the argument, whatever the task returns."""
        call = named_access_producer(
            payload="v", dest=str(tmp_path / "n.txt"), check=str(tmp_path / "n.check.txt")
        )
        multi = write_multi_inferred(
            payload="v", dest=str(tmp_path / "i.txt"), check=str(tmp_path / "i.check.txt")
        )

        assert evaluate(
            [
                named_access_reader(path=call.output["check"]),
                named_access_reader(path=call.output["dest"]),
                multi.output[0],
            ]
        ) == ["checked:v", "v", file(str(tmp_path / "i.txt"))]

    def test_named_access_on_map_result(self, tmp_path: Path):
        dests = [str(tmp_path / f"m{i}.txt") for i in range(3)]
        checks = [str(tmp_path / f"m{i}.check.txt") for i in range(3)]
        calls = named_access_producer().map(payload=["a", "b", "c"], dest=dests, check=checks)

        paths = evaluate(calls.output["check"])

        assert [str(p) for p in paths] == checks
        assert [Path(p).read_text(encoding="utf-8") for p in paths] == [
            "checked:a",
            "checked:b",
            "checked:c",
        ]

    def test_bad_name_raises_clear_error(self, tmp_path: Path):
        call = named_access_producer(payload="v", dest="d.txt", check="c.txt")
        with pytest.raises(KeyError, match="no Out\\[\\.\\.\\.\\] parameter named 'bogus'"):
            call.output["bogus"]


class TestShellOutputInference:
    def test_shell_output_is_inferred_from_the_out_params(self, tmp_path: Path):
        single, multi, explicit = evaluate(
            [
                shell_inferred_single(payload="v", dest=str(tmp_path / "s.txt")),
                shell_inferred_multi(
                    payload="v", dest=str(tmp_path / "m.txt"), check=str(tmp_path / "c.txt")
                ),
                shell_explicit_output_matching(payload="v", dest=str(tmp_path / "e.txt")),
            ]
        )
        assert Path(str(single)).read_text(encoding="utf-8") == "v"
        assert [Path(str(p)).read_text(encoding="utf-8") for p in multi] == ["v", "checked"]
        assert Path(str(explicit)).read_text(encoding="utf-8") == "v"

    @pytest.mark.parametrize(
        ("build", "match"),
        [
            (
                lambda tmp: shell_explicit_output_mismatch(
                    payload="v", dest=str(tmp / "d.txt"), other=str(tmp / "o.txt")
                ),
                "Out\\[\\.\\.\\.\\] parameters",
            ),
            (
                lambda tmp: shell_mixed_container_explicit_return(
                    dest=str(tmp / "dest.txt"), parts=[str(tmp / "a.txt"), str(tmp / "b.txt")]
                ),
                "cannot be inferred unambiguously",
            ),
            (lambda tmp: shell_no_out_no_output(payload="v"), "output"),
        ],
        ids=["mismatch", "ambiguous", "no-output"],
    )
    def test_an_undeterminable_shell_output_errors(self, tmp_path: Path, build, match: str):
        with pytest.raises(ValueError, match=match):
            evaluate(build(tmp_path))
