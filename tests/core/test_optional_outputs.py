"""Optional declared outputs for driver tasks.

Covers the contract from issue #98: a path wrapped in ``optional()`` may be
absent after execution without failing the task, resolves to ``None`` in the
result, and reproduces faithfully from cache in both states.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import ginkgo
from ginkgo import Out, file, folder, optional, shell, task
from ginkgo.core.optional import OptionalOutput
from ginkgo.core.types import unwrap_optional_annotation
from ginkgo.runtime.artifacts.output_index import output_summary
from ginkgo.runtime.artifacts.remote_arg_transfer import _stage_encoded_value
from ginkgo.runtime.artifacts.value_codec import decode_value, encode_value
from ginkgo.runtime.task_runners.shell import (
    iter_output_values,
    iter_required_output_values,
    resolve_output_value,
)
from ginkgo.runtime.task_validation import TaskValidator


@task(kind="shell")
def emit_counted(*, emit_extra: bool) -> tuple[file, file | None]:
    """Write the required output, the optional one on demand, and count the run."""
    extra = " && echo extra > results/extra.txt" if emit_extra else ""
    return shell(
        cmd=(
            "mkdir -p results && echo main > results/main.txt"
            f"{extra} && echo ran >> results/executions.txt"
        ),
        output=("results/main.txt", optional("results/extra.txt")),
    )


@task(kind="shell")
def emit_missing_required() -> tuple[file, file | None]:
    """Never write the required output — the task must still fail."""
    return shell(
        cmd="mkdir -p results && echo extra > results/extra.txt",
        output=("results/required.txt", optional("results/extra.txt")),
    )


@task(kind="shell")
def emit_only_optional() -> file | None:
    """A lone optional output, declared without a container."""
    return shell(cmd="true", output=optional("results/lonely.txt"))


@task(kind="shell")
def emit_optional_folder() -> tuple[file, folder | None]:
    """A heterogeneous tuple whose optional element is a folder, not a file."""
    return shell(
        cmd=(
            "mkdir -p results/tree && echo main > results/main.txt "
            "&& echo x > results/tree/x.txt && echo ran >> results/executions.txt"
        ),
        output=("results/main.txt", optional("results/tree")),
    )


@task(kind="shell")
def emit_per_name(*, name: str) -> tuple[file, file | None]:
    """Produce the optional output for one branch only, to vary presence."""
    extra = f" && echo x > results/{name}_extra.txt" if name == "b" else ""
    return shell(
        cmd=(
            f"mkdir -p results && echo {name} > results/{name}.txt{extra} "
            "&& echo ran >> results/executions.txt"
        ),
        output=(f"results/{name}.txt", optional(f"results/{name}_extra.txt")),
    )


@task()
def describe(*, main: file, extra: file | None) -> str:
    """Consume both outputs as separate arguments, branching on absence."""
    return f"{Path(main).name}:{'absent' if extra is None else Path(extra).name}"


@task()
def consume_pair(*, pair: tuple[file, file | None]) -> str:
    """Receive a heterogeneous tuple as a task input, exercising cache hashing."""
    return f"{Path(pair[0]).name}:{'absent' if pair[1] is None else Path(pair[1]).name}"


@task()
def _write_without_return(output_path: Out[file]) -> file:
    Path(output_path).write_text("x", encoding="utf-8")


def _execution_count() -> int:
    """Return how many times a counting task body has actually run.

    Equal results across two evaluations hold whether the task was cached or
    re-executed; the task appends a line per execution, so this counts real runs.
    """
    counter = Path("results/executions.txt")
    return len(counter.read_text(encoding="utf-8").splitlines()) if counter.is_file() else 0


class TestOptionalConstruction:
    def test_wraps_a_path_string(self) -> None:
        assert optional("results/x.txt") == OptionalOutput(payload="results/x.txt")

    @pytest.mark.parametrize(
        ("payload", "match"),
        [
            (optional("results/x.txt"), "must not be nested"),
            (Path("results/x.txt"), "not a Path"),
            (None, "takes a path string"),
            (42, "takes a path string"),
            (["results/x.txt"], "takes a path string"),
        ],
    )
    def test_rejects_anything_but_a_path_string(self, payload: Any, match: str) -> None:
        with pytest.raises(TypeError, match=match):
            optional(payload)


class TestDeclarationWalking:
    """Required and optional paths are distinguished, but both are cleaned."""

    def test_walks(self, tmp_path: Path) -> None:
        declared = ("a.txt", optional("b.txt"))
        assert iter_output_values(declared) == [Path("a.txt"), Path("b.txt")]
        assert iter_required_output_values(declared) == [Path("a.txt")]
        assert iter_required_output_values(optional("b.txt")) == []

    def test_resolve_keeps_the_shape_and_drops_only_absent_optionals(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")
        present = str(tmp_path / "a.txt")

        assert resolve_output_value((present, optional(str(tmp_path / "missing.txt")))) == (
            present,
            None,
        )
        assert resolve_output_value([optional(present)]) == [present]


class TestOptionalAnnotation:
    """``X | None`` splits into its inner type and a nullability flag."""

    def test_unwraps_only_an_optional(self) -> None:
        assert unwrap_optional_annotation(file | None) == (file, True)
        assert unwrap_optional_annotation(file) == (file, False)
        assert unwrap_optional_annotation(file | str) == (file | str, False)

    def test_none_is_only_admitted_where_the_annotation_says_so(self) -> None:
        validator = TaskValidator()
        with pytest.raises(TypeError, match="does not admit None"):
            validator.validate_annotated_value(annotation=file, value=None, label="demo.out")
        # `doctor` and `inspect workflow` pass None for unsupplied parameters,
        # and non-path annotations have never validated it.
        validator.validate_annotated_value(annotation=str, value=None, label="demo.r")

    def test_a_python_task_without_a_return_is_told_to_return_its_output(self) -> None:
        """K4-2: `optional(...)` is a driver-kind remedy, not this one."""
        with pytest.raises(TypeError) as excinfo:
            TaskValidator().validate_return_value(task_def=_write_without_return, value=None)
        message = str(excinfo.value)
        assert "`return output_path`" in message
        assert "optional" not in message


class TestOptionalExecution:
    def test_presence_resolves_reaches_consumers_and_caches_in_both_states(self) -> None:
        """Absence is a success, and the two states key a consumer differently.

        Were absence hashed as plain null — or the two states to collide — the
        present-state consumer would be served the absent-state answer.
        """
        absent = emit_counted(emit_extra=False)
        assert ginkgo.evaluate(
            (
                describe(main=absent.output[0], extra=absent.output[1]),
                consume_pair(pair=(absent.output[0], absent.output[1])),
            )
        ) == ("main.txt:absent", "main.txt:absent")
        assert ginkgo.evaluate(emit_counted(emit_extra=False))[1] is None
        assert _execution_count() == 1, "absent optional re-executed instead of hitting cache"

        present = emit_counted(emit_extra=True)
        assert (
            ginkgo.evaluate(consume_pair(pair=(present.output[0], present.output[1])))
            == "main.txt:extra.txt"
        )
        assert _execution_count() == 2

        Path("results/extra.txt").unlink()
        restored = ginkgo.evaluate(emit_counted(emit_extra=True))
        assert Path(restored[1]).read_text(encoding="utf-8").strip() == "extra"
        assert _execution_count() == 2, "restoring the optional output re-ran the task"

    def test_a_missing_required_output_still_fails(self) -> None:
        assert ginkgo.evaluate(emit_only_optional()) is None
        with pytest.raises(FileNotFoundError, match="results/required.txt"):
            ginkgo.evaluate(emit_missing_required())

    def test_optional_folder_output_is_cached(self) -> None:
        """A `folder | None` element must not be stored as if it were a file.

        Applying the tuple's first annotation to every element made the folder
        arrive annotated `file`, so it was silently never stored and the task
        re-ran forever.
        """
        ginkgo.evaluate(emit_optional_folder())
        ginkgo.evaluate(emit_optional_folder())

        assert _execution_count() == 1, "optional folder output never cached"

    def test_fan_out_branches_carry_and_cache_independent_presence(self) -> None:
        for _ in range(2):
            result = ginkgo.evaluate(emit_per_name().map(name=["a", "b", "c"]).output[1])
            assert result == [None, "results/b_extra.txt", None]

        assert _execution_count() == 3, "second fan-out re-executed instead of caching"


class TestOptionalTransportAndIndex:
    def test_none_round_trips_through_the_codec_and_remote_staging(self, tmp_path: Path) -> None:
        target = tmp_path / "a.txt"
        target.write_text("a", encoding="utf-8")
        encoded = encode_value((file(str(target)), None), base_dir=tmp_path)
        assert decode_value(encoded, base_dir=tmp_path) == (str(target), None)

        class NeverStore:
            def store(self, **kwargs: Any) -> Any:
                raise AssertionError("remote store called for an absent output")

        # Staging walks the encoded tree structurally, so None needs no store.
        staged = _stage_encoded_value(
            value=encode_value((None, None), base_dir=tmp_path), remote_store=NeverStore()
        )
        assert decode_value(staged, base_dir=tmp_path) == (None, None)

    def test_presence_is_explicit_in_the_output_index(self) -> None:
        absent = {"name": "return[1]", "type": "file", "optional": True}
        assert output_summary(file | None, None, name="return[1]") == [
            {**absent, "present": False}
        ]
        assert output_summary(file | None, file("results/x.txt"), name="return[1]") == [
            {**absent, "path": "results/x.txt", "present": True}
        ]
        assert output_summary(None, None) == []
