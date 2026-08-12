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
from ginkgo import file, folder, optional, shell, task
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

# ---------------------------------------------------------------------------
# Task fixtures
# ---------------------------------------------------------------------------


@task(kind="shell")
def emit_with_optional(*, emit_extra: bool) -> tuple[file, file | None]:
    """Always write the required output; write the optional one on demand."""
    extra = " && echo extra > results/extra.txt" if emit_extra else ""
    return shell(
        cmd=f"mkdir -p results && echo main > results/main.txt{extra}",
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


@task()
def describe(*, main: file, extra: file | None) -> str:
    """Consume both outputs, branching on absence."""
    return f"{Path(main).name}:{'absent' if extra is None else Path(extra).name}"


# ---------------------------------------------------------------------------
# optional() construction
# ---------------------------------------------------------------------------


class TestOptionalConstruction:
    """``optional()`` accepts a path declaration and rejects everything else."""

    def test_wraps_a_path_string(self) -> None:
        assert optional("results/x.txt") == OptionalOutput(payload="results/x.txt")

    def test_rejects_nesting(self) -> None:
        with pytest.raises(TypeError, match="must not be nested"):
            optional(optional("results/x.txt"))

    def test_rejects_path_object(self) -> None:
        with pytest.raises(TypeError, match="not a Path"):
            optional(Path("results/x.txt"))

    @pytest.mark.parametrize("payload", [None, 42, ["results/x.txt"]])
    def test_rejects_non_path_payload(self, payload: Any) -> None:
        with pytest.raises(TypeError, match="takes a path string or an asset"):
            optional(payload)


# ---------------------------------------------------------------------------
# Declaration walking
# ---------------------------------------------------------------------------


class TestDeclarationWalking:
    """Required and optional paths are distinguished, but both are cleaned."""

    def test_cleanup_walk_includes_optional_paths(self) -> None:
        declared = ("a.txt", optional("b.txt"))
        assert iter_output_values(declared) == [Path("a.txt"), Path("b.txt")]

    def test_required_walk_excludes_optional_paths(self) -> None:
        declared = ("a.txt", optional("b.txt"))
        assert iter_required_output_values(declared) == [Path("a.txt")]

    def test_lone_optional_has_no_required_paths(self) -> None:
        assert iter_required_output_values(optional("b.txt")) == []

    def test_resolve_preserves_tuple_shape(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")
        declared = (str(tmp_path / "a.txt"), optional(str(tmp_path / "missing.txt")))
        resolved = resolve_output_value(declared)
        assert isinstance(resolved, tuple)
        assert resolved == (str(tmp_path / "a.txt"), None)

    def test_resolve_keeps_present_optional(self, tmp_path: Path) -> None:
        present = tmp_path / "b.txt"
        present.write_text("b", encoding="utf-8")
        assert resolve_output_value([optional(str(present))]) == [str(present)]


# ---------------------------------------------------------------------------
# Annotation handling
# ---------------------------------------------------------------------------


class TestOptionalAnnotation:
    """``X | None`` splits into its inner type and a nullability flag."""

    def test_unwraps_optional_file(self) -> None:
        assert unwrap_optional_annotation(file | None) == (file, True)

    def test_leaves_plain_annotation_alone(self) -> None:
        assert unwrap_optional_annotation(file) == (file, False)

    def test_leaves_non_optional_union_alone(self) -> None:
        annotation = file | str
        assert unwrap_optional_annotation(annotation) == (annotation, False)

    def test_none_under_bare_file_is_rejected(self) -> None:
        """Absence is only legal where the annotation says so."""
        validator = TaskValidator()
        with pytest.raises(TypeError, match="does not admit None"):
            validator.validate_annotated_value(annotation=file, value=None, label="demo.out")

    def test_none_under_non_path_annotation_is_tolerated(self) -> None:
        """`doctor` and `inspect workflow` pass None for unsupplied parameters,
        and non-path annotations have never validated it."""
        validator = TaskValidator()
        validator.validate_annotated_value(annotation=str, value=None, label="demo.r")


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class TestOptionalExecution:
    """Absence is a success; a missing required output is still a failure."""

    def test_absent_optional_resolves_to_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(emit_with_optional(emit_extra=False))
        assert result[1] is None
        assert Path(result[0]).read_text(encoding="utf-8").strip() == "main"

    def test_present_optional_resolves_to_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(emit_with_optional(emit_extra=True))
        assert Path(result[1]).read_text(encoding="utf-8").strip() == "extra"

    def test_missing_required_output_still_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError, match="results/required.txt"):
            ginkgo.evaluate(emit_missing_required())

    def test_lone_absent_optional_succeeds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert ginkgo.evaluate(emit_only_optional()) is None

    def test_downstream_consumer_receives_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        produced = emit_with_optional(emit_extra=False)
        result = ginkgo.evaluate(describe(main=produced.output[0], extra=produced.output[1]))
        assert result == "main.txt:absent"

    def test_downstream_consumer_receives_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        produced = emit_with_optional(emit_extra=True)
        result = ginkgo.evaluate(describe(main=produced.output[0], extra=produced.output[1]))
        assert result == "main.txt:extra.txt"


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def _execution_count() -> int:
    """Return how many times a counting task body has actually run.

    Asserting equal results across two evaluations proves nothing — it holds
    whether the task was cached or re-executed. The task appends a line per
    execution, so this counts real runs.
    """
    counter = Path("results/executions.txt")
    if not counter.is_file():
        return 0
    return len(counter.read_text(encoding="utf-8").strip().splitlines())


@task(kind="shell")
def emit_counted(*, emit_extra: bool) -> tuple[file, file | None]:
    """As ``emit_with_optional``, but records each real execution."""
    extra = " && echo extra > results/extra.txt" if emit_extra else ""
    return shell(
        cmd=(
            "mkdir -p results && echo main > results/main.txt"
            f"{extra} && echo ran >> results/executions.txt"
        ),
        output=("results/main.txt", optional("results/extra.txt")),
    )


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


@task()
def consume_pair(*, pair: tuple[file, file | None]) -> str:
    """Receive a heterogeneous tuple as a task input, exercising cache hashing."""
    return f"{Path(pair[0]).name}:{'absent' if pair[1] is None else Path(pair[1]).name}"


class TestOptionalCaching:
    """Both presence states survive a cache round trip, and key differently."""

    def test_absent_optional_is_reproduced_from_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        first = ginkgo.evaluate(emit_counted(emit_extra=False))
        second = ginkgo.evaluate(emit_counted(emit_extra=False))

        assert first == second
        assert second[1] is None
        assert _execution_count() == 1, "second evaluation re-executed instead of hitting cache"

    def test_present_optional_is_restored_from_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        first = ginkgo.evaluate(emit_counted(emit_extra=True))
        Path(first[1]).unlink()

        second = ginkgo.evaluate(emit_counted(emit_extra=True))
        assert Path(second[1]).read_text(encoding="utf-8").strip() == "extra"
        assert _execution_count() == 1, "restoring the optional output re-ran the task"

    def test_optional_folder_output_is_cached(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `folder | None` element must not be stored as if it were a file.

        Applying the tuple's first annotation to every element made the folder
        arrive annotated `file`, so it was silently never stored and the task
        re-ran forever.
        """
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(emit_optional_folder())
        ginkgo.evaluate(emit_optional_folder())

        assert _execution_count() == 1, "optional folder output never cached"

    def test_heterogeneous_tuple_survives_as_a_downstream_input(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Hashing a `tuple[file, file | None]` input must tolerate the None.

        The cache key walk applied the tuple's first annotation to every
        element, so an absent optional arrived annotated `file` and was
        rejected as a non-path value.
        """
        monkeypatch.chdir(tmp_path)
        produced = emit_counted(emit_extra=False)
        result = ginkgo.evaluate(consume_pair(pair=(produced.output[0], produced.output[1])))
        assert result == "main.txt:absent"

    def test_presence_and_absence_key_a_consumer_differently(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A consumer fed a present optional must not be served the absent entry.

        This is what the distinct ``{"type": "absent"}`` token in the cache key
        buys. Were absence hashed as plain null — or were the two states to
        collide — the second call would return the first call's cached answer.
        """
        monkeypatch.chdir(tmp_path)
        without = emit_counted(emit_extra=False)
        assert (
            ginkgo.evaluate(consume_pair(pair=(without.output[0], without.output[1])))
            == "main.txt:absent"
        )

        with_extra = emit_counted(emit_extra=True)
        assert (
            ginkgo.evaluate(consume_pair(pair=(with_extra.output[0], with_extra.output[1])))
            == "main.txt:extra.txt"
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


class TestOptionalFanOut:
    """Presence may differ per branch of a fan-out."""

    def test_branches_carry_independent_presence(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        mapped = emit_per_name().map(name=["a", "b", "c"])

        assert ginkgo.evaluate(mapped.output[1]) == [
            None,
            "results/b_extra.txt",
            None,
        ]

    def test_mixed_presence_fan_out_caches(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A list of heterogeneous tuples must cache every branch."""
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(emit_per_name().map(name=["a", "b", "c"]))
        ginkgo.evaluate(emit_per_name().map(name=["a", "b", "c"]))

        assert _execution_count() == 3, "second fan-out re-executed instead of caching"


class TestOptionalRemoteTransport:
    """Absence survives the encode / stage / decode path used for remote runs."""

    def test_none_round_trips_through_the_codec(self, tmp_path: Path) -> None:
        target = tmp_path / "a.txt"
        target.write_text("a", encoding="utf-8")
        encoded = encode_value((file(str(target)), None), base_dir=tmp_path)

        assert decode_value(encoded, base_dir=tmp_path) == (str(target), None)

    def test_remote_staging_leaves_none_untouched(self, tmp_path: Path) -> None:
        """Staging walks the encoded tree structurally, so None needs no store."""

        class NeverStore:
            def store(self, **kwargs: Any) -> Any:
                raise AssertionError("remote store called for an absent output")

        encoded = encode_value((None, None), base_dir=tmp_path)
        staged = _stage_encoded_value(value=encoded, remote_store=NeverStore())

        assert decode_value(staged, base_dir=tmp_path) == (None, None)


# ---------------------------------------------------------------------------
# Manifest visibility
# ---------------------------------------------------------------------------


class TestOptionalOutputIndex:
    """Presence is explicit in the output index the manifest carries."""

    def test_absent_optional_is_indexed_as_absent(self) -> None:
        summary = output_summary(file | None, None, name="return[1]")
        assert summary == [
            {"name": "return[1]", "type": "file", "optional": True, "present": False}
        ]

    def test_present_optional_is_indexed_with_its_path(self) -> None:
        summary = output_summary(file | None, file("results/extra.txt"), name="return[1]")
        assert summary == [
            {
                "name": "return[1]",
                "type": "file",
                "path": "results/extra.txt",
                "optional": True,
                "present": True,
            }
        ]

    def test_plain_none_result_indexes_nothing(self) -> None:
        assert output_summary(None, None) == []
