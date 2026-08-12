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
from ginkgo import file, optional, shell, task
from ginkgo.core.optional import OptionalOutput
from ginkgo.core.types import unwrap_optional_annotation
from ginkgo.runtime.artifacts.output_index import output_summary
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


class TestOptionalCaching:
    """Both presence states survive a cache round trip, and key differently."""

    def test_absent_optional_is_reproduced_from_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        first = ginkgo.evaluate(emit_with_optional(emit_extra=False))
        second = ginkgo.evaluate(emit_with_optional(emit_extra=False))
        assert first == second
        assert second[1] is None

    def test_present_optional_is_restored_from_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        first = ginkgo.evaluate(emit_with_optional(emit_extra=True))
        Path(first[1]).unlink()

        second = ginkgo.evaluate(emit_with_optional(emit_extra=True))
        assert Path(second[1]).read_text(encoding="utf-8").strip() == "extra"

    def test_presence_and_absence_are_distinct_cache_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A present optional must not be served from an absent entry."""
        monkeypatch.chdir(tmp_path)
        absent = ginkgo.evaluate(emit_with_optional(emit_extra=False))
        present = ginkgo.evaluate(emit_with_optional(emit_extra=True))
        assert absent[1] is None
        assert present[1] is not None


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
