"""Tests for ``Out[...]`` — a direction wrapper for task path parameters.

Covers the runtime side of phase 1 (issue #307): pre-execution validation,
post-execution existence checks, cache-key contribution, and cache-hit
revalidation. Definition-time acceptance/rejection lives in
``tests/core/test_task.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, folder, task
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.task_validation import DeclaredOutputNotWrittenError
from tests.conftest import EventCollector
from tests.runtime.conftest import prebuilt_registry


# ---------------------------------------------------------------------------
# Module-level tasks (importable, as ``@task`` requires for process execution)
# ---------------------------------------------------------------------------


@task()
def write_output(payload: str, out_path: Out[file]) -> file:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return file(out_path)


@task()
def write_output_untracked_return(payload: str, out_path: Out[file]) -> int:
    """Writes its ``Out[file]`` but returns something else entirely.

    Isolates the ``Out[...]`` path from the return-value artifact-restoration
    machinery (``-> file`` would restore/heal the same path from the
    artifact store on a hit, which is a different, pre-existing mechanism).
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return len(payload)


@task()
def write_output_dir(payload: str, out_dir: Out[folder]) -> folder:
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    (path / "a.txt").write_text(payload, encoding="utf-8")
    return folder(out_dir)


@task()
def forgetful(marker: file, out_path: Out[file]) -> file:
    """Declares an output but never writes it — returns an unrelated file."""
    return marker


@task()
def makes_a_directory(marker: file, out_path: Out[file]) -> file:
    """Creates a directory where its declared file output should be."""
    Path(out_path).mkdir()
    return marker


@task()
def writes_beside_its_output(out_path: Out[file]) -> file:
    """Writes the wrong path, then returns the declared one."""
    Path(out_path + ".x").write_text("x", encoding="utf-8")
    return file(out_path)


@task()
def never_writes(out_path: Out[file]) -> None:
    """Declares an output, writes nothing, and returns nothing."""


@task()
def maybe_write(payload: str | None, out_path: Out[file | None]) -> file | None:
    if out_path is None:
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload or "", encoding="utf-8")
    return file(out_path)


@task(remote=True)
def remote_with_output(bam: Out[file]) -> file:
    raise AssertionError("never runs")


@task(executor="gpu-k8s")
def executor_with_output(bam: Out[file]) -> file:
    raise AssertionError("never runs")


# ---------------------------------------------------------------------------
# Remote dispatch places Out[...] tasks like any other python task
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("stub_executor_types")
@pytest.mark.parametrize("task_def", [remote_with_output, executor_with_output])
def test_out_params_do_not_block_remote_placement(task_def) -> None:
    """Issue #307 phase 2C; round trips live in ``tests/remote/test_remote_out.py``."""
    config = {"remote": {"executors": {"gpu-k8s": {"type": "k8s", "namespace": "ml"}}}}
    registry = prebuilt_registry(config, default="gpu-k8s")
    evaluator = ConcurrentEvaluator(jobs=1, executor_registry=registry)
    assert evaluator.placement.resolve(task_def=task_def) == "gpu-k8s"


# ---------------------------------------------------------------------------
# Cache-key contribution: path string only, never content
# ---------------------------------------------------------------------------


def test_an_out_param_keys_on_its_path_string_not_disk(tmp_path: Path):
    """Building the key never needs the declared path to exist."""
    with CacheIndex.in_memory() as index:
        store = CacheStore(index=index, root=tmp_path / "cache")

        def key(name: str):
            path = str(tmp_path / "does-not-exist" / name)
            return store.build_cache_key(
                task_def=write_output, resolved_args={"payload": "x", "out_path": path}
            )

        (key_a, hashes), (again, _), (key_b, _) = key("a.bam"), key("a.bam"), key("b.bam")
        assert hashes["out_path"]["type"] == "str"
        assert key_a == again != key_b


@pytest.mark.parametrize(
    ("make", "existing", "match"),
    [
        (write_output, "dir", "already exists and is a directory"),
        (write_output_dir, "file", "already exists and is a file"),
    ],
)
def test_an_out_path_of_the_wrong_kind_fails_before_the_run(
    tmp_path: Path, make, existing: str, match: str
):
    """Unlike ``file``, an ``Out[file]`` need not exist, but must not be a directory."""
    path = tmp_path / "already"
    path.mkdir() if existing == "dir" else path.write_text("x")
    with pytest.raises(TypeError, match=match):
        evaluate(make(payload="hi", **{make.output_params_in_order[0]: str(path)}))


class TestOutPostExecutionValidation:
    def test_task_that_fails_to_write_its_output_fails_clearly(self, tmp_path: Path):
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="was not written"):
            evaluate(forgetful(marker=file(str(marker)), out_path=str(tmp_path / "never.txt")))

    def test_a_directory_where_a_file_was_declared_is_named(self, tmp_path: Path):
        """ED5-9: the path was written, as the wrong kind; say so."""
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")

        with pytest.raises(DeclaredOutputNotWrittenError, match="is a directory"):
            evaluate(makes_a_directory(marker=file(str(marker)), out_path=str(tmp_path / "o.txt")))

    @pytest.mark.parametrize("make", [writes_beside_its_output, never_writes])
    def test_an_unwritten_output_is_an_output_failure_naming_the_parameter(
        self, tmp_path: Path, make
    ):
        """#341: the declared-output check runs before the generic ``.return`` check."""
        out_path = tmp_path / "o.txt"
        collector = EventCollector()

        with pytest.raises(DeclaredOutputNotWrittenError) as excinfo:
            evaluate(make(out_path=str(out_path)), event_bus=collector.bus)

        message = str(excinfo.value)
        assert f"{make.base_name}.out_path is declared `Out[file]`" in message
        assert str(out_path) in message
        assert ".return" not in message
        [failed] = collector.failed()
        assert failed.failure["kind"] == "output_validation_error"
        assert failed.failure["code"] == "DeclaredOutputNotWrittenError"


def _cached(expr) -> bool:
    collector = EventCollector()
    evaluate(expr, event_bus=collector.bus)
    return bool(collector.cached())


def test_an_edited_or_deleted_output_is_restored_on_a_hit_whatever_the_return(tmp_path: Path):
    out_path = tmp_path / "out.txt"
    expr = write_output_untracked_return(payload="v1", out_path=str(out_path))
    evaluate(expr)

    out_path.write_text("tampered", encoding="utf-8")
    assert _cached(expr)
    assert out_path.read_text(encoding="utf-8") == "v1"

    out_path.unlink()
    assert _cached(expr)
    assert out_path.read_text(encoding="utf-8") == "v1"


@pytest.mark.parametrize("payload", [None, "hi"], ids=["absent", "present"])
def test_an_optional_output_is_cached_whether_or_not_given(tmp_path: Path, payload):
    out_path = None if payload is None else str(tmp_path / "present.txt")
    result = evaluate(maybe_write(payload=payload, out_path=out_path))

    assert (result is None) == (payload is None)
    assert _cached(maybe_write(payload=payload, out_path=out_path))


def test_a_read_only_output_left_by_an_earlier_run_is_replaced(tmp_path: Path) -> None:
    out_path = tmp_path / "left.txt"
    out_path.write_text("old", encoding="utf-8")
    out_path.chmod(0o444)

    evaluate(write_output(payload="new", out_path=str(out_path)))

    assert out_path.read_text(encoding="utf-8") == "new"


@task()
def list_folder(out_dir: Out[folder]) -> list[str]:
    """Writes one file into its folder and reports everything it finds there."""
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    (path / "new.txt").write_text("new", encoding="utf-8")
    return sorted(child.name for child in path.iterdir())


def test_a_python_task_starts_with_its_out_folder_cleared(tmp_path: Path) -> None:
    out_dir = tmp_path / "chunks"
    out_dir.mkdir()
    (out_dir / "stale.txt").write_text("stale", encoding="utf-8")

    assert evaluate(list_folder(out_dir=str(out_dir))) == ["new.txt"]
