"""Integration tests for cache integrity and working-tree materialization."""

import textwrap
from pathlib import Path

import pytest

from ginkgo import evaluate, file, folder, shell, task
from ginkgo.core.task import TaskDef


# ---------------------------------------------------------------------------
# Source hashing
# ---------------------------------------------------------------------------


class TestSourceHash:
    def test_source_hash_is_stable(self):
        @task()
        def stable_fn(x: int) -> int:
            return x + 1

        assert stable_fn.source_hash == stable_fn.source_hash

    def test_different_bodies_produce_different_hashes(self):
        @task()
        def fn_a(x: int) -> int:
            return x + 1

        @task()
        def fn_b(x: int) -> int:
            return x + 2

        assert fn_a.source_hash != fn_b.source_hash

    def test_unsourceable_function_raises_at_registration(self):
        # Functions created via exec() have no inspectable source.
        ns: dict = {}
        exec("def dynamic_fn(x): return x", ns)
        with pytest.raises(ValueError, match="Cannot extract source"):
            TaskDef(fn=ns["dynamic_fn"])


class TestSourceHashCacheInvalidation:
    """Verify that modifying a task function body causes a cache miss."""

    def test_modified_source_causes_cache_miss(self, tmp_path, capsys):
        """Write two versions of a task module and verify cache miss on change."""
        module_dir = tmp_path / "pkg"
        module_dir.mkdir()
        (module_dir / "__init__.py").write_text("")

        # Version 1 of the task.
        v1_source = textwrap.dedent("""\
            from ginkgo import task

            @task()
            def compute(x: int) -> int:
                return x + 1
        """)
        (module_dir / "tasks.py").write_text(v1_source)

        import importlib
        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            mod = importlib.import_module("pkg.tasks")
            result1 = evaluate(mod.compute(x=5))
            assert result1 == 6
            capsys.readouterr()

            # Version 2: change the function body.
            v2_source = textwrap.dedent("""\
                from ginkgo import task

                @task()
                def compute(x: int) -> int:
                    return x + 10
            """)
            (module_dir / "tasks.py").write_text(v2_source)

            # Reload the module to pick up the new source.
            importlib.reload(mod)
            result2 = evaluate(mod.compute(x=5))
            assert result2 == 15
            captured = capsys.readouterr()

            # Should have re-executed, not served from cache.
            assert '"status": "running"' in captured.err
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("pkg.tasks", None)
            sys.modules.pop("pkg", None)


# ---------------------------------------------------------------------------
# File output symlinks
# ---------------------------------------------------------------------------


@task(kind="shell")
def write_file_task(output_path: str) -> file:
    return shell(cmd=f"echo 'hello' > {output_path}", output=output_path)


@task(kind="shell")
def write_folder_task(output_dir: str) -> folder:
    return shell(
        cmd=f"mkdir -p {output_dir} && echo 'a' > {output_dir}/a.txt && echo 'b' > {output_dir}/b.txt",
        output=output_dir,
    )


@task()
def write_python_file_task(output_path: str, payload: str) -> file:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload, encoding="utf-8")
    return str(output)


class TestWritableFileOutputs:
    def test_output_remains_writable_after_execution(self, tmp_path):
        output = tmp_path / "result.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        result_path = Path(str(result))
        assert result_path.is_file()
        assert not result_path.is_symlink()
        assert result_path.read_text().strip() == "hello"

    def test_output_can_be_overwritten_locally(self, tmp_path):
        output = tmp_path / "editable.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        result_path = Path(str(result))
        result_path.write_text("modified", encoding="utf-8")
        assert result_path.read_text(encoding="utf-8") == "modified"

    def test_deleted_file_is_restored_on_cache_hit(self, tmp_path, capsys):
        output = tmp_path / "recreate.txt"
        evaluate(write_file_task(output_path=str(output)))
        capsys.readouterr()

        output.unlink()
        assert not output.exists()

        result = evaluate(write_file_task(output_path=str(output)))
        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
        result_path = Path(str(result))
        assert result_path.is_file()
        assert not result_path.is_symlink()
        assert result_path.read_text().strip() == "hello"

    def test_modified_file_is_restored_without_cache_miss(self, tmp_path, capsys):
        output = tmp_path / "modified.txt"
        evaluate(write_file_task(output_path=str(output)))
        capsys.readouterr()

        output.write_text("tampered", encoding="utf-8")

        result = evaluate(write_file_task(output_path=str(output)))
        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
        result_path = Path(str(result))
        assert result_path.is_file()
        assert result_path.read_text(encoding="utf-8").strip() == "hello"

    def test_python_file_output_rerun_overwrites_previous_materialization(self, tmp_path):
        output = tmp_path / "python.txt"
        first = evaluate(write_python_file_task(output_path=str(output), payload="first"))
        assert Path(str(first)).read_text(encoding="utf-8") == "first"

        second = evaluate(write_python_file_task(output_path=str(output), payload="second"))
        assert Path(str(second)).read_text(encoding="utf-8") == "second"


class TestWritableFolderOutputs:
    def test_output_is_directory_with_regular_files_after_execution(self, tmp_path):
        output = tmp_path / "outdir"
        result = evaluate(write_folder_task(output_dir=str(output)))
        result_path = Path(str(result))
        assert result_path.is_dir()
        assert (result_path / "a.txt").read_text().strip() == "a"
        assert (result_path / "b.txt").read_text().strip() == "b"
        assert not (result_path / "a.txt").is_symlink()
        assert not (result_path / "b.txt").is_symlink()

    def test_folder_contents_are_writable(self, tmp_path):
        output = tmp_path / "writable_dir"
        result = evaluate(write_folder_task(output_dir=str(output)))
        result_path = Path(str(result))
        (result_path / "a.txt").write_text("updated", encoding="utf-8")
        assert (result_path / "a.txt").read_text(encoding="utf-8") == "updated"

    def test_deleted_folder_is_restored(self, tmp_path, capsys):
        output = tmp_path / "dir_recreate"
        evaluate(write_folder_task(output_dir=str(output)))
        capsys.readouterr()

        import shutil

        shutil.rmtree(output)
        assert not output.exists()

        result = evaluate(write_folder_task(output_dir=str(output)))
        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
        result_path = Path(str(result))
        assert result_path.is_dir()
        assert (result_path / "a.txt").read_text().strip() == "a"

    def test_modified_folder_is_restored_without_cache_miss(self, tmp_path, capsys):
        output = tmp_path / "dir_modified"
        evaluate(write_folder_task(output_dir=str(output)))
        capsys.readouterr()

        (output / "a.txt").write_text("tampered", encoding="utf-8")

        result = evaluate(write_folder_task(output_dir=str(output)))
        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
        assert (Path(str(result)) / "a.txt").read_text(encoding="utf-8").strip() == "a"


# ---------------------------------------------------------------------------
# Cache prune with read-only artifacts
# ---------------------------------------------------------------------------


class TestCachePruneReadOnly:
    def test_prune_handles_read_only_artifacts(self, tmp_path):
        """Pruning cache entries with read-only artifacts should not raise."""
        output = tmp_path / "prunable.txt"
        evaluate(write_file_task(output_path=str(output)))

        cache_root = Path(".ginkgo") / "cache"
        assert cache_root.exists()

        # Prune everything by removing all entries directly.
        import shutil
        from ginkgo.runtime.artifacts.artifact_store import _make_writable_recursive

        for entry in cache_root.iterdir():
            if entry.is_dir():
                try:
                    shutil.rmtree(entry)
                except PermissionError:
                    _make_writable_recursive(entry)
                    shutil.rmtree(entry)
