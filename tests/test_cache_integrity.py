"""Integration tests for Phase 2 cache integrity: source hashing and symlink outputs."""

import stat
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


class TestFileOutputSymlinks:
    def test_output_is_symlinked_after_execution(self, tmp_path):
        output = tmp_path / "result.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        assert Path(str(result)).is_symlink()
        assert Path(str(result)).read_text().strip() == "hello"

    def test_symlink_target_is_read_only(self, tmp_path):
        output = tmp_path / "readonly.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        target = Path(str(result)).resolve()
        mode = target.stat().st_mode
        assert not (mode & stat.S_IWUSR)

    def test_writing_through_symlink_raises_permission_error(self, tmp_path):
        output = tmp_path / "locked.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        with pytest.raises(PermissionError):
            Path(str(result)).write_text("modified")

    def test_deleted_symlink_is_recreated_on_cache_hit(self, tmp_path, capsys):
        output = tmp_path / "recreate.txt"
        evaluate(write_file_task(output_path=str(output)))
        capsys.readouterr()

        # Delete the symlink.
        output.unlink()
        assert not output.exists()

        # Re-evaluate: should be a cache hit that recreates the symlink.
        result = evaluate(write_file_task(output_path=str(output)))
        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
        assert Path(str(result)).is_symlink()
        assert Path(str(result)).read_text().strip() == "hello"

    def test_replaced_file_causes_cache_miss(self, tmp_path, capsys):
        output = tmp_path / "replaced.txt"
        evaluate(write_file_task(output_path=str(output)))
        capsys.readouterr()

        # Replace the symlink with a regular file.
        output.unlink()
        output.write_text("tampered")

        # Re-evaluate: should be a cache miss (regular file, not symlink).
        result = evaluate(write_file_task(output_path=str(output)))
        captured = capsys.readouterr()
        assert '"status": "running"' in captured.err
        # After re-execution, should be symlinked again.
        assert Path(str(result)).is_symlink()


class TestFolderOutputSymlinks:
    def test_output_is_symlinked_after_execution(self, tmp_path):
        output = tmp_path / "outdir"
        result = evaluate(write_folder_task(output_dir=str(output)))
        assert Path(str(result)).is_symlink()
        assert (Path(str(result)) / "a.txt").read_text().strip() == "a"
        assert (Path(str(result)) / "b.txt").read_text().strip() == "b"

    def test_folder_contents_are_read_only(self, tmp_path):
        output = tmp_path / "ro_dir"
        result = evaluate(write_folder_task(output_dir=str(output)))
        target = Path(str(result)).resolve()
        file_mode = (target / "a.txt").stat().st_mode
        assert not (file_mode & stat.S_IWUSR)

    def test_deleted_symlink_is_recreated(self, tmp_path, capsys):
        output = tmp_path / "dir_recreate"
        evaluate(write_folder_task(output_dir=str(output)))
        capsys.readouterr()

        # Remove the symlink.
        output.unlink()
        assert not output.exists()

        result = evaluate(write_folder_task(output_dir=str(output)))
        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
        assert Path(str(result)).is_symlink()


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
        from ginkgo.runtime.artifact_store import _make_writable_recursive

        for entry in cache_root.iterdir():
            if entry.is_dir():
                try:
                    shutil.rmtree(entry)
                except PermissionError:
                    _make_writable_recursive(entry)
                    shutil.rmtree(entry)
