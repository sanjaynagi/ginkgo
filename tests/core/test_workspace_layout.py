"""Tests for the ``.ginkgo/`` directory layout value object.

The layout's job is to keep the directory convention in one place, so the
tests that matter are the ones pinning the names it hands out and the ones
checking that real call sites still land where they did before it existed.
"""

from __future__ import annotations

from pathlib import Path

from ginkgo.cli.commands.report import _resolve_output_dir
from ginkgo.runtime.artifacts.remote_arg_transfer import _is_managed_cas_blob
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.workspace_layout import DIRECTORY_NAME, WorkspaceLayout


class TestConstruction:
    """The ways a layout is obtained."""

    def test_for_cwd_is_rooted_at_the_working_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        # Compared via resolve() because macOS reports /private/var for /var.
        assert WorkspaceLayout.for_cwd().root.resolve() == (tmp_path / DIRECTORY_NAME).resolve()

    def test_relative_keeps_paths_workspace_relative(self):
        layout = WorkspaceLayout.relative()

        assert layout.root == Path(DIRECTORY_NAME)
        assert not layout.runs.is_absolute()

    def test_sibling_of_returns_the_neighbouring_layout(self, tmp_path):
        layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

        assert WorkspaceLayout.sibling_of(layout.cache) == layout

    def test_sibling_of_round_trips_every_directory(self, tmp_path):
        layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

        for path in (layout.runs, layout.cache, layout.artifacts):
            assert WorkspaceLayout.sibling_of(path) == layout

    def test_sibling_of_accepts_a_root_outside_a_ginkgo_directory(self, tmp_path):
        """Store roots are caller-supplied, so this must not require .ginkgo."""
        assert WorkspaceLayout.sibling_of(tmp_path / "scratch").artifacts == tmp_path / "artifacts"


class TestDirectories:
    """Every path the layout owns hangs off its root."""

    def test_directory_names_are_stable(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GINKGO_DB", raising=False)
        layout = WorkspaceLayout(root=tmp_path)

        assert layout.runs.name == "runs"
        assert layout.cache.name == "cache"
        assert layout.artifacts.name == "artifacts"
        assert layout.staging.name == "staging"
        assert layout.fuse.name == "fuse"
        assert layout.notebooks.name == "notebooks"
        assert layout.reports.name == "reports"
        assert layout.db.name == "ginkgo.db"

    def test_no_two_concerns_share_a_directory(self, tmp_path, monkeypatch):
        # GINKGO_DB would move the database out of the layout and collapse the
        # count, so this asks the question of the default layout.
        monkeypatch.delenv("GINKGO_DB", raising=False)
        layout = WorkspaceLayout(root=tmp_path)

        paths = {
            layout.runs,
            layout.cache,
            layout.artifacts,
            layout.staging,
            layout.fuse,
            layout.notebooks,
            layout.reports,
            layout.db,
        }

        assert len(paths) == 8
        assert all(path.parent == layout.root for path in paths)

    def test_creates_no_directories(self, tmp_path):
        layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

        _ = (layout.runs, layout.cache, layout.artifacts)

        assert not layout.root.exists()


class TestDatabaseLocation:
    """``GINKGO_DB`` is read here and nowhere else."""

    def test_the_database_sits_in_the_workspace_by_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GINKGO_DB", raising=False)
        layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

        assert layout.db == tmp_path / DIRECTORY_NAME / "ginkgo.db"

    def test_the_environment_override_relocates_it(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GINKGO_DB", str(tmp_path / "local" / "ledger.db"))
        layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

        assert layout.db == tmp_path / "local" / "ledger.db"

    def test_an_empty_override_is_ignored(self, tmp_path, monkeypatch):
        """An unset-looking variable must not resolve the database to the cwd."""
        monkeypatch.setenv("GINKGO_DB", "")
        layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

        assert layout.db == tmp_path / DIRECTORY_NAME / "ginkgo.db"


class TestValueSemantics:
    """The layout is a value, not a handle."""

    def test_layouts_with_the_same_root_are_equal(self, tmp_path):
        assert WorkspaceLayout(root=tmp_path) == WorkspaceLayout(root=tmp_path)

    def test_is_hashable(self, tmp_path):
        assert len({WorkspaceLayout(root=tmp_path), WorkspaceLayout(root=tmp_path)}) == 1


class TestCallSitesUnchanged:
    """The paths real components resolve to must not have moved."""

    def test_cache_store_puts_artifacts_beside_its_cache_root(self, tmp_path):
        cache_root = tmp_path / DIRECTORY_NAME / "cache"
        store = CacheStore(root=cache_root, index=CacheIndex.in_memory())

        # The sibling derivation the layout now owns.
        assert store._artifact_store._root == tmp_path / DIRECTORY_NAME / "artifacts"

    def test_cache_store_default_root_is_under_the_working_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        store = CacheStore(index=CacheIndex.in_memory())

        assert store._root.resolve() == (tmp_path / DIRECTORY_NAME / "cache").resolve()

    def test_report_output_dir_lands_in_the_reports_directory(self, tmp_path):
        run_dir = tmp_path / DIRECTORY_NAME / "runs" / "run-abc"

        resolved = _resolve_output_dir(run_dir=run_dir, out=None, single_file=False)

        assert resolved == (tmp_path / DIRECTORY_NAME / "reports" / "run-abc").resolve()

    def test_managed_blob_detection_still_matches_both_trees(self):
        for tree in ("staging", "artifacts"):
            path = Path(f"/work/{DIRECTORY_NAME}/{tree}/blobs/abc123/data.bin")
            assert _is_managed_cas_blob(path=path)

    def test_managed_blob_detection_rejects_other_paths(self):
        assert not _is_managed_cas_blob(path=Path("/work/other/blobs/abc/data.bin"))
        assert not _is_managed_cas_blob(path=Path(f"/work/{DIRECTORY_NAME}/cache/abc/data.bin"))
