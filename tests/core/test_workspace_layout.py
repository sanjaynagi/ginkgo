"""Tests for the ``.ginkgo/`` directory layout value object."""

from __future__ import annotations

from pathlib import Path

from ginkgo.workspace_layout import DIRECTORY_NAME, WorkspaceLayout


class TestConstruction:
    """The ways a layout is obtained."""

    def test_for_workspace_appends_the_directory_name(self, tmp_path):
        layout = WorkspaceLayout.for_workspace(tmp_path)

        assert layout.root == tmp_path / DIRECTORY_NAME

    def test_for_cwd_is_rooted_at_the_working_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        # Compared via resolve() because macOS reports /private/var for /var.
        assert WorkspaceLayout.for_cwd().root.resolve() == (tmp_path / DIRECTORY_NAME).resolve()

    def test_relative_keeps_paths_workspace_relative(self):
        layout = WorkspaceLayout.relative()

        assert layout.root == Path(DIRECTORY_NAME)
        assert not layout.runs.is_absolute()

    def test_containing_recovers_the_layout_from_a_subdirectory(self, tmp_path):
        layout = WorkspaceLayout.for_workspace(tmp_path)

        assert WorkspaceLayout.containing(layout.cache) == layout

    def test_containing_round_trips_every_directory(self, tmp_path):
        layout = WorkspaceLayout.for_workspace(tmp_path)

        for path in (layout.runs, layout.cache, layout.assets, layout.artifacts):
            assert WorkspaceLayout.containing(path) == layout


class TestDirectories:
    """Every path the layout owns hangs off its root."""

    def test_each_directory_is_a_child_of_the_root(self, tmp_path):
        layout = WorkspaceLayout.for_workspace(tmp_path)

        paths = {
            layout.runs,
            layout.cache,
            layout.assets,
            layout.artifacts,
            layout.staging,
            layout.fuse,
            layout.notebooks,
            layout.reports,
            layout.staging_cache_file,
        }

        assert all(path.parent == layout.root for path in paths)
        # No two concerns may share a directory.
        assert len(paths) == 9

    def test_directory_names_are_stable(self, tmp_path):
        layout = WorkspaceLayout.for_workspace(tmp_path)

        assert layout.runs.name == "runs"
        assert layout.cache.name == "cache"
        assert layout.assets.name == "assets"
        assert layout.artifacts.name == "artifacts"
        assert layout.staging.name == "staging"
        assert layout.fuse.name == "fuse"
        assert layout.notebooks.name == "notebooks"
        assert layout.reports.name == "reports"
        assert layout.staging_cache_file.name == "remote-staged.json"

    def test_relocating_the_root_moves_every_directory(self, tmp_path):
        moved = WorkspaceLayout(root=tmp_path / "elsewhere")

        assert moved.artifacts == tmp_path / "elsewhere" / "artifacts"
        assert moved.staging_cache_file == tmp_path / "elsewhere" / "remote-staged.json"


class TestValueSemantics:
    """The layout is a value, not a handle."""

    def test_layouts_with_the_same_root_are_equal(self, tmp_path):
        assert WorkspaceLayout(root=tmp_path) == WorkspaceLayout(root=tmp_path)

    def test_is_hashable(self, tmp_path):
        assert len({WorkspaceLayout(root=tmp_path), WorkspaceLayout(root=tmp_path)}) == 1

    def test_creates_no_directories(self, tmp_path):
        layout = WorkspaceLayout.for_workspace(tmp_path)

        _ = (layout.runs, layout.cache, layout.artifacts)

        assert not layout.root.exists()
