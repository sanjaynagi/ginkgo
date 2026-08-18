"""Notebook artifact pointers stored in and replayed from the cache.

Regression cover for issue #137: a cache-hit notebook task replayed a
cwd-relative pointer that consumers joined onto the new run directory,
producing a path that existed nowhere.

Regression cover for issue #202 part 2: the replayed pointer carried no
provenance, so the new run claimed an earlier run's artifact — and its
recorded export failure — as its own.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.runtime.task_runners.notebook import (
    NotebookRunner,
    resolve_cached_artifact_pointers,
)


@dataclass(kw_only=True)
class _StubProvenance:
    """Minimal provenance recorder capturing manifest extras."""

    run_dir: Path
    run_id: str = "old_run"
    extras: dict[str, Any] = field(default_factory=dict)

    def update_task_extra(self, *, node_id: int, **extra: Any) -> None:
        self.extras.update(extra)


@dataclass(kw_only=True)
class _StubNode:
    """Minimal DAG node accepting the stashed cache extras."""

    node_id: int = 14
    notebook_extras: dict[str, Any] | None = None
    task_def: Any = None


def _write_artifacts(*, run_dir: Path) -> tuple[Path, Path]:
    """Create a notebook HTML/ipynb pair under ``run_dir`` and return both."""
    notebooks = run_dir / "notebooks"
    notebooks.mkdir(parents=True)
    html_path = notebooks / "task_0014.html"
    executed_path = notebooks / "task_0014.ipynb"
    html_path.write_text("<html></html>")
    executed_path.write_text("{}")
    return html_path, executed_path


def _record(*, runner: NotebookRunner, node: _StubNode, html: Path, executed: Path) -> None:
    runner._record_notebook_manifest(
        node=node,
        notebook_kind="ipynb",
        notebook_path=Path("notebooks/overview.ipynb"),
        notebook_description=None,
        executed_path=executed,
        rendered_html=html,
        render_status="ok",
        render_error=None,
    )


class TestStoredPointerForm:
    """The manifest keeps run-relative pointers; the cache keeps absolute ones."""

    def test_manifest_is_run_relative_and_cache_is_absolute(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        monkeypatch.chdir(tmp_path)
        run_dir = Path(".ginkgo") / "runs" / "old_run"
        html, executed = _write_artifacts(run_dir=run_dir)

        provenance = _StubProvenance(run_dir=run_dir)
        runner = NotebookRunner.__new__(NotebookRunner)
        runner.provenance = provenance
        node = _StubNode()
        _record(runner=runner, node=node, html=html, executed=executed)

        assert provenance.extras["rendered_html"] == "notebooks/task_0014.html"
        assert provenance.extras["executed_notebook"] == "notebooks/task_0014.ipynb"

        cache_extras = node.notebook_extras
        assert cache_extras is not None
        for key, path in (
            ("rendered_html", html),
            ("executed_notebook", executed),
        ):
            stored = Path(cache_extras[key])
            assert stored.is_absolute()
            assert stored == path.resolve()


class TestReplayedPointerResolves:
    """A replayed pointer must resolve to the real file under the new run dir."""

    def test_absolute_pointer_survives_join_onto_new_run_dir(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        monkeypatch.chdir(tmp_path)
        old_run = Path(".ginkgo") / "runs" / "old_run"
        html, executed = _write_artifacts(run_dir=old_run)
        new_run = Path(".ginkgo") / "runs" / "new_run"
        new_run.mkdir(parents=True)

        cache_extras = {
            "rendered_html": str(html.resolve()),
            "executed_notebook": str(executed.resolve()),
        }
        replayed = resolve_cached_artifact_pointers(extras=cache_extras)

        # Consumers join the replayed pointer onto the new run directory.
        assert (new_run / replayed["rendered_html"]).resolve() == html.resolve()
        assert (new_run / replayed["executed_notebook"]).resolve() == executed.resolve()

    def test_cwd_relative_pointer_from_a_poisoned_entry_is_repaired(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        monkeypatch.chdir(tmp_path)
        old_run = Path(".ginkgo") / "runs" / "old_run"
        html, _ = _write_artifacts(run_dir=old_run)
        new_run = Path(".ginkgo") / "runs" / "new_run"
        new_run.mkdir(parents=True)

        replayed = resolve_cached_artifact_pointers(extras={"rendered_html": str(html)})

        assert (new_run / replayed["rendered_html"]).resolve() == html.resolve()

    def test_pointer_to_a_missing_file_is_dropped(self, tmp_path: Path, monkeypatch: Any) -> None:
        monkeypatch.chdir(tmp_path)
        extras = {
            "task_type": "notebook",
            "rendered_html": str(tmp_path / "gone" / "task_0014.html"),
            "executed_notebook": ".ginkgo/runs/pruned/notebooks/task_0014.ipynb",
        }

        replayed = resolve_cached_artifact_pointers(extras=extras)

        assert replayed == {"task_type": "notebook"}

    def test_replayed_pointer_is_attributed_to_the_producing_run(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """The run id travels with the pointer, so the new run can disown it."""
        monkeypatch.chdir(tmp_path)
        old_run = Path(".ginkgo") / "runs" / "old_run"
        html, executed = _write_artifacts(run_dir=old_run)

        provenance = _StubProvenance(run_dir=old_run, run_id="old_run")
        runner = NotebookRunner.__new__(NotebookRunner)
        runner.provenance = provenance
        node = _StubNode()
        _record(runner=runner, node=node, html=html, executed=executed)

        # The producing run names itself in its own manifest.
        assert provenance.extras["notebook_artifact_run_id"] == "old_run"

        cache_extras = node.notebook_extras
        assert cache_extras is not None
        replayed = resolve_cached_artifact_pointers(extras=cache_extras)

        # A later run replaying these extras learns which run produced them.
        assert replayed["notebook_artifact_run_id"] == "old_run"
        assert Path(replayed["rendered_html"]) == html.resolve()

    def test_attribution_is_dropped_with_the_pointers_it_describes(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        monkeypatch.chdir(tmp_path)
        extras = {
            "task_type": "notebook",
            "notebook_artifact_run_id": "pruned_run",
            "rendered_html": str(tmp_path / "gone" / "task_0014.html"),
        }

        assert resolve_cached_artifact_pointers(extras=extras) == {"task_type": "notebook"}


@dataclass(kw_only=True)
class _StubTaskDef:
    """Minimal task definition carrying the kind the runner checks."""

    kind: str = "notebook"
    name: str = "render_overview_notebook"


@dataclass(kw_only=True)
class _StubCacheStore:
    """Cache store returning one canned extra-meta payload."""

    extra: dict[str, Any] | None

    def load_extra_meta(self, *, cache_key: str) -> dict[str, Any] | None:
        return self.extra


def _replay(*, cached_extras: dict[str, Any], run_dir: Path) -> list[str]:
    """Replay ``cached_extras`` onto a fresh run and return emitted notices."""
    notices: list[str] = []
    runner = NotebookRunner.__new__(NotebookRunner)
    runner.provenance = _StubProvenance(run_dir=run_dir, run_id="new_run")
    runner.cache_store = _StubCacheStore(extra={"notebook_extras": cached_extras})
    runner.notice_emitter = lambda node, message: notices.append(message)
    runner.replay_cached_extras(node=_StubNode(task_def=_StubTaskDef()), cache_key="key")
    return notices


class TestReplayedExportFailureNotice:
    """A replayed export failure reaches the event stream on every run.

    Issue #218: the outcome was recorded in the manifest but not surfaced
    where an automated consumer decides. A cache hit replays the earlier
    run's placeholder failure page as this run's notebook artifact, so this
    run's report links a traceback page too.
    """

    def test_replayed_failed_render_emits_a_notice(self, tmp_path: Path, monkeypatch: Any) -> None:
        monkeypatch.chdir(tmp_path)
        html, _ = _write_artifacts(run_dir=Path(".ginkgo") / "runs" / "old_run")

        notices = _replay(
            cached_extras={
                "task_type": "notebook",
                "render_status": "failed",
                "notebook_artifact_run_id": "old_run",
                "rendered_html": str(html.resolve()),
            },
            run_dir=Path(".ginkgo") / "runs" / "new_run",
        )

        assert notices == [
            "HTML export failed in run old_run, whose notebook artifacts this task replayed; "
            f"{html.resolve()} holds the export error instead of the rendered notebook."
        ]

    def test_replayed_successful_render_emits_nothing(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        monkeypatch.chdir(tmp_path)
        html, _ = _write_artifacts(run_dir=Path(".ginkgo") / "runs" / "old_run")

        notices = _replay(
            cached_extras={
                "task_type": "notebook",
                "render_status": "succeeded",
                "notebook_artifact_run_id": "old_run",
                "rendered_html": str(html.resolve()),
            },
            run_dir=Path(".ginkgo") / "runs" / "new_run",
        )

        assert notices == []
