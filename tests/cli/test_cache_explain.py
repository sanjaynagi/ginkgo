"""Tests for the per-component cache-key diff behind ``ginkgo cache explain``.

Entries are planted as rows, because rows are the only cache index there is.
Each case builds one run whose task wrote ``current`` and one or more earlier
runs or entries to compare it against.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any

from ginkgo import task
from ginkgo.cli.commands.cache import _render_explain_text, explain_run_cache
from ginkgo.query import Query
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

WORKFLOW = "workflow.py"


def _meta(cache_key: str, **fields: Any) -> dict[str, Any]:
    """Return the facts one cache entry records."""
    meta: dict[str, Any] = {
        "cache_key": cache_key,
        "function": "produce",
        "version": "v1",
        "source_hash": "src-1",
        "extra_source_hash": None,
        "env": None,
        "env_hash": None,
        "input_hashes": {"samples": "hash-a", "threads": "hash-t"},
        "created_at": "2026-08-18T10:00:00+00:00",
    }
    meta.update(fields)
    return meta


def _write_entry(index: CacheIndex, cache_key: str, **fields: Any) -> None:
    """Record one cache entry with the given key components."""
    index.record_entry(
        cache_key=cache_key,
        meta=_meta(cache_key, **fields),
        artifact_ids={},
        size_bytes=0,
        run_id=None,
    )


def _write_run(
    db_path: Path,
    *,
    run_id: str,
    started_at: str,
    cache_key: str,
    task_name: str = "produce",
    display_label: str | None = None,
    status: str = "succeeded",
) -> None:
    """Record one run with a single task that used *cache_key*."""
    with open_store(db_path) as store, store.transaction():
        store.apply(
            [
                ProjectionOp(
                    sql="INSERT INTO runs (run_id, workflow, status, started_at) "
                    "VALUES (?, ?, 'succeeded', ?)",
                    params=(run_id, WORKFLOW, started_at),
                ),
                ProjectionOp(
                    sql="INSERT INTO tasks (run_id, task_id, node_id, name, display_label, "
                    "kind, execution_mode, status, cache_key, attempts) "
                    "VALUES (?, 'task_0000', 0, ?, ?, 'task', 'thread', ?, ?, 1)",
                    params=(run_id, task_name, display_label, status, cache_key),
                ),
            ]
        )


def _explain(db_path: Path, run_id: str = "run-2") -> dict[str, Any]:
    """Explain the run whose task wrote the entry under test."""
    with Query(open_store(db_path, readonly=True), layout=WorkspaceLayout.relative()) as reader:
        payload = explain_run_cache(reader=reader, run_id=run_id)
    tasks = payload["tasks"]
    assert isinstance(tasks, list)
    return tasks[0]


def _component(explanation: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the reported diff for one named component."""
    components = explanation["components"]
    matches = [entry for entry in components if entry["component"] == name]
    assert matches, f"{name} not reported in {components}"
    return matches[0]


def _write_task_input(
    db_path: Path, *, run_id: str, task_id: str, param: str, tracking: str | None
) -> None:
    """Record one position-0 ``task_inputs`` row with a tracking label."""
    with open_store(db_path) as store, store.transaction():
        store.apply(
            [
                ProjectionOp(
                    sql="INSERT INTO task_inputs (run_id, task_id, param, position, tracking) "
                    "VALUES (?, ?, ?, 0, ?)",
                    params=(run_id, task_id, param, tracking),
                )
            ]
        )


def _index(tmp_path: Path) -> CacheIndex:
    """Return a cache index over a fresh database."""
    return CacheIndex.open(path=tmp_path / "ginkgo.db")


def _db(tmp_path: Path) -> Path:
    """Return the database the helpers in this module share."""
    return tmp_path / "ginkgo.db"


def _two_runs(index: CacheIndex, db_path: Path, **current_fields: Any) -> None:
    """Record a prior run and the current one, with an entry for each."""
    _write_entry(index, "prior", created_at="2026-08-18T09:00:00+00:00")
    _write_entry(index, "current", **current_fields)
    _write_run(db_path, run_id="run-1", started_at="2026-08-18T09:00:00+00:00", cache_key="prior")
    _write_run(
        db_path, run_id="run-2", started_at="2026-08-18T10:00:00+00:00", cache_key="current"
    )


class TestCacheExplainComponents:
    def test_source_hash_change_is_named(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _two_runs(index, _db(tmp_path), source_hash="src-2")
            explanation = _explain(_db(tmp_path))

        assert explanation["reason"] == "source_hash_changed"
        assert explanation["compared_with"] == {"cache_key": "prior", "strategy": "same_node"}
        assert _component(explanation, "source_hash") == {
            "component": "source_hash",
            "status": "changed",
            "current": "src-2",
            "prior": "src-1",
        }
        assert [entry["component"] for entry in explanation["components"]] == ["source_hash"]

    def test_input_change_names_the_parameter(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _two_runs(
                index, _db(tmp_path), input_hashes={"samples": "hash-b", "threads": "hash-t"}
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["reason"] == "input_changed"
        assert _component(explanation, "inputs.samples")["status"] == "changed"
        assert [entry["component"] for entry in explanation["components"]] == ["inputs.samples"]

    def test_added_input_parameter_is_named(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _two_runs(
                index,
                _db(tmp_path),
                input_hashes={"samples": "hash-a", "threads": "hash-t", "seed": "hash-s"},
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["reason"] == "input_changed"
        assert _component(explanation, "inputs.seed") == {
            "component": "inputs.seed",
            "status": "added",
            "current": "hash-s",
        }

    def test_environment_identity_change_is_named(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _write_entry(
                index,
                "prior",
                env="bio",
                env_hash={"env": "bio", "pixi_lock": "manifest-1"},
                created_at="2026-08-18T09:00:00+00:00",
            )
            _write_entry(
                index, "current", env="bio", env_hash={"env": "bio", "pixi_lock": "manifest-2"}
            )
            _write_run(
                _db(tmp_path),
                run_id="run-1",
                started_at="2026-08-18T09:00:00+00:00",
                cache_key="prior",
            )
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["reason"] == "env_changed"
        assert _component(explanation, "env_hash.pixi_lock") == {
            "component": "env_hash.pixi_lock",
            "status": "changed",
            "current": "manifest-2",
            "prior": "manifest-1",
        }

    def test_version_bump_is_named(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _two_runs(index, _db(tmp_path), version="v2")
            explanation = _explain(_db(tmp_path))

        assert explanation["reason"] == "version_bump"
        assert _component(explanation, "version")["status"] == "changed"

    def test_the_same_node_beats_a_fan_out_sibling(self, tmp_path: Path) -> None:
        """A sibling branch is not this node's history, however recent (issue #223)."""
        with _index(tmp_path) as index:
            _write_entry(index, "prior", created_at="2026-08-18T09:00:00+00:00")
            _write_entry(
                index, "sibling", source_hash="src-9", created_at="2026-08-18T09:30:00+00:00"
            )
            _write_entry(index, "current", source_hash="src-2")
            _write_run(
                _db(tmp_path),
                run_id="run-1",
                started_at="2026-08-18T09:00:00+00:00",
                cache_key="prior",
                display_label="produce[a]",
            )
            _write_run(
                _db(tmp_path),
                run_id="run-1b",
                started_at="2026-08-18T09:30:00+00:00",
                cache_key="sibling",
                display_label="produce[b]",
            )
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
                display_label="produce[a]",
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["compared_with"] == {"cache_key": "prior", "strategy": "same_node"}
        assert _component(explanation, "source_hash")["prior"] == "src-1"

    def test_a_new_node_falls_back_to_the_newest_entry_for_the_function(
        self, tmp_path: Path
    ) -> None:
        with _index(tmp_path) as index:
            _write_entry(
                index, "older", source_hash="src-0", created_at="2026-08-17T09:00:00+00:00"
            )
            _write_entry(
                index, "newer", source_hash="src-1", created_at="2026-08-18T09:00:00+00:00"
            )
            _write_entry(index, "current", source_hash="src-2")
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
                display_label="produce[new]",
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["compared_with"] == {
            "cache_key": "newer",
            "strategy": "newest_by_function",
        }
        assert _component(explanation, "source_hash")["prior"] == "src-1"

    def test_a_differing_task_identity_is_named(self, tmp_path: Path) -> None:
        """Same base name, different module: the moved component is ``task``."""
        with _index(tmp_path) as index:
            _write_entry(
                index,
                "prior",
                function="analysis.produce",
                created_at="2026-08-18T09:00:00+00:00",
            )
            _write_entry(index, "current", function="pipeline.produce")
            _write_run(
                _db(tmp_path),
                run_id="run-1",
                started_at="2026-08-18T09:00:00+00:00",
                cache_key="prior",
                task_name="pipeline.produce",
            )
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
                task_name="pipeline.produce",
            )
            explanation = _explain(_db(tmp_path))

        assert _component(explanation, "task") == {
            "component": "task",
            "status": "changed",
            "current": "pipeline.produce",
            "prior": "analysis.produce",
        }
        assert explanation["reason"] == "cache_key_changed"

    def test_cached_task_and_first_run_keep_their_summaries(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _write_entry(index, "current")
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
            )
            assert _explain(_db(tmp_path))["reason"] == "no_prior_entry"

            _write_run(
                _db(tmp_path),
                run_id="run-3",
                started_at="2026-08-18T11:00:00+00:00",
                cache_key="current",
                status="cached",
            )
            assert _explain(_db(tmp_path), run_id="run-3")["reason"] == "all_inputs_match"

    def test_the_fanned_out_branch_is_named(self, tmp_path: Path) -> None:
        """Two siblings differ only by label, so the label has to be reported."""
        with _index(tmp_path) as index:
            _write_entry(index, "prior", created_at="2026-08-18T09:00:00+00:00")
            _write_entry(index, "current", source_hash="src-2")
            _write_run(
                _db(tmp_path),
                run_id="run-1",
                started_at="2026-08-18T09:00:00+00:00",
                cache_key="prior",
                display_label="produce[alpha]",
            )
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
                display_label="produce[alpha]",
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["display_label"] == "produce[alpha]"

    def test_task_without_an_entry_says_so(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _write_entry(index, "prior", created_at="2026-08-18T09:00:00+00:00")
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="missing",
            )
            assert _explain(_db(tmp_path))["reason"] == "no_entry_for_key"


class TestSavedKeyComponents:
    def test_a_saved_entry_records_the_components_explain_diffs(self, tmp_path: Path) -> None:
        """The components explain needs must be recoverable from a new entry."""

        @task()
        def produce(value: str) -> str:
            return value

        index = CacheIndex.open(path=_db(tmp_path))
        store = CacheStore(index=index, root=tmp_path / "cache")
        cache_key, input_hashes = store.build_cache_key(
            task_def=produce,
            resolved_args={"value": "a"},
            extra_source_hash="notebook-1",
        )
        store.save(
            cache_key=cache_key,
            result="a",
            task_def=produce,
            resolved_args={"value": "a"},
            input_hashes=input_hashes,
            extra_source_hash="notebook-1",
        )

        index.close()
        with Query(
            open_store(_db(tmp_path), readonly=True), layout=WorkspaceLayout.relative()
        ) as reader:
            components = reader._cache_key_components(cache_key)
        with CacheIndex.for_reading(_db(tmp_path)) as reopened:
            entry = reopened.entry(cache_key)

        assert components["extra_source_hash"] == "notebook-1"
        assert components["env_hash.pixi_lock"] is None
        assert components["inputs.value"] == input_hashes["value"]
        assert entry is not None
        assert entry["function"] == produce.name


class TestInputTrackingLabels:
    """Each input's cache-tracking label, read from ``task_inputs.tracking``."""

    def test_labels_are_reported_alongside_the_explanation(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _write_entry(index, "current")
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
            )
            _write_task_input(
                _db(tmp_path),
                run_id="run-2",
                task_id="task_0000",
                param="samples",
                tracking="path",
            )
            _write_task_input(
                _db(tmp_path),
                run_id="run-2",
                task_id="task_0000",
                param="threads",
                tracking="value",
            )
            explanation = _explain(_db(tmp_path))

        assert explanation["input_labels"] == {"samples": "path", "threads": "value"}

    def test_a_run_recorded_before_labels_existed_reports_none(self, tmp_path: Path) -> None:
        """No ``task_inputs`` rows at all: absent, not a guess (item 4)."""
        with _index(tmp_path) as index:
            _write_entry(index, "current")
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
            )
            explanation = _explain(_db(tmp_path))

        assert "input_labels" not in explanation

    def test_a_row_with_no_tracking_value_is_skipped_not_guessed(self, tmp_path: Path) -> None:
        with _index(tmp_path) as index:
            _write_entry(index, "current")
            _write_run(
                _db(tmp_path),
                run_id="run-2",
                started_at="2026-08-18T10:00:00+00:00",
                cache_key="current",
            )
            _write_task_input(
                _db(tmp_path), run_id="run-2", task_id="task_0000", param="legacy", tracking=None
            )
            explanation = _explain(_db(tmp_path))

        assert "input_labels" not in explanation


class TestExplainTextRendering:
    """``ginkgo cache explain`` without ``--json``: one reading of the payload."""

    def _rendered(self, payload: dict[str, Any]) -> str:
        from rich.console import Console

        output = StringIO()
        console = Console(file=output, width=120, force_terminal=False)
        _render_explain_text(console, payload)
        return output.getvalue()

    def test_a_path_label_is_named_with_its_hint(self) -> None:
        payload = {
            "run_id": "run-2",
            "workflow": "flow.py",
            "tasks": [
                {
                    "task_id": "task_0000",
                    "task_name": "produce",
                    "display_label": None,
                    "cache_key": "abc123",
                    "reason": "all_inputs_match",
                    "input_labels": {"samples": "path", "threads": "value"},
                }
            ],
        }
        text = self._rendered(payload)

        assert "samples" in text
        assert "path" in text
        assert "tracked by path string only" in text
        assert "threads" in text
        assert "value" in text

    def test_markup_looking_values_are_escaped_not_swallowed(self) -> None:
        """A param or label that looks like Rich markup must print literally."""
        payload = {
            "run_id": "run-2",
            "workflow": None,
            "tasks": [
                {
                    "task_id": "task_0000",
                    "task_name": "[file]",
                    "display_label": None,
                    "cache_key": None,
                    "reason": "no_entry_for_key",
                    "input_labels": {"[weird]": "value"},
                }
            ],
        }
        text = self._rendered(payload)

        assert "[file]" in text
        assert "[weird]" in text

    def test_a_task_with_no_labels_prints_nothing_extra(self) -> None:
        """An entry from before labelling existed: no inputs section, no crash."""
        payload = {
            "run_id": "run-2",
            "workflow": "flow.py",
            "tasks": [
                {
                    "task_id": "task_0000",
                    "task_name": "produce",
                    "display_label": None,
                    "cache_key": "abc123",
                    "reason": "all_inputs_match",
                }
            ],
        }
        text = self._rendered(payload)

        assert "produce" in text
        assert "inputs:" not in text

    def test_no_tasks_says_so_rather_than_printing_nothing(self) -> None:
        text = self._rendered({"run_id": "run-2", "workflow": None, "tasks": []})
        assert "No tasks found" in text
