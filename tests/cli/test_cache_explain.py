"""Tests for the per-component cache-key diff behind ``ginkgo cache explain``.

Entries are planted as rows, because rows are the only cache index there is.
Each case records one run whose task wrote ``current`` and earlier runs or
entries to compare it against, then checks the explanation.
"""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from rich.console import Console

from ginkgo import task
from ginkgo.cli.commands.cache import _render_explain_text, explain_run_cache
from ginkgo.query import Query
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

T0, T1, T1B, T2, T3 = (
    "2026-08-17T09:00:00+00:00",
    "2026-08-18T09:00:00+00:00",
    "2026-08-18T09:30:00+00:00",
    "2026-08-18T10:00:00+00:00",
    "2026-08-18T11:00:00+00:00",
)
ABSENT = object()


def _meta(cache_key: str, **fields: Any) -> dict[str, Any]:
    """Return the facts one cache entry records."""
    return {
        "cache_key": cache_key,
        "function": "produce",
        "version": "v1",
        "source_hash": "src-1",
        "extra_source_hash": None,
        "env": None,
        "env_hash": None,
        "input_hashes": {"samples": "hash-a", "threads": "hash-t"},
        "created_at": T2,
        **fields,
    }


def _run(run_id: str, started_at: str, cache_key: str, **task: Any) -> dict[str, Any]:
    """Describe one run with a single task that used *cache_key*."""
    return {"run_id": run_id, "started_at": started_at, "cache_key": cache_key, **task}


def _apply(db: Path, sql: str, params: tuple) -> None:
    with open_store(db) as store, store.transaction():
        store.apply([ProjectionOp(sql=sql, params=params)])


def _explain(
    tmp_path: Path,
    *,
    entries: list[tuple[str, dict]],
    runs: list[dict],
    inputs: tuple[tuple[str, str, str | None], ...] = (),
    rerun_requested: bool = False,
    run_id: str = "run-2",
) -> dict[str, Any]:
    """Plant *entries*, *runs* and their *inputs*, then explain *run_id*'s task."""
    db = tmp_path / "ginkgo.db"
    with CacheIndex.open(path=db) as index:
        for cache_key, fields in entries:
            index.record_entry(
                cache_key=cache_key,
                meta=_meta(cache_key, **fields),
                artifact_ids={},
                size_bytes=0,
                run_id=None,
            )
        for run in runs:
            _apply(
                db,
                "INSERT INTO runs (run_id, workflow, status, started_at) "
                "VALUES (?, 'workflow.py', 'succeeded', ?)",
                (run["run_id"], run["started_at"]),
            )
            _apply(
                db,
                "INSERT INTO tasks (run_id, task_id, node_id, name, display_label, kind, "
                "execution_mode, status, cache_key, attempts) "
                "VALUES (?, 'task_0000', 0, ?, ?, 'task', 'thread', ?, ?, 1)",
                (
                    run["run_id"],
                    run.get("name", "produce"),
                    run.get("label"),
                    run.get("status", "succeeded"),
                    run["cache_key"],
                ),
            )
        for input_run, param, tracking in inputs:
            _apply(
                db,
                "INSERT INTO task_inputs (run_id, task_id, param, tracking) "
                "VALUES (?, 'task_0000', ?, ?)",
                (input_run, param, tracking),
            )
        if rerun_requested:
            _apply(
                db,
                "INSERT INTO events (run_id, ts, type, task_id, payload) "
                "VALUES (?, ?, 'task_cache_miss', 'task_0000', ?)",
                (run_id, T2, json.dumps({"rerun_requested": True})),
            )
        with Query(open_store(db, readonly=True), layout=WorkspaceLayout.relative()) as reader:
            (explanation,) = explain_run_cache(reader=reader, run_id=run_id)["tasks"]
    return explanation


def _two_runs(**current: Any) -> dict[str, Any]:
    """A prior run and the current one, each with its own entry."""
    return {
        "entries": [("prior", {"created_at": T1}), ("current", current)],
        "runs": [_run("run-1", T1, "prior"), _run("run-2", T2, "current")],
    }


def _text(explanation: dict[str, Any]) -> str:
    out = StringIO()
    _render_explain_text(Console(file=out, width=300), {"run_id": "run-2", "tasks": [explanation]})
    return out.getvalue()


def _changed(component: str, current: str, prior: str) -> dict[str, str]:
    return {"component": component, "status": "changed", "current": current, "prior": prior}


_CASES = {
    "source-hash": (
        _two_runs(source_hash="src-2"),
        {
            "reason": "source_hash_changed",
            "compared_with": {"cache_key": "prior", "strategy": "same_node"},
            "components": [_changed("source_hash", "src-2", "src-1")],
        },
    ),
    "input-changed": (
        _two_runs(input_hashes={"samples": "hash-b", "threads": "hash-t"}),
        {
            "reason": "input_changed",
            "components": [_changed("inputs.samples", "hash-b", "hash-a")],
            "text": ["reason: input samples changed", "inputs.samples (changed: hash-a → hash-b)"],
        },
    ),
    "input-added": (
        _two_runs(input_hashes={"samples": "hash-a", "threads": "hash-t", "seed": "hash-s"}),
        {
            "reason": "input_changed",
            "components": [{"component": "inputs.seed", "status": "added", "current": "hash-s"}],
        },
    ),
    "env-changed": (
        {
            "entries": [
                (
                    "prior",
                    {
                        "env": "bio",
                        "env_hash": {"env": "bio", "pixi_lock": "m-1"},
                        "created_at": T1,
                    },
                ),
                ("current", {"env": "bio", "env_hash": {"env": "bio", "pixi_lock": "m-2"}}),
            ],
            "runs": _two_runs()["runs"],
        },
        {
            "reason": "env_changed",
            "components": [_changed("env_hash.pixi_lock", "m-2", "m-1")],
        },
    ),
    "version-bump": (_two_runs(version="v2"), {"reason": "version_bump"}),
    # Issue #223: a sibling branch is not this node's history, however recent.
    "same-node-beats-sibling": (
        {
            "entries": [
                ("prior", {"created_at": T1}),
                ("sibling", {"source_hash": "src-9", "created_at": T1B}),
                ("current", {"source_hash": "src-2"}),
            ],
            "runs": [
                _run("run-1", T1, "prior", label="produce[a]"),
                _run("run-1b", T1B, "sibling", label="produce[b]"),
                _run("run-2", T2, "current", label="produce[a]"),
            ],
        },
        {"compared_with": {"cache_key": "prior", "strategy": "same_node"}},
    ),
    # A sibling is context, not history: nothing of this node's own changed.
    "new-node-falls-back-to-the-function": (
        {
            "entries": [
                ("older", {"source_hash": "src-0", "created_at": T0}),
                ("newer", {"source_hash": "src-1", "created_at": T1}),
                ("current", {"source_hash": "src-2"}),
            ],
            "runs": [_run("run-2", T2, "current", label="produce[new]")],
        },
        {
            "reason": "no_prior_entry",
            "details": ["source_hash_changed"],
            "compared_with": {"cache_key": "newer", "strategy": "most_similar_by_function"},
        },
    ),
    "fallback-differs-least": (
        {
            "entries": [
                ("same-branch", {"source_hash": "src-0", "created_at": T0}),
                (
                    "other-branch",
                    {"input_hashes": {"samples": "hash-b", "threads": "hash-t"}, "created_at": T1},
                ),
                ("current", {"source_hash": "src-0", "version": "v2"}),
            ],
            "runs": [_run("run-2", T2, "current", label="produce[new]")],
        },
        {
            "details": ["version_bump"],
            "compared_with": {"cache_key": "same-branch", "strategy": "most_similar_by_function"},
        },
    ),
    # Same base name, different module: the moved component is ``task``.
    "task-identity": (
        {
            "entries": [
                ("prior", {"function": "analysis.produce", "created_at": T1}),
                ("current", {"function": "pipeline.produce"}),
            ],
            "runs": [
                _run("run-1", T1, "prior", name="pipeline.produce"),
                _run("run-2", T2, "current", name="pipeline.produce"),
            ],
        },
        {
            "reason": "cache_key_changed",
            "components": [_changed("task", "pipeline.produce", "analysis.produce")],
        },
    ),
    # EV-6: moving a task restarts its cache; say so, not "no earlier run".
    "task-moved": (
        {
            "entries": [
                ("prior", {"function": "workflow.old.produce"}),
                ("current", {"function": "workflow.new.produce"}),
            ],
            "runs": [
                _run("run-1", T1, "prior", name="workflow.old.produce"),
                _run("run-2", T2, "current", name="workflow.new.produce"),
            ],
        },
        {
            "reason": "task_moved",
            "moved_from": "workflow.old.produce",
            "text": ["earlier runs knew it as workflow.old.produce"],
        },
    ),
    # EV5-6: the branch label matches, the module does not.
    "fanned-out-task-moved": (
        {
            "entries": [
                ("prior", {"function": "old.produce", "created_at": T1}),
                ("current", {"function": "new.produce", "source_hash": "src-2"}),
            ],
            "runs": [
                _run("run-1", T1, "prior", name="old.produce", label="produce[a]"),
                _run("run-2", T2, "current", name="new.produce", label="produce[a]"),
            ],
        },
        {"reason": "task_moved", "moved_from": "old.produce", "display_label": "produce[a]"},
    ),
    # EV5-6: same node, same parameters, a name the run no longer has.
    "renamed-task": (
        {
            "entries": [("current", {"function": "m.make"})],
            "runs": [
                _run("run-1", T1, "prior", name="m.produce"),
                _run("run-2", T2, "current", name="m.make"),
            ],
            "inputs": (("run-1", "samples", "value"), ("run-2", "samples", "value")),
        },
        {"reason": "task_moved", "moved_from": "m.produce"},
    ),
    # EV5-7: the task's own text is unchanged; its module moved.
    "module-coupling": (
        {
            "entries": [
                ("prior", {"extra": {"own_source_hash": "own-1"}, "created_at": T1}),
                ("current", {"source_hash": "src-2", "extra": {"own_source_hash": "own-1"}}),
            ],
            "runs": _two_runs()["runs"],
        },
        {"reason": "module_source_changed", "text": ["another task defined in the same file"]},
    ),
    "first-run": (
        {"entries": [("current", {})], "runs": [_run("run-2", T2, "current")]},
        {"reason": "no_prior_entry", "input_labels": ABSENT},
    ),
    "cached-run": (
        {
            "entries": [("current", {})],
            "runs": [_run("run-2", T2, "current"), _run("run-3", T3, "current", status="cached")],
            "run_id": "run-3",
        },
        {"reason": "all_inputs_match"},
    ),
    "no-entry-for-key": (
        {"entries": [("prior", {"created_at": T1})], "runs": [_run("run-2", T2, "missing")]},
        {"reason": "no_entry_for_key"},
    ),
    # EV5-1: the node ran under this key before; its entry was pruned.
    "pruned-entry": (
        {
            "entries": [("older", {"source_hash": "src-0", "created_at": T0}), ("current", {})],
            "runs": [
                _run("run-0", T0, "older"),
                _run("run-1", T1, "current"),
                _run("run-2", T2, "current"),
            ],
        },
        {"reason": "no_entry_for_key", "components": ABSENT},
    ),
    "prior-entry-removed": (
        {
            "entries": [("current", {"source_hash": "src-2"})],
            "runs": [_run("run-1", T1, "pruned"), _run("run-2", T2, "current")],
        },
        {"reason": "prior_entry_removed", "components": []},
    ),
    # EV5-2: --rerun never consulted the cache, so no key diff applies.
    "forced-rerun": (
        {**_two_runs(source_hash="src-2"), "rerun_requested": True},
        {"reason": "rerun_requested"},
    ),
    "input-labels": (
        {
            "entries": [("current", {})],
            "runs": [_run("run-2", T2, "current")],
            "inputs": (("run-2", "samples", "path"), ("run-2", "threads", "value")),
        },
        {"input_labels": {"samples": "path", "threads": "value"}},
    ),
    # A row with no tracking value is skipped, not guessed.
    "untracked-input-row": (
        {
            "entries": [("current", {})],
            "runs": [_run("run-2", T2, "current")],
            "inputs": (("run-2", "legacy", None),),
        },
        {"input_labels": ABSENT},
    ),
}


@pytest.mark.parametrize(("scenario", "expected"), _CASES.values(), ids=_CASES.keys())
def test_explanation(tmp_path: Path, scenario: dict, expected: dict) -> None:
    explanation = _explain(tmp_path, **scenario)
    for key, value in expected.items():
        if key == "text":
            text = _text(explanation)
            assert all(phrase in text for phrase in value), text
        else:
            assert explanation.get(key, ABSENT) == value, (key, explanation)


def _store(tmp_path: Path) -> tuple[CacheIndex, CacheStore]:
    index = CacheIndex.open(path=tmp_path / "ginkgo.db")
    return index, CacheStore(index=index, root=tmp_path / "cache")


@task()
def produce(value: str) -> str:
    return value


def _save(store: CacheStore, value: str, extra: str = "notebook-1") -> tuple[str, dict]:
    args = {"value": value}
    cache_key, input_hashes = store.build_cache_key(
        task_def=produce, resolved_args=args, extra_source_hash=extra
    )
    store.save(
        cache_key=cache_key,
        result=value,
        task_def=produce,
        resolved_args=args,
        input_hashes=input_hashes,
        extra_source_hash=extra,
    )
    return cache_key, input_hashes


def test_a_miss_is_explained_against_the_entry_that_differs_least(tmp_path: Path) -> None:
    """A fan-out sibling written last must not read as an input change."""
    index, store = _store(tmp_path)
    cache_key, input_hashes = _save(store, "a")
    _save(store, "b")
    reasons = store.miss_reasons(
        cache_key=cache_key + "-moved",
        task_def=produce,
        input_hashes=input_hashes,
        extra_source_hash="notebook-2",
    )
    index.close()
    assert reasons == ["source_hash_changed"]


def test_a_saved_entry_records_the_components_explain_diffs(tmp_path: Path) -> None:
    index, store = _store(tmp_path)
    cache_key, input_hashes = _save(store, "a")
    index.close()

    db = tmp_path / "ginkgo.db"
    with Query(open_store(db, readonly=True), layout=WorkspaceLayout.relative()) as reader:
        components = reader._cache_key_components(cache_key)
    with CacheIndex.for_reading(db) as reopened:
        entry = reopened.entry(cache_key)

    assert components["extra_source_hash"] == "notebook-1"
    assert components["env_hash.pixi_lock"] is None
    assert components["inputs.value"] == input_hashes["value"]
    assert entry is not None and entry["function"] == produce.name


@pytest.mark.parametrize(
    ("tasks", "present", "absent"),
    [
        (
            [{"task_name": "produce", "input_labels": {"samples": "path", "threads": "value"}}],
            ["samples", "tracked by path string only", "threads", "value"],
            [],
        ),
        # A param or label that looks like Rich markup must print literally.
        (
            [{"task_name": "[file]", "input_labels": {"[weird]": "value"}}],
            ["[file]", "[weird]"],
            [],
        ),
        # An entry from before labelling existed: no inputs section, no crash.
        ([{"task_name": "produce"}], ["produce"], ["inputs:"]),
        ([], ["No tasks found"], []),
    ],
)
def test_explain_text_rendering(tasks: list, present: list, absent: list) -> None:
    payload = {
        "run_id": "run-2",
        "workflow": "flow.py",
        "tasks": [
            {
                "task_id": "task_0000",
                "display_label": None,
                "cache_key": "abc123",
                "reason": "all_inputs_match",
                **task,
            }
            for task in tasks
        ],
    }
    out = StringIO()
    _render_explain_text(Console(file=out, width=120, force_terminal=False), payload)
    assert all(phrase in out.getvalue() for phrase in present)
    assert not any(phrase in out.getvalue() for phrase in absent)
