"""Tests for the per-component cache-key diff behind ``ginkgo cache explain``."""

import json
from pathlib import Path
from typing import Any

from ginkgo import task
from ginkgo.cli.commands.cache import explain_run_cache
from ginkgo.runtime.caching.cache import CacheStore


def _write_entry(cache_root: Path, cache_key: str, **fields: Any) -> None:
    """Write a cache entry whose meta.json carries the given key components."""
    meta: dict[str, Any] = {
        "cache_key": cache_key,
        "function": "produce",
        "version": "v1",
        "source_hash": "src-1",
        "extra_source_hash": None,
        "env": None,
        "env_hash": None,
        "input_hashes": {"samples": "hash-a", "threads": "hash-t"},
        "timestamp": "2026-08-18T10:00:00+00:00",
    }
    meta.update(fields)
    entry = cache_root / cache_key
    entry.mkdir(parents=True)
    (entry / "meta.json").write_text(json.dumps(meta), encoding="utf-8")


def _explain(
    cache_root: Path, cache_key: str = "current", task_name: str = "produce"
) -> dict[str, Any]:
    """Explain a single ran task whose entry is ``cache_key``."""
    payload = explain_run_cache(
        cache_root=cache_root,
        run_snapshot={
            "run_id": "run-1",
            "workflow": "workflow.py",
            "tasks": [
                {
                    "task_id": "produce-1",
                    "task_name": task_name,
                    "cache_key": cache_key,
                    "status": "succeeded",
                }
            ],
        },
    )
    tasks = payload["tasks"]
    assert isinstance(tasks, list)
    return tasks[0]


def _component(explanation: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the reported diff for one named component."""
    components = explanation["components"]
    matches = [entry for entry in components if entry["component"] == name]
    assert matches, f"{name} not reported in {components}"
    return matches[0]


class TestCacheExplainComponents:
    def test_source_hash_change_is_named(self, tmp_path: Path) -> None:
        _write_entry(tmp_path, "current", source_hash="src-2")
        _write_entry(tmp_path, "prior", source_hash="src-1")

        explanation = _explain(tmp_path)

        assert explanation["reason"] == "source_hash_changed"
        assert explanation["compared_with"] == "prior"
        assert _component(explanation, "source_hash") == {
            "component": "source_hash",
            "status": "changed",
            "current": "src-2",
            "prior": "src-1",
        }
        assert [entry["component"] for entry in explanation["components"]] == ["source_hash"]

    def test_input_change_names_the_parameter(self, tmp_path: Path) -> None:
        _write_entry(tmp_path, "current", input_hashes={"samples": "hash-b", "threads": "hash-t"})
        _write_entry(tmp_path, "prior")

        explanation = _explain(tmp_path)

        assert explanation["reason"] == "input_changed"
        assert _component(explanation, "inputs.samples")["status"] == "changed"
        assert [entry["component"] for entry in explanation["components"]] == ["inputs.samples"]

    def test_added_input_parameter_is_named(self, tmp_path: Path) -> None:
        _write_entry(
            tmp_path,
            "current",
            input_hashes={"samples": "hash-a", "threads": "hash-t", "seed": "hash-s"},
        )
        _write_entry(tmp_path, "prior")

        explanation = _explain(tmp_path)

        assert explanation["reason"] == "input_changed"
        assert _component(explanation, "inputs.seed") == {
            "component": "inputs.seed",
            "status": "added",
            "current": "hash-s",
        }

    def test_environment_identity_change_is_named(self, tmp_path: Path) -> None:
        _write_entry(
            tmp_path, "current", env="bio", env_hash={"env": "bio", "pixi_lock": "manifest-2"}
        )
        _write_entry(
            tmp_path, "prior", env="bio", env_hash={"env": "bio", "pixi_lock": "manifest-1"}
        )

        explanation = _explain(tmp_path)

        assert explanation["reason"] == "env_changed"
        assert _component(explanation, "env_hash.pixi_lock") == {
            "component": "env_hash.pixi_lock",
            "status": "changed",
            "current": "manifest-2",
            "prior": "manifest-1",
        }

    def test_version_bump_is_named(self, tmp_path: Path) -> None:
        _write_entry(tmp_path, "current", version="v2")
        _write_entry(tmp_path, "prior")

        explanation = _explain(tmp_path)

        assert explanation["reason"] == "version_bump"
        assert _component(explanation, "version")["status"] == "changed"

    def test_unrecorded_component_is_reported_as_unknown(self, tmp_path: Path) -> None:
        """An entry predating a recorded field must not read as unchanged."""
        _write_entry(tmp_path, "current", env="bio", env_hash={"pixi_lock": "manifest-1"})
        prior = {
            "cache_key": "prior",
            "function": "produce",
            "version": "v1",
            "source_hash": "src-1",
            "env": "bio",
            "input_hashes": {"samples": "hash-a", "threads": "hash-t"},
            "timestamp": "2026-08-18T09:00:00+00:00",
        }
        (tmp_path / "prior").mkdir(parents=True)
        (tmp_path / "prior" / "meta.json").write_text(json.dumps(prior), encoding="utf-8")

        explanation = _explain(tmp_path)

        assert explanation["reason"] == "cache_key_changed"
        for name in ("env_hash.pixi_lock", "extra_source_hash"):
            reported = _component(explanation, name)
            assert reported["status"] == "not_recorded"
            assert "prior entry's meta.json" in reported["detail"]

    def test_missing_input_hashes_are_reported_as_unknown(self, tmp_path: Path) -> None:
        _write_entry(tmp_path, "current")
        _write_entry(tmp_path, "prior", input_hashes=None)

        explanation = _explain(tmp_path)

        assert _component(explanation, "inputs")["status"] == "not_recorded"
        assert explanation["reason"] == "cache_key_changed"

    def test_prior_entry_is_the_newest_sibling_written_before_the_current_one(
        self, tmp_path: Path
    ) -> None:
        """A sibling written after this entry cannot be what it superseded."""
        _write_entry(
            tmp_path, "current", source_hash="src-3", timestamp="2026-08-18T12:00:00+00:00"
        )
        _write_entry(
            tmp_path, "aaa-newest", source_hash="src-2", timestamp="2026-08-18T11:00:00+00:00"
        )
        _write_entry(
            tmp_path, "zzz-oldest", source_hash="src-1", timestamp="2026-08-17T11:00:00+00:00"
        )
        _write_entry(
            tmp_path, "later-sibling", source_hash="src-4", timestamp="2026-08-18T13:00:00+00:00"
        )

        explanation = _explain(tmp_path)

        assert explanation["compared_with"] == "aaa-newest"
        assert _component(explanation, "source_hash")["prior"] == "src-2"

    def test_a_differing_task_identity_is_named(self, tmp_path: Path) -> None:
        """Same base name, different module: the moved component is ``task``."""
        _write_entry(tmp_path, "current", function="pipeline.produce")
        _write_entry(tmp_path, "prior", function="analysis.produce")

        explanation = _explain(tmp_path, task_name="pipeline.produce")

        assert _component(explanation, "task") == {
            "component": "task",
            "status": "changed",
            "current": "pipeline.produce",
            "prior": "analysis.produce",
        }
        assert explanation["reason"] == "cache_key_changed"

    def test_cached_task_and_first_run_keep_their_summaries(self, tmp_path: Path) -> None:
        _write_entry(tmp_path, "current")
        assert _explain(tmp_path)["reason"] == "no_prior_entry"

        payload = explain_run_cache(
            cache_root=tmp_path,
            run_snapshot={
                "tasks": [{"task_name": "produce", "cache_key": "current", "status": "cached"}]
            },
        )
        tasks = payload["tasks"]
        assert isinstance(tasks, list)
        assert tasks[0]["reason"] == "all_inputs_match"

    def test_task_without_an_entry_says_so(self, tmp_path: Path) -> None:
        _write_entry(tmp_path, "prior")
        assert _explain(tmp_path, cache_key="missing")["reason"] == "no_entry_for_key"


class TestSavedKeyComponents:
    def test_meta_records_the_driver_source_hash_and_env_identity(self, tmp_path: Path) -> None:
        """The components explain needs must be recoverable from a new entry."""

        @task()
        def produce(value: str) -> str:
            return value

        store = CacheStore(root=tmp_path / "cache")
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

        meta = json.loads(
            (tmp_path / "cache" / cache_key / "meta.json").read_text(encoding="utf-8")
        )
        assert meta["extra_source_hash"] == "notebook-1"
        assert meta["env_hash"] is None
