"""A large plain input costs the ledger a few hundred bytes, not a row per element."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import task
from ginkgo.core.types import file
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.event_values import render_value
from ginkgo.runtime.task_validation import TaskValidator, label_input_value
from ginkgo.store import open_store
from tests.conftest import Ledger


@task()
def numbers(n: int) -> list[int]:
    return list(range(n))


@task()
def total(xs: list[int]) -> int:
    return sum(xs)


def _evaluate_and_measure(root: Path) -> dict[str, int]:
    """Run ``total(xs=numbers(...))`` once; return the largest records it left."""
    ledger = Ledger.start(root=root)
    try:
        result = ConcurrentEvaluator(
            jobs=1, run_dir=ledger.run_dir, event_bus=ledger.bus
        ).evaluate(total(xs=numbers(n=200_000)))
        ledger.finish()
        with open_store(ledger.db, readonly=True) as store:
            events = store.query("SELECT max(length(payload)) AS size FROM events")
            entries = store.query(
                "SELECT max(length(input_hashes) + length(inputs)) AS size FROM cache_entries"
            )
            inputs = store.query("SELECT max(length(value_summary)) AS size FROM task_inputs")
    finally:
        ledger.close()
    assert result == sum(range(200_000))
    return {
        "event": events[0]["size"],
        "cache_entry": entries[0]["size"],
        "task_input": inputs[0]["size"],
        "manifest": (ledger.path / "manifest.yaml").stat().st_size,
    }


def test_a_long_list_input_keeps_every_record_small(tmp_path: Path) -> None:
    cold = _evaluate_and_measure(tmp_path)
    warm = _evaluate_and_measure(tmp_path)

    for sizes in (cold, warm):
        assert sizes["event"] < 5_000
        assert sizes["cache_entry"] < 2_000
        assert sizes["task_input"] < 1_000
        assert sizes["manifest"] < 20_000


def test_a_container_without_paths_hashes_as_one_digest(tmp_path: Path) -> None:
    store = CacheStore(index=CacheIndex.in_memory(), root=tmp_path / "cache")

    digest = store._hash_value(annotation=list[int], value=list(range(1_000)))
    changed = store._hash_value(annotation=list[int], value=[*range(999), -1])
    mapping = store._hash_value(annotation=dict, value={"a": [1, 2], "b": "text"})

    assert set(digest) == {"digest", "type"}
    assert digest != changed
    assert set(mapping) == {"digest", "type"}
    assert store._hash_value(annotation=list, value=[1]) != store._hash_value(
        annotation=tuple, value=(1,)
    )


def test_a_container_holding_a_file_keeps_an_entry_per_element(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a")
    store = CacheStore(index=CacheIndex.in_memory(), root=tmp_path / "cache")

    hashed = store._hash_value(annotation=list, value=[file(tmp_path / "a.txt"), 3])

    assert hashed["items"][0]["type"] == "file"
    assert hashed["items"][1]["type"] == "int"


def test_render_value_keeps_the_first_items_of_a_long_container() -> None:
    rendered = render_value({"xs": list(range(1_000)), "short": [1, 2]})

    assert rendered["short"] == [1, 2]
    assert rendered["xs"]["length"] == 1_000
    assert rendered["xs"]["items"][:3] == [0, 1, 2]
    assert rendered["xs"]["omitted"] == 1_000 - len(rendered["xs"]["items"])
    assert render_value(list(range(1_000)), item_limit=None) == list(range(1_000))


def test_a_list_of_numbers_is_labelled_by_value() -> None:
    assert label_input_value(annotation=list[int], value=list(range(1_000))) == "value"


@pytest.mark.parametrize("bad", ["3", True, 2.5])
def test_list_int_validation_still_rejects_a_wrong_element(bad: object) -> None:
    with pytest.raises(TypeError, match=r"xs\[1\]"):
        TaskValidator().validate_annotated_value(
            annotation=list[int], value=[1, bad, 3], label="xs"
        )
