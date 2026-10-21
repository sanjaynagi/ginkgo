"""How the cache key hashes Python values: container subclasses and large plain data."""

from __future__ import annotations

from collections import OrderedDict, defaultdict, namedtuple
from pathlib import Path
from typing import Any

import pytest

from ginkgo import task
from ginkgo.core.types import file
from ginkgo.runtime.caching import cache
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.task_validation import label_input_value

Pair = namedtuple("Pair", "x y")


@pytest.fixture
def store(tmp_path: Path) -> CacheStore:
    return CacheStore(index=CacheIndex.in_memory(), root=tmp_path / "cache")


class TestContainerSubclasses:
    @pytest.mark.parametrize(
        ("subclass_value", "builtin_value"),
        [
            (Pair(1, 2), (1, 2)),
            (OrderedDict(a=1), {"a": 1}),
            (defaultdict(list, {"a": [1]}), {"a": [1]}),
        ],
        ids=["namedtuple", "ordereddict", "defaultdict"],
    )
    def test_a_subclass_never_keys_the_same_as_its_builtin(
        self, store, subclass_value, builtin_value
    ) -> None:
        subclass_hash = store._hash_value(annotation=Any, value=subclass_value)
        builtin_hash = store._hash_value(annotation=Any, value=builtin_value)

        assert subclass_hash != builtin_hash
        assert subclass_hash["type"] == (
            f"{type(subclass_value).__module__}.{type(subclass_value).__name__}"
        )

    def test_a_file_in_a_namedtuple_is_still_tracked_by_content(self, store, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("a", encoding="utf-8")
        before = store._hash_value(annotation=Any, value=Pair(file(path), 1))
        path.write_text("b", encoding="utf-8")
        store.hash_memo = None

        after = store._hash_value(annotation=Any, value=Pair(file(path), 1))

        assert before["items"][0]["type"] == "file"
        assert before != after


NESTED = {
    "k1": {"a": 1, "b": [1, 2.5, None, True]},
    "k2": ({"c": "text"}, [("x", 2)]),
}


class TestPlainDataFastPath:
    @pytest.mark.parametrize("annotation", [Any, dict, list, list[Any]])
    def test_nested_plain_data_hashes_as_the_general_walk_does(
        self, store, monkeypatch, annotation
    ) -> None:
        value = [NESTED, NESTED] if annotation is not dict else NESTED
        fast = store._hash_value(annotation=annotation, value=value)

        monkeypatch.setattr(cache, "_plain_data_hash", lambda **_: None)
        slow = store._hash_value(annotation=annotation, value=value)

        assert fast == slow

    def test_a_path_shaped_string_leaves_the_general_walk_to_decide(self, store, tmp_path) -> None:
        path = tmp_path / "data.csv"
        path.write_text("a", encoding="utf-8")

        hashed = store._hash_value(annotation=Any, value=[{"input": str(path)}])

        assert hashed["items"][0]["items"][0]["value"]["type"] == "file"

    def test_nested_plain_data_is_labelled_by_value(self) -> None:
        assert label_input_value(annotation=dict, value=NESTED) == "value"

    def test_a_bare_word_naming_a_file_is_labelled_path(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "k1").write_text("", encoding="utf-8")

        assert label_input_value(annotation=dict, value=NESTED) == "path"


@task()
def _consume(values: dict, path: file) -> int:
    return len(values)


def test_the_stat_fingerprint_reuses_the_key_s_input_hashes(store, tmp_path) -> None:
    path = tmp_path / "a.txt"
    path.write_text("a", encoding="utf-8")
    args = {"values": NESTED, "path": file(path)}
    _key, input_hashes = store.build_cache_key(task_def=_consume, resolved_args=args)

    reused = store.stat_fingerprint(
        task_def=_consume, resolved_args=args, input_hashes=input_hashes
    )

    assert reused == store.stat_fingerprint(task_def=_consume, resolved_args=args)
