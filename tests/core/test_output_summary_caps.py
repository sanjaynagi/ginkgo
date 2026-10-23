"""Output summaries stay small for large values but keep every path."""

from __future__ import annotations

from pathlib import Path

from ginkgo import task
from ginkgo.core.types import file
from ginkgo.runtime.artifacts.output_index import output_summary
from ginkgo.runtime.artifacts.value_codec import SUMMARY_ITEM_LIMIT, summarise_value
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from tests.conftest import Ledger


def test_summarise_value_keeps_the_first_items_and_every_path() -> None:
    summary = summarise_value([*range(100), file("late.txt")])

    assert summary["length"] == 101
    assert summary["omitted"] == 100 - SUMMARY_ITEM_LIMIT
    assert summary["items"][:3] == [0, 1, 2]
    assert summary["items"][-1] == {"type": "file", "value": "late.txt"}


def test_output_summary_counts_omitted_values_but_keeps_files() -> None:
    entries = output_summary(list, [*range(100), file("late.txt")])

    assert len(entries) == SUMMARY_ITEM_LIMIT + 2
    assert entries[-2] == {"name": "return[100]", "type": "file", "path": "late.txt"}
    assert entries[-1] == {"name": "return", "type": "truncated", "omitted": 80}


@task()
def big_list(n: int) -> list:
    return list(range(n))


def test_a_long_returned_list_keeps_the_ledger_and_manifest_small(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path)
    try:
        ConcurrentEvaluator(jobs=1, run_dir=ledger.run_dir, event_bus=ledger.bus).evaluate(
            big_list(n=5_000)
        )
        outputs = ledger.task()["output_summary"]
        ledger.finish()
    finally:
        ledger.close()

    assert len(outputs) == SUMMARY_ITEM_LIMIT + 1
    assert (ledger.path / "manifest.yaml").stat().st_size < 20_000
