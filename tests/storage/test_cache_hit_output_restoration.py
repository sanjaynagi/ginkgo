"""A cache hit puts the cached output bytes back on disk (issue #331).

Every return form that hands back a tracked file has an artifact behind it,
and a cache hit must leave the working tree holding that artifact's bytes
whatever happened to the file in between: an edit, a truncation, a deletion,
or a later run with a different parameter overwriting it. The task body must
not run again to get there.

A body run is counted by a line appended to ``body_runs.txt`` in the test's
working directory, so "the hit skipped the body" is checked directly rather
than inferred from events. The bytes each cached run wrote are the bytes its
artifact holds, so the payload is what the restored file must read.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from ginkgo import Out, asset, evaluate, file, task, untracked
from tests.conftest import EventCollector

BODY_RUNS = Path("body_runs.txt")


def _write(*, payload: str, dest: str) -> Path:
    """Record one body run and write *payload* to *dest*."""
    with BODY_RUNS.open("a", encoding="utf-8") as handle:
        handle.write("run\n")
    path = Path(dest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return path


def _body_runs() -> int:
    """Return how many times a task body has run in this test."""
    if not BODY_RUNS.exists():
        return 0
    return len(BODY_RUNS.read_text(encoding="utf-8").splitlines())


# ---------------------------------------------------------------------------
# Module-level tasks, one per return form
# ---------------------------------------------------------------------------


@task()
def returns_file(payload: str, dest: Out[file]) -> file:
    return file(str(_write(payload=payload, dest=dest)))


@task()
def returns_asset_of_out(payload: str, dest: Out[file]) -> file:
    return asset(_write(payload=payload, dest=dest), name="restoration/out")


@task()
def returns_inferred(payload: str, dest: Out[file]):
    _write(payload=payload, dest=dest)


@task()
def returns_asset_of_plain_path(payload: str, dest: untracked) -> file:
    return asset(_write(payload=payload, dest=dest), name="restoration/plain")


@task()
def returns_none(payload: str, dest: Out[file]) -> None:
    _write(payload=payload, dest=dest)


RESTORING_FORMS = {
    "file": returns_file,
    "asset-of-out": returns_asset_of_out,
    "inferred": returns_inferred,
    "asset-of-plain-path": returns_asset_of_plain_path,
}


def _edit(path: Path) -> None:
    path.write_text("tampered", encoding="utf-8")


def _truncate(path: Path) -> None:
    path.write_bytes(b"")


def _delete(path: Path) -> None:
    path.unlink()


MUTATIONS: dict[str, Callable[[Path], None]] = {
    "edit": _edit,
    "truncate": _truncate,
    "delete": _delete,
}


def _run(producer: object, *, payload: str, dest: Path) -> EventCollector:
    """Evaluate one call of *producer* and return the events it published."""
    collector = EventCollector()
    evaluate(producer(payload=payload, dest=str(dest)), event_bus=collector.bus)
    return collector


# ---------------------------------------------------------------------------
# Return forms that are restored from the artifact store
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mutation", MUTATIONS)
@pytest.mark.parametrize("form", RESTORING_FORMS)
def test_a_mutated_output_is_restored_on_a_cache_hit(
    form: str, mutation: str, tmp_path: Path
) -> None:
    producer = RESTORING_FORMS[form]
    dest = tmp_path / "out" / "result.txt"
    _run(producer, payload="A", dest=dest)

    MUTATIONS[mutation](dest)
    hit = _run(producer, payload="A", dest=dest)

    assert hit.cached()
    assert _body_runs() == 1
    assert dest.read_bytes() == b"A"


@pytest.mark.parametrize("form", RESTORING_FORMS)
def test_flipping_a_param_back_restores_the_earlier_output(form: str, tmp_path: Path) -> None:
    producer = RESTORING_FORMS[form]
    dest = tmp_path / "out" / "result.txt"
    _run(producer, payload="A", dest=dest)
    _run(producer, payload="B", dest=dest)

    hit = _run(producer, payload="A", dest=dest)

    assert hit.cached()
    assert _body_runs() == 2
    assert dest.read_bytes() == b"A"


# ---------------------------------------------------------------------------
# An explicit ``-> None`` return stores no artifact
# ---------------------------------------------------------------------------


class TestExplicitNoneReturnHasNoArtifact:
    """What ``-> None`` with an ``Out[file]`` does today, recorded rather than fixed.

    The return value carries no path, so nothing is stored to restore from.
    A hit only checks that the ``Out[...]`` path exists: changed bytes are left
    in place, and a missing file turns the hit into a re-run.
    """

    @pytest.mark.parametrize(
        ("mutation", "left_on_disk"), [("edit", b"tampered"), ("truncate", b"")]
    )
    def test_changed_bytes_are_left_in_place(
        self, mutation: str, left_on_disk: bytes, tmp_path: Path
    ) -> None:
        dest = tmp_path / "out" / "result.txt"
        _run(returns_none, payload="A", dest=dest)

        MUTATIONS[mutation](dest)
        hit = _run(returns_none, payload="A", dest=dest)

        assert hit.cached()
        assert _body_runs() == 1
        assert dest.read_bytes() == left_on_disk

    def test_a_deleted_output_reruns_the_task(self, tmp_path: Path) -> None:
        dest = tmp_path / "out" / "result.txt"
        _run(returns_none, payload="A", dest=dest)

        _delete(dest)
        rerun = _run(returns_none, payload="A", dest=dest)

        assert not rerun.cached()
        assert _body_runs() == 2
        assert dest.read_bytes() == b"A"

    def test_flipping_a_param_back_leaves_the_later_output(self, tmp_path: Path) -> None:
        dest = tmp_path / "out" / "result.txt"
        _run(returns_none, payload="A", dest=dest)
        _run(returns_none, payload="B", dest=dest)

        hit = _run(returns_none, payload="A", dest=dest)

        assert hit.cached()
        assert _body_runs() == 2
        assert dest.read_bytes() == b"B"
