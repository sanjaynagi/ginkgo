"""Values of classes a workflow defines cross task boundaries on every run.

A dataclass or enum defined in a single-file workflow lives in a module named
after the workflow's absolute path in one process. Its values once worked on a
fresh run but failed in the consumer whenever the producer was served from the
cache, and container subclasses (``namedtuple``, ``defaultdict``) lost their
type at every task boundary.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from tests.conftest import run_cli

FLOW_SOURCE = """
import dataclasses
import enum
from collections import defaultdict, namedtuple

from ginkgo import file, flow, task

from helpers import use_colour, use_defaults, use_pair, use_point

Pair = namedtuple("Pair", "x y")


@dataclasses.dataclass
class Point:
    x: int


class Colour(enum.Enum):
    RED = 1


@task()
def make_point() -> Point:
    return Point(3)


@task()
def make_colour() -> Colour:
    return Colour.RED


@task()
def make_pair() -> Pair:
    return Pair(1, 2)


@task()
def make_defaults() -> dict:
    return defaultdict(list, {"a": [1]})


@flow
def main():
    stamp = file("stamp.txt")
    return [
        use_point(point=make_point(), stamp=stamp),
        use_colour(colour=make_colour(), stamp=stamp),
        use_pair(pair=make_pair(), stamp=stamp),
        use_defaults(values=make_defaults(), stamp=stamp),
    ]
"""

# The consumers live in a module of their own, so a worker running one has
# not imported the workflow that defines the classes of its arguments.
HELPERS_SOURCE = """
from ginkgo import file, task


@task()
def use_point(point, stamp: file) -> int:
    return point.x


@task()
def use_colour(colour, stamp: file) -> str:
    return colour.name


@task()
def use_pair(pair, stamp: file) -> int:
    return pair.y


@task()
def use_defaults(values, stamp: file) -> list:
    return values["missing"]
"""


def _run(*args: str, cwd: Path) -> str:
    # Each `ginkgo run` is a fresh process that has not imported the project's helpers.
    sys.modules.pop("helpers", None)
    result = run_cli("run", "workflow/flow.py", *args, cwd=cwd)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _project(root: Path) -> Path:
    (root / "workflow").mkdir(parents=True)
    (root / "ginkgo.toml").write_text("", encoding="utf-8")
    (root / "workflow" / "flow.py").write_text(FLOW_SOURCE, encoding="utf-8")
    (root / "workflow" / "helpers.py").write_text(HELPERS_SOURCE, encoding="utf-8")
    (root / "stamp.txt").write_text("1\n", encoding="utf-8")
    return root


def test_workflow_values_survive_fresh_cached_and_consumer_only_runs(tmp_path: Path) -> None:
    project = _project(tmp_path / "project")
    assert "8 tasks executed, 0 cached" in _run(cwd=project)
    assert "0 tasks executed, 8 cached" in _run(cwd=project)

    # Changing only the consumers' input runs them in fresh workers, against
    # values the cache decodes, while the producers stay cached.
    (project / "stamp.txt").write_text("2\n", encoding="utf-8")
    assert "4 tasks executed, 4 cached" in _run(cwd=project)
    rerun = _run("--rerun", "use_colour", "--rerun", "use_defaults", cwd=project)
    assert "2 tasks executed, 6 cached" in rerun


def test_a_moved_project_keeps_its_cache_for_workflow_values(tmp_path: Path) -> None:
    project = _project(tmp_path / "project")
    _run(cwd=project)
    moved = tmp_path / "moved"
    shutil.move(project, moved)

    assert "0 tasks executed, 8 cached" in _run(cwd=moved)
    assert "1 task executed, 7 cached" in _run("--rerun", "use_point", cwd=moved)
