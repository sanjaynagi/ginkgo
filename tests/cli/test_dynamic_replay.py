"""A cached dynamic task replays the expressions it returned (G1, G6, G2, G3).

The parent's cache key covers its own code and inputs, not its children's, so
a hit on it must still look each child up on its own: a stale child re-runs,
an unchanged one is served from the cache with its outputs restored.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

PYTHON = Path(sys.executable)

SCRIPT = """\
import argparse, pathlib
p = argparse.ArgumentParser(); p.add_argument("--x"); p.add_argument("--out")
a = p.parse_args(); pathlib.Path(a.out).write_text("{tag} " + a.x)
"""

FLOW = """\
import importlib
from pathlib import Path

from ginkgo import Out, file, flow, script, task

from other import readchild

S = Path(__file__).resolve().parent / "scr.py"


@task("script")
def schild(x: str, out: Out[file]):
    return script(S)


@task()
def pa(x: str) -> file:
    return schild(x=x, out="a.txt")


@task()
def pd(x: str) -> str:
    return readchild(src="data.txt")


@task()
def pe(x: str) -> str:
    far = importlib.import_module("lib.far")
    return far.farchild(x=x)


@task()
def pf(x: str) -> file:
    far = importlib.import_module("lib.far")
    return far.farsh(x=x, out="f.txt")


@flow
def main():
    return [pa(x="a"), pd(x="d"), pe(x="e"), pf(x="f")]
"""

OTHER = """\
from ginkgo import file, task


@task()
def readchild(src: file) -> str:
    return "READ " + open(src).read().strip()
"""

FAR = """\
from ginkgo import Out, file, shell, task


@task()
def farchild(x: str) -> str:
    return "{tag} " + x


@task("shell")
def farsh(x: str, out: Out[file]):
    return shell(cmd=f"echo {tag} {{x}} > {{out}}")
"""


def _write(path: str, text: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(textwrap.dedent(text), encoding="utf-8")


def _run(*args: str) -> dict[str, list[str]]:
    """Run ``ginkgo run`` and return each task's completion statuses by base name."""
    result = subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", "run", *args, "--agent-output"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    statuses: dict[str, list[str]] = {}
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if event["event"] == "task_completed":
            name = event["task_name"].rsplit(".", 1)[-1]
            statuses.setdefault(name, []).append(event["status"])
    return statuses


def _project(*, tag: str = "V1", data: str = "one") -> None:
    _write("flow.py", FLOW)
    _write("other.py", OTHER)
    _write("scr.py", SCRIPT.format(tag=tag))
    _write("lib/__init__.py", "")
    _write("lib/far.py", FAR.format(tag=tag))
    _write("data.txt", data)


ALL_CACHED = {
    name: ["cached"]
    for name in ("pa", "schild", "pd", "readchild", "pe", "farchild", "pf", "farsh")
}


def test_unchanged_dynamic_children_stay_cached_and_are_recorded() -> None:
    _project()
    _run("flow.py")

    assert _run("flow.py") == ALL_CACHED
    history = subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", "history", "readchild", "--json"],
        check=True,
        text=True,
        capture_output=True,
    )
    assert [row["cached"] for row in json.loads(history.stdout)] == [True, False]


def test_a_cached_parent_re_runs_each_stale_child_and_only_those() -> None:
    _project()
    _run("flow.py")

    # The script, the data file, and a module only importlib reaches change;
    # no parent's own source or inputs do.
    _write("scr.py", SCRIPT.format(tag="V2"))
    _write("data.txt", "two")
    _write("lib/far.py", FAR.format(tag="V2"))
    statuses = _run("flow.py")

    assert statuses == {
        "pa": ["cached"],
        "schild": ["success"],
        "pd": ["cached"],
        "readchild": ["success"],
        "pe": ["cached"],
        "farchild": ["success"],
        "pf": ["cached"],
        "farsh": ["success"],
    }
    assert Path("a.txt").read_text() == "V2 a"
    assert Path("f.txt").read_text().strip() == "V2 f"


def test_a_deleted_child_output_is_restored_under_a_cached_parent() -> None:
    _write(
        "flow.py",
        """\
        from ginkgo import Out, file, flow, shell, task


        @task("shell")
        def mk(out: Out[file]):
            return shell(cmd=f"echo inter > {out}")


        @task()
        def rd(src: file) -> str:
            return open(src).read().strip()


        @task()
        def pz() -> str:
            return rd(src=mk(out="i.txt"))


        @flow
        def main():
            return pz()
        """,
    )
    _run("flow.py")
    Path("i.txt").unlink()

    assert _run("flow.py") == {"mk": ["cached"], "rd": ["cached"], "pz": ["cached"]}
    assert Path("i.txt").read_text().strip() == "inter"


def test_nested_dynamic_expansion_replays_at_every_level() -> None:
    _write("scr.py", SCRIPT.format(tag="V1"))
    _write(
        "flow.py",
        """\
        from pathlib import Path

        from ginkgo import Out, file, flow, script, task

        S = Path(__file__).resolve().parent / "scr.py"


        @task("script")
        def leaf(x: str, out: Out[file]):
            return script(S)


        @task()
        def middle(x: str) -> file:
            return leaf(x=x, out="leaf.txt")


        @task()
        def top(x: str) -> file:
            return middle(x=x)


        @flow
        def main():
            return top(x="n")
        """,
    )
    _run("flow.py")
    _write("scr.py", SCRIPT.format(tag="V2"))

    assert _run("flow.py") == {"top": ["cached"], "middle": ["cached"], "leaf": ["success"]}
    assert Path("leaf.txt").read_text() == "V2 n"


def test_checkpoint_parent_returns_children_reading_its_own_output_folder() -> None:
    _write(
        "flow.py",
        """\
        from pathlib import Path

        from ginkgo import Out, file, flow, folder, task


        @task()
        def count(src: file) -> int:
            return len(Path(src).read_text())


        @task()
        def split(split_dir: Out[folder]) -> list:
            d = Path(split_dir)
            d.mkdir(parents=True, exist_ok=True)
            out = []
            for i in range(3):
                p = d / f"chunk{i}.txt"
                p.write_text("x" * (i + 1))
                out.append(count(src=str(p)))
            return out


        @flow
        def main():
            return split(split_dir="chunks")
        """,
    )

    assert _run("flow.py") == {"split": ["success"], "count": ["success"] * 3}
    assert _run("flow.py") == {"split": ["cached"], "count": ["cached"] * 3}


def test_rerun_skips_the_cache_for_the_named_task_only() -> None:
    _project()
    _run("flow.py")

    statuses = _run("flow.py", "--rerun", "schild", "--rerun", "pe")

    assert statuses == {
        **ALL_CACHED,
        "schild": ["success"],
        "pe": ["success"],
        # pe's body ran again and returned the same child, which is unchanged.
        "farchild": ["cached"],
    }


SCORES = """\
from ginkgo import flow, task


@task()
def score(name: str) -> str:
    return name.upper()


@task()
def score_all(names: list[str]) -> list:
    return [score(name=name) for name in names]


@flow
def main():
    return score_all(names=["ann", "bo"])
"""


def test_rerun_names_a_returned_call_by_its_label() -> None:
    _write("flow.py", SCORES)
    _run("flow.py")

    assert _run("flow.py", "--rerun", "score[bo]") == {
        "score_all": ["cached"],
        "score": ["cached", "success"],
    }


def test_rerun_of_a_name_no_task_carries_is_a_usage_error() -> None:
    _write("flow.py", SCORES)

    result = subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", "run", "flow.py", "--rerun", "nosuch"],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 2
    assert "--rerun nosuch names no task" in result.stderr
