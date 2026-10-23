"""Regression cover for issue #291.

Renaming or copying a byte-identical single-file workflow used to cold-start
every cached task, because the synthetic per-path module name the workflow
loads under (see ``USER_MODULE_PREFIX``) leaked into the cache key's task
identity and into its source hash. The case that mattered was a run of the
identical bytes under a new file name.
"""

from __future__ import annotations

import pytest

from tests.cli.conftest import run_ok, write

WORKFLOW_SOURCE = """
from ginkgo import flow, task

@task()
def greet(name: str) -> str:
    return f"hello {name}"

@flow
def main():
    return greet(name="world")
"""


@pytest.mark.parametrize("moved", ["a/renamed.py", "b/renamed.py"])
def test_a_byte_identical_copy_hits_cache_under_a_new_name(moved: str) -> None:
    write("ginkgo.toml")
    write("a/workflow.py", WORKFLOW_SOURCE)
    assert "1 tasks executed, 0 cached" in run_ok("run", "a/workflow.py").stdout
    assert "0 tasks executed, 1 cached" in run_ok("run", "a/workflow.py").stdout

    write(moved, WORKFLOW_SOURCE)
    assert "0 tasks executed, 1 cached" in run_ok("run", moved).stdout
