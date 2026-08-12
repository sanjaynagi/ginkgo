"""VW-1 — Linear Chain: evaluation tests."""

from pathlib import Path

from ginkgo import evaluate, flow, task
from tests._vw_support import append_line


@task()
def step_a(x: int, log_path: str) -> int:
    append_line(log_path, "step_a")
    return x + 1


@task()
def step_b(x: int, log_path: str) -> int:
    append_line(log_path, "step_b")
    return x * 2


@task()
def step_c(x: int, log_path: str) -> int:
    append_line(log_path, "step_c")
    return x - 3


@flow
def linear(start: int, log_path: str):
    a = step_a(x=start, log_path=log_path)
    b = step_b(x=a, log_path=log_path)
    c = step_c(x=b, log_path=log_path)
    return c


class TestVW1Evaluation:
    def test_linear_chain_result(self):
        result = evaluate(linear(start=5, log_path="events.log"), jobs=1, cores=1)
        assert result == 9

    def test_tasks_execute_in_dependency_order(self):
        log_path = "events.log"
        evaluate(linear(start=8, log_path=log_path), jobs=1, cores=1)
        # Exact-list equality already proves each step ran once, in order.
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "step_a",
            "step_b",
            "step_c",
        ]
