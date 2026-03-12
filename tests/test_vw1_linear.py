"""VW-1 — Linear Chain: evaluation tests."""

from pathlib import Path

from ginkgo import evaluate, flow, task


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task()
def step_a(x: int, log_path: str) -> int:
    _append_line(log_path, "step_a")
    return x + 1


@task()
def step_b(x: int, log_path: str) -> int:
    _append_line(log_path, "step_b")
    return x * 2


@task()
def step_c(x: int, log_path: str) -> int:
    _append_line(log_path, "step_c")
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
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "step_a",
            "step_b",
            "step_c",
        ]

    def test_no_task_runs_more_than_once(self):
        log_path = "events.log"
        evaluate(linear(start=2, log_path=log_path), jobs=1, cores=1)
        events = Path(log_path).read_text(encoding="utf-8").splitlines()
        assert events.count("step_a") == 1
        assert events.count("step_b") == 1
        assert events.count("step_c") == 1
