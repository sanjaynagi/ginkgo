"""VW-3 — Conditional Branching: evaluation tests."""

from collections import Counter
from pathlib import Path

from ginkgo import evaluate, flow, task


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task()
def categorise(value: int, log_path: str) -> str:
    _append_line(log_path, "categorise")
    return "high" if value > 50 else "low"


@task()
def process_high(value: int, log_path: str) -> str:
    _append_line(log_path, "process_high")
    return f"high:{value}"


@task()
def process_low(value: int, log_path: str) -> str:
    _append_line(log_path, "process_low")
    return f"low:{value}"


@task()
def route(value: int, category: str, log_path: str):
    _append_line(log_path, f"route:{category}")
    if category == "high":
        return process_high(value=value, log_path=log_path)
    return process_low(value=value, log_path=log_path)


@task()
def unrelated(seed: int, log_path: str) -> str:
    _append_line(log_path, "unrelated")
    return f"unrelated:{seed}"


@flow
def conditional_pipeline(value: int, log_path: str):
    side_effect = unrelated(seed=1, log_path=log_path)
    category = categorise(value=value, log_path=log_path)
    return side_effect, route(value=value, category=category, log_path=log_path)


class TestVW3ConditionalEvaluation:
    def test_high_branch_executes_only_high_path(self):
        log_path = "events.log"
        result = evaluate(conditional_pipeline(value=80, log_path=log_path), jobs=1, cores=1)

        assert result == ("unrelated:1", "high:80")
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "unrelated",
            "categorise",
            "route:high",
            "process_high",
        ]

    def test_low_branch_executes_only_low_path(self):
        log_path = "events.log"
        result = evaluate(conditional_pipeline(value=20, log_path=log_path), jobs=1, cores=1)

        assert result == ("unrelated:1", "low:20")
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "unrelated",
            "categorise",
            "route:low",
            "process_low",
        ]

    def test_switching_inputs_invalidates_only_relevant_cached_tasks(self):
        log_path = "events.log"

        result_high = evaluate(conditional_pipeline(value=80, log_path=log_path), jobs=1, cores=1)
        result_low = evaluate(conditional_pipeline(value=20, log_path=log_path), jobs=1, cores=1)

        assert result_high == ("unrelated:1", "high:80")
        assert result_low == ("unrelated:1", "low:20")

        counts = Counter(Path(log_path).read_text(encoding="utf-8").splitlines())
        assert counts == Counter(
            {
                "categorise": 2,
                "process_high": 1,
                "process_low": 1,
                "route:high": 1,
                "route:low": 1,
                "unrelated": 1,
            }
        )
