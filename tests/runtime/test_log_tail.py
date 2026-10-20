"""The log tail failure output shows: the last attempt's stdout and stderr."""

from __future__ import annotations

from pathlib import Path

from ginkgo.runtime.rundir import attempt_log_header, combined_log_tail


def _write(path: Path, *chunks: str) -> str:
    path.write_text("".join(chunks), encoding="utf-8")
    return path.name


def test_only_the_last_attempt_of_each_log_is_shown(tmp_path: Path) -> None:
    stdout = _write(
        tmp_path / "out.log",
        "first stdout\n",
        attempt_log_header(attempt=2, attempts=2),
        "second stdout\n",
    )
    stderr = _write(
        tmp_path / "err.log",
        "first stderr\n" * 100,
        attempt_log_header(attempt=2, attempts=2),
        "second stderr\n",
    )

    tail = combined_log_tail(run_dir=tmp_path, stdout_log=stdout, stderr_log=stderr, lines=10)

    assert tail == ["--- attempt 2/2 ---", "second stdout", "second stderr"]


def test_a_log_without_retries_is_tailed_whole(tmp_path: Path) -> None:
    stdout = _write(tmp_path / "out.log", "".join(f"line {n}\n" for n in range(5)))

    tail = combined_log_tail(run_dir=tmp_path, stdout_log=stdout, stderr_log=None, lines=3)

    assert tail == ["line 2", "line 3", "line 4"]


def test_the_header_survives_a_long_last_attempt(tmp_path: Path) -> None:
    stderr = _write(
        tmp_path / "err.log",
        attempt_log_header(attempt=3, attempts=3),
        "".join(f"line {n}\n" for n in range(20)),
    )

    tail = combined_log_tail(run_dir=tmp_path, stdout_log=None, stderr_log=stderr, lines=3)

    assert tail == ["--- attempt 3/3 ---", "line 18", "line 19"]
