"""Output capture for command tasks survives output that is not UTF-8."""

from __future__ import annotations

from pathlib import Path

from ginkgo import Out, evaluate, file, shell, task


@task(kind="shell")
def print_binary(dest: Out[file], log_path: str) -> file:
    # More than a pipe buffer of undecodable bytes, then text after them.
    return shell(
        cmd=f"head -c 200000 /dev/zero | tr '\\0' '\\377'; echo; echo after; touch {dest}",
        output=dest,
        log=log_path,
    )


def test_a_command_printing_binary_succeeds_and_keeps_its_output(tmp_path: Path) -> None:
    log_path = tmp_path / "binary.log"
    evaluate(print_binary(dest=str(tmp_path / "done.txt"), log_path=str(log_path)))

    log = log_path.read_text(encoding="utf-8")
    assert log.count("�") == 200_000
    assert log.endswith("after\n")
