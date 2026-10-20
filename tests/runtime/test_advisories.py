"""Tests for the advisory notices, without an evaluator."""

from __future__ import annotations

from pathlib import Path

from ginkgo import folder, task
from ginkgo.runtime.advisories import untracked_directory_notices, written_str_inputs_notice


@task()
def make_dir(*, name: str) -> str:
    return name


@task()
def consume(*, inputs: list[str]) -> str:
    return ",".join(inputs)


@task()
def consume_folder(*, inputs: folder) -> str:
    return str(inputs)


class TestUntrackedDirectoryNotices:
    def test_an_upstream_directory_inside_a_list_is_warned_about_once(
        self, tmp_path: Path
    ) -> None:
        directory = tmp_path / "out"
        directory.mkdir()
        expr = consume(inputs=[make_dir(name="a"), make_dir(name="b")])
        warned: set[tuple[str, str, str]] = set()

        notices = untracked_directory_notices(
            task_def=expr.task_def,
            args=expr.args,
            resolved_args={"inputs": [str(directory), str(directory)]},
            warned=warned,
        )
        again = untracked_directory_notices(
            task_def=expr.task_def,
            args=expr.args,
            resolved_args={"inputs": [str(directory)]},
            warned=warned,
        )

        assert len(notices) == 1
        assert "make_dir returns a path to a directory as 'str'" in notices[0]
        assert again == []

    def test_a_literal_directory_is_not_warned_about(self, tmp_path: Path) -> None:
        expr = consume(inputs=[str(tmp_path)])

        assert (
            untracked_directory_notices(
                task_def=expr.task_def,
                args=expr.args,
                resolved_args={"inputs": [str(tmp_path)]},
                warned=set(),
            )
            == []
        )

    def test_a_folder_annotation_is_not_warned_about(self, tmp_path: Path) -> None:
        expr = consume_folder(inputs=make_dir(name="a"))

        assert (
            untracked_directory_notices(
                task_def=expr.task_def,
                args=expr.args,
                resolved_args={"inputs": str(tmp_path)},
                warned=set(),
            )
            == []
        )


class TestWrittenStrInputsNotice:
    def test_names_each_path_and_parameter(self) -> None:
        notice = written_str_inputs_notice([("log", "run.log"), ("out", "a.txt")])

        assert notice is not None
        assert "run.log (parameter 'log'), a.txt (parameter 'out')" in notice
        assert "Annotate 'log', 'out' `Out[file]`" in notice

    def test_nothing_written_means_no_notice(self) -> None:
        assert written_str_inputs_notice([]) is None
