"""Tests for the @task(memory=...) parameter."""

import pytest

from ginkgo.core.task import _parse_memory, task


class TestParseMemory:
    """Unit tests for the _parse_memory helper."""

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            (None, 0),
            ("4Gi", 4),
            ("2.5Gi", 3),  # binary Gi, rounds up
            ("512Mi", 1),  # rounds up to one
            ("4096Mi", 4),
            ("128Mi", 1),  # small, rounds to one
            ("1Ti", 1024),
            ("8G", 8),  # decimal G
            ("1000M", 1),  # decimal M
            ("1048576Ki", 1),  # 1048576 Ki == 1 Gi
            ("  4Gi  ", 4),  # surrounding whitespace stripped
        ],
    )
    def test_parse_memory_valid(self, spec: str | None, expected: int) -> None:
        assert _parse_memory(spec) == expected

    @pytest.mark.parametrize(
        "spec",
        [
            "4Tb",  # invalid unit
            "4096",  # bare number, no unit
            "",  # empty
            "-4Gi",  # negative
        ],
    )
    def test_parse_memory_invalid_raises(self, spec: str) -> None:
        with pytest.raises(ValueError, match="Invalid memory specification"):
            _parse_memory(spec)


class TestTaskDefMemory:
    """Tests for memory integration in TaskDef and the @task decorator."""

    def test_default_memory_gb_is_zero(self) -> None:
        @task()
        def my_task(x: int) -> int:
            return x

        assert my_task.memory_gb == 0
        assert my_task.memory is None

    def test_memory_string_parsed(self) -> None:
        @task(memory="4Gi")
        def my_task(x: int) -> int:
            return x

        assert my_task.memory == "4Gi"
        assert my_task.memory_gb == 4

    def test_memory_with_threads(self) -> None:
        @task(threads=4, memory="8Gi")
        def my_task(x: int) -> int:
            return x

        assert my_task.threads == 4
        assert my_task.memory_gb == 8

    def test_invalid_memory_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid memory specification"):

            @task(memory="lots")
            def my_task(x: int) -> int:
                return x
