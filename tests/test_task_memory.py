"""Tests for the @task(memory=...) parameter."""

import pytest

from ginkgo.core.task import _parse_memory, task


class TestParseMemory:
    """Unit tests for the _parse_memory helper."""

    def test_none_returns_zero(self) -> None:
        assert _parse_memory(None) == 0

    def test_gi_integer(self) -> None:
        assert _parse_memory("4Gi") == 4

    def test_gi_float(self) -> None:
        assert _parse_memory("2.5Gi") == 3

    def test_mi_rounds_up(self) -> None:
        assert _parse_memory("512Mi") == 1

    def test_mi_large(self) -> None:
        assert _parse_memory("4096Mi") == 4

    def test_mi_small_rounds_to_one(self) -> None:
        assert _parse_memory("128Mi") == 1

    def test_ti(self) -> None:
        assert _parse_memory("1Ti") == 1024

    def test_g_decimal(self) -> None:
        assert _parse_memory("8G") == 8

    def test_m_decimal(self) -> None:
        assert _parse_memory("1000M") == 1

    def test_ki(self) -> None:
        # 1048576 Ki = 1 Gi
        assert _parse_memory("1048576Ki") == 1

    def test_whitespace_stripped(self) -> None:
        assert _parse_memory("  4Gi  ") == 4

    def test_invalid_unit_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid memory specification"):
            _parse_memory("4Tb")

    def test_bare_number_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid memory specification"):
            _parse_memory("4096")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid memory specification"):
            _parse_memory("")

    def test_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid memory specification"):
            _parse_memory("-4Gi")


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

    def test_memory_512mi(self) -> None:
        @task(memory="512Mi")
        def my_task(x: int) -> int:
            return x

        assert my_task.memory_gb == 1

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
