"""Unit tests for @task decorator, TaskDef, and PartialCall."""

import pytest

from ginkgo import Expr, ExprList, PartialCall, TaskDef, task, tmp_dir


class TestTaskDecorator:
    def test_task_returns_taskdef(self):
        @task()
        def my_fn(x: int) -> int:
            return x

        assert isinstance(my_fn, TaskDef)

    def test_task_preserves_function(self):
        @task()
        def my_fn(x: int) -> int:
            return x

        assert my_fn.fn.__name__ == "my_fn"

    def test_task_env_and_version(self):
        @task(env="my_env", version=3, kind="shell")
        def my_fn(x: int) -> int:
            return x

        assert my_fn.env == "my_env"
        assert my_fn.version == 3
        assert my_fn.kind == "shell"

    def test_task_defaults(self):
        @task()
        def my_fn(x: int) -> int:
            return x

        assert my_fn.env is None
        assert my_fn.version == 1
        assert my_fn.kind == "python"

    def test_task_rejects_unknown_kind(self):
        with pytest.raises(ValueError, match="kind must be one of"):

            @task(kind="bash")
            def my_fn(x: int) -> int:
                return x


class TestTaskDefCall:
    def test_full_call_returns_expr(self):
        @task()
        def add(x: int, y: int) -> int:
            return x + y

        result = add(x=1, y=2)
        assert isinstance(result, Expr)
        assert result.args == {"x": 1, "y": 2}

    def test_partial_call_returns_partial(self):
        @task()
        def add(x: int, y: int) -> int:
            return x + y

        result = add(x=1)
        assert isinstance(result, PartialCall)

    def test_zero_args_returns_partial(self):
        @task()
        def add(x: int, y: int) -> int:
            return x + y

        result = add()
        assert isinstance(result, PartialCall)

    def test_full_call_with_defaults(self):
        @task()
        def process(x: int, scale: int = 2) -> int:
            return x * scale

        # Only x is required; providing just x should be a full call
        result = process(x=5)
        assert isinstance(result, Expr)

    def test_full_call_overrides_defaults(self):
        @task()
        def process(x: int, scale: int = 2) -> int:
            return x * scale

        result = process(x=5, scale=10)
        assert isinstance(result, Expr)
        assert result.args == {"x": 5, "scale": 10}

    def test_tmp_dir_is_auto_managed_not_required(self):
        @task()
        def process(x: int, scratch: tmp_dir) -> int:
            return x

        result = process(x=5)
        assert isinstance(result, Expr)
        assert result.args == {"x": 5}

    def test_tmp_dir_cannot_be_supplied_by_caller(self):
        @task()
        def process(x: int, scratch: tmp_dir) -> int:
            return x

        with pytest.raises(TypeError, match="auto-managed by ginkgo"):
            process(x=5, scratch="/tmp/custom")

    def test_unknown_kwarg_raises(self):
        @task()
        def my_fn(x: int) -> int:
            return x

        with pytest.raises(TypeError, match="unexpected keyword arguments"):
            my_fn(x=1, bogus=2)

    def test_expr_as_argument(self):
        @task()
        def step_a(x: int) -> int:
            return x + 1

        @task()
        def step_b(x: int) -> int:
            return x * 2

        a = step_a(x=10)
        b = step_b(x=a)
        assert isinstance(b, Expr)
        assert isinstance(b.args["x"], Expr)


class TestPartialCallMap:
    def test_map_produces_exprlist(self):
        @task()
        def process(item: str, scale: int) -> str:
            return item * scale

        result = process(scale=3).map(item=["a", "b", "c"])
        assert isinstance(result, ExprList)
        assert len(result) == 3

    def test_map_preserves_fixed_args(self):
        @task()
        def process(item: str, scale: int) -> str:
            return item * scale

        result = process(scale=3).map(item=["a", "b"])
        for expr in result:
            assert expr.args["scale"] == 3

    def test_map_varies_args_correctly(self):
        @task()
        def process(item: str, scale: int) -> str:
            return item * scale

        result = process(scale=3).map(item=["a", "b", "c"])
        items = [expr.args["item"] for expr in result]
        assert items == ["a", "b", "c"]

    def test_map_multiple_varying(self):
        @task()
        def align(r1: str, r2: str, ref: str) -> str:
            return f"{ref}:{r1}:{r2}"

        result = align(ref="hg38").map(r1=["s1_R1", "s2_R1"], r2=["s1_R2", "s2_R2"])
        assert len(result) == 2
        assert result[0].args["r1"] == "s1_R1"
        assert result[0].args["r2"] == "s1_R2"
        assert result[1].args["r1"] == "s2_R1"
        assert result[1].args["r2"] == "s2_R2"

    def test_map_mismatched_lengths_raises(self):
        @task()
        def process(item: str, scale: int) -> str:
            return item * scale

        with pytest.raises(ValueError, match="mismatched lengths"):
            process().map(item=["a", "b"], scale=[1, 2, 3])

    def test_map_no_varying_raises(self):
        @task()
        def process(item: str) -> str:
            return item

        with pytest.raises(ValueError, match="at least one varying"):
            process().map()

    def test_map_unknown_arg_raises(self):
        @task()
        def process(item: str) -> str:
            return item

        with pytest.raises(TypeError, match="unexpected keyword arguments"):
            process().map(bogus=["a", "b"])

    def test_map_with_exprlist_as_varying(self):
        @task()
        def step_a(x: int) -> int:
            return x + 1

        @task()
        def step_b(y: int, z: int) -> int:
            return y * z

        # step_a mapped over items produces an ExprList
        a_results = step_a().map(x=[1, 2, 3])

        # Use that ExprList as a varying arg for step_b
        b_results = step_b(z=10).map(y=a_results)
        assert isinstance(b_results, ExprList)
        assert len(b_results) == 3
        # Each b expr should have an Expr (from step_a) as its y argument
        for expr in b_results:
            assert isinstance(expr.args["y"], Expr)

    def test_zero_args_partial_then_map(self):
        @task()
        def process(item: str) -> str:
            return item

        result = process().map(item=["a", "b"])
        assert len(result) == 2
