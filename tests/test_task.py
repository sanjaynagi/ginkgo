"""Unit tests for @task decorator, TaskDef, and PartialCall."""

from pathlib import Path

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

    def test_task_positional_kind_shell(self):
        @task("shell")
        def run_cmd(x: int) -> int:
            return x

        assert isinstance(run_cmd, TaskDef)
        assert run_cmd.kind == "shell"

    def test_task_positional_kind_notebook(self):
        @task("notebook")
        def render(sample_id: str) -> Path:
            return Path(sample_id)

        assert isinstance(render, TaskDef)
        assert render.kind == "notebook"

    def test_task_positional_kind_script(self):
        @task("script")
        def run_script(data: str) -> Path:
            return Path(data)

        assert isinstance(run_script, TaskDef)
        assert run_script.kind == "script"

    def test_task_positional_kind_and_keyword_differ_raises(self):
        with pytest.raises(ValueError, match="kind specified twice"):

            @task("shell", kind="notebook")
            def conflict(x: int) -> int:
                return x

    def test_task_positional_kind_and_matching_keyword_ok(self):
        @task("shell", kind="shell")
        def consistent(x: int) -> int:
            return x

        assert consistent.kind == "shell"

    def test_notebook_kind_execution_mode_is_driver(self):
        @task("notebook")
        def render(x: str) -> str:
            return x

        assert render.execution_mode == "driver"

    def test_script_kind_execution_mode_is_driver(self):
        @task("script")
        def run(x: str) -> str:
            return x

        assert run.execution_mode == "driver"


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

    def test_product_map_produces_cartesian_exprlist(self):
        @task()
        def process(item: str, suffix: str, scale: int) -> str:
            return f"{item}{suffix}" * scale

        result = process(scale=2).product_map(item=["a", "b"], suffix=["x", "y"])
        rows = [(expr.args["item"], expr.args["suffix"]) for expr in result]
        assert isinstance(result, ExprList)
        assert rows == [("a", "x"), ("a", "y"), ("b", "x"), ("b", "y")]

    def test_product_map_no_varying_raises(self):
        @task()
        def process(item: str) -> str:
            return item

        with pytest.raises(ValueError, match="at least one varying"):
            process().product_map()

    def test_product_map_unknown_arg_raises(self):
        @task()
        def process(item: str) -> str:
            return item

        with pytest.raises(TypeError, match="unexpected keyword arguments"):
            process().product_map(bogus=["a", "b"])

    def test_product_map_rejects_tmp_dir(self):
        @task()
        def process(item: str, scratch: tmp_dir) -> str:
            return item

        with pytest.raises(TypeError, match="auto-managed by ginkgo"):
            process().product_map(item=["a"], scratch=["/tmp/a"])

    def test_exprlist_map_multiplies_existing_branches(self):
        @task()
        def process(sample: str, lr: float) -> str:
            return f"{sample}:{lr}"

        result = process().map(sample=["s1", "s2"]).map(lr=[0.01, 0.1])
        rows = [(expr.args["sample"], expr.args["lr"]) for expr in result]
        assert rows == [("s1", 0.01), ("s1", 0.1), ("s2", 0.01), ("s2", 0.1)]

    def test_exprlist_product_map_multiplies_existing_branches(self):
        @task()
        def process(sample: str, lr: float, epochs: int) -> str:
            return f"{sample}:{lr}:{epochs}"

        result = process().map(sample=["s1", "s2"]).product_map(lr=[0.01, 0.1], epochs=[10, 50])
        rows = [(expr.args["sample"], expr.args["lr"], expr.args["epochs"]) for expr in result]
        assert rows == [
            ("s1", 0.01, 10),
            ("s1", 0.01, 50),
            ("s1", 0.1, 10),
            ("s1", 0.1, 50),
            ("s2", 0.01, 10),
            ("s2", 0.01, 50),
            ("s2", 0.1, 10),
            ("s2", 0.1, 50),
        ]

    def test_exprlist_map_after_product_map_keeps_existing_outer_order(self):
        @task()
        def process(lr: float, epochs: int, sample: str) -> str:
            return f"{sample}:{lr}:{epochs}"

        result = process().product_map(lr=[0.01, 0.1], epochs=[10, 50]).map(sample=["s1", "s2"])
        rows = [(expr.args["lr"], expr.args["epochs"], expr.args["sample"]) for expr in result]
        assert rows == [
            (0.01, 10, "s1"),
            (0.01, 10, "s2"),
            (0.01, 50, "s1"),
            (0.01, 50, "s2"),
            (0.1, 10, "s1"),
            (0.1, 10, "s2"),
            (0.1, 50, "s1"),
            (0.1, 50, "s2"),
        ]

    def test_product_map_sets_named_display_label_parts(self):
        @task()
        def process(sample: str, lr: float) -> str:
            return f"{sample}:{lr}"

        result = process().product_map(sample=["s1"], lr=[0.01])
        assert result[0].display_label_parts == ("sample=s1", "lr=0.01")

    def test_chained_map_and_product_map_compose_display_label_parts(self):
        @task()
        def process(sample: str, lr: float, epochs: int) -> str:
            return f"{sample}:{lr}:{epochs}"

        result = process().map(sample=["s1"]).product_map(lr=[0.01], epochs=[10])
        assert result[0].display_label_parts == ("s1", "lr=0.01", "epochs=10")
