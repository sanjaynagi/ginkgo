"""Unit tests for @task decorator, TaskDef, and PartialCall."""

import pytest

from ginkgo import (
    Expr,
    ExprList,
    PartialCall,
    TaskDef,
    expand,
    per_branch,
    task,
    tmp_dir,
    zip_expand,
)


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

    def test_task_declares_its_failure_policy(self):
        @task(on_failure="ignore")
        def my_fn(x: int) -> int:
            return x

        @task()
        def other_fn(x: int) -> int:
            return x

        assert my_fn.on_failure == "ignore"
        assert other_fn.on_failure == "fail_fast"

    def test_task_rejects_an_unknown_failure_policy(self):
        with pytest.raises(ValueError, match="on_failure must be one of"):

            @task(on_failure="carry_on")
            def my_fn(x: int) -> int:
                return x

    def test_task_rejects_unknown_kind(self):
        with pytest.raises(ValueError, match="kind must be one of"):

            @task(kind="bash")
            def my_fn(x: int) -> int:
                return x

    @pytest.mark.parametrize("kind", ["shell", "notebook", "script"])
    def test_task_positional_kind(self, kind: str) -> None:
        @task(kind)
        def run(x: int) -> int:
            return x

        assert isinstance(run, TaskDef)
        assert run.kind == kind

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

    @pytest.mark.parametrize("kind", ["notebook", "script"])
    def test_driver_kind_execution_mode_is_driver(self, kind: str) -> None:
        @task(kind)
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
        assert result.args["x"] == 5

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

    def test_map_single_varying_scalar_labels_by_value(self):
        @task()
        def simulate_variants(sample: str) -> str:
            return sample

        result = simulate_variants().map(sample=["chr1", "chr2"])
        assert result[0].display_label_parts == ("chr1",)
        assert result[1].display_label_parts == ("chr2",)

    def test_map_prefers_short_scalar_over_earlier_path_like_key(self):
        @task()
        def compute_window_stat(path: str, chrom: str) -> str:
            return path

        result = compute_window_stat().map(
            path=["results/raw/chr1.csv", "results/raw/chr2.csv"],
            chrom=["chr1", "chr2"],
        )
        # The first varying key ("path") looks like a file path, so the
        # short scalar "chrom" key should be used for the label instead.
        assert result[0].display_label_parts == ("chr1",)
        assert result[1].display_label_parts == ("chr2",)

    def test_map_short_decimal_scalar_is_not_treated_as_path_like(self):
        @task()
        def train(version: str, path: str) -> str:
            return version

        result = train().map(
            version=["v2.1", "v3.0"],
            path=["results/raw/chr1.csv", "results/raw/chr2.csv"],
        )
        # A short decimal-looking scalar (e.g. a version string) must not be
        # rejected as path-like just because it contains a ".<digits>" suffix.
        assert result[0].display_label_parts == ("v2.1",)
        assert result[1].display_label_parts == ("v3.0",)

    def test_map_all_expression_values_inherits_upstream_label(self):
        @task()
        def build_brief(item: str) -> str:
            return item

        @task()
        def package_brief(brief: str) -> str:
            return brief

        briefs = build_brief().map(item=["alpha", "beta"])
        result = package_brief().map(brief=briefs)

        # Every varying value is an Expr, so no short scalar exists; the
        # label should inherit the producing upstream branch's label
        # rather than come back empty.
        assert result[0].display_label_parts == ("alpha",)
        assert result[1].display_label_parts == ("beta",)


class TestFanOutDerivedArguments:
    """Per-branch derived values must never become a grid axis (issue #198)."""

    @staticmethod
    def _simulate() -> TaskDef:
        @task()
        def simulate(temperature: float, defect_density: float, output_path: str) -> str:
            return output_path

        return simulate

    def test_product_map_rejects_expand_column(self):
        simulate = self._simulate()

        with pytest.raises(ValueError) as excinfo:
            simulate().product_map(
                temperature=[300, 400],
                defect_density=[0.01, 0.02],
                output_path=expand("results/{t}_{d}.json", t=[300, 400], d=[0.01, 0.02]),
            )

        message = str(excinfo.value)
        assert "'output_path'" in message
        assert "expand('results/{t}_{d}.json')" in message

    def test_rejection_suggests_the_template_when_placeholders_name_arguments(self):
        simulate = self._simulate()
        template = "results/{temperature}_{defect_density}.json"

        with pytest.raises(ValueError) as excinfo:
            simulate().product_map(
                temperature=[300, 400],
                defect_density=[0.01, 0.02],
                output_path=expand(template, temperature=[300, 400], defect_density=[0.01, 0.02]),
            )

        assert f"output_path=per_branch({template!r})" in str(excinfo.value)

    def test_rejection_offers_no_template_when_a_placeholder_names_no_argument(self):
        """A suggestion a user cannot copy is worse than none (PR #216 review)."""
        simulate = self._simulate()

        with pytest.raises(ValueError) as excinfo:
            simulate().product_map(
                temperature=[300, 400],
                defect_density=[0.01, 0.02],
                output_path=expand("results/{t}.json", t=[300, 400]),
            )

        message = str(excinfo.value)
        assert "per_branch('results/{t}.json')" not in message
        assert "per_branch(" not in message.replace("per_branch(...)", "").replace(
            "per_branch()", ""
        )
        assert "placeholder 't' names no varying argument of this call" in message
        assert "temperature, defect_density" in message

    def test_product_map_rejects_zip_expand_column(self):
        simulate = self._simulate()

        with pytest.raises(ValueError, match="zip_expand"):
            simulate().product_map(
                temperature=[300, 400],
                defect_density=[0.01, 0.02],
                output_path=zip_expand("results/{t}_{d}.json", t=[300, 400], d=[0.01, 0.02]),
            )

    def test_product_map_grid_derives_one_output_path_per_cell(self):
        simulate = self._simulate()

        result = simulate().product_map(
            temperature=[300, 400],
            defect_density=[0.01, 0.02],
            output_path=per_branch("results/{temperature}_{defect_density}.json"),
        )

        assert len(result) == 4
        rows = [
            (expr.args["temperature"], expr.args["defect_density"], expr.args["output_path"])
            for expr in result
        ]
        assert rows == [
            (300, 0.01, "results/300_0.01.json"),
            (300, 0.02, "results/300_0.02.json"),
            (400, 0.01, "results/400_0.01.json"),
            (400, 0.02, "results/400_0.02.json"),
        ]

    def test_every_branch_path_matches_its_own_parameters(self):
        simulate = self._simulate()

        result = simulate().product_map(
            temperature=[300, 400, 500],
            defect_density=[0.01, 0.02],
            output_path=per_branch("results/{temperature}_{defect_density}.json"),
        )

        for expr in result:
            expected = f"results/{expr.args['temperature']}_{expr.args['defect_density']}.json"
            assert expr.args["output_path"] == expected

    def test_map_expand_grid_keeps_paths_aligned_with_parameters(self):
        """Guards the ordering `expand()` and `.map()` silently share."""
        simulate = self._simulate()
        temperatures = [300, 400]
        densities = [0.01, 0.02]

        result = simulate().map(
            temperature=[t for t in temperatures for _ in densities],
            defect_density=[d for _ in temperatures for d in densities],
            output_path=expand("results/{t}_{d}.json", t=temperatures, d=densities),
        )

        assert len(result) == len(temperatures) * len(densities)
        for expr in result:
            expected = f"results/{expr.args['temperature']}_{expr.args['defect_density']}.json"
            assert expr.args["output_path"] == expected

    def test_map_accepts_per_branch_template(self):
        simulate = self._simulate()

        result = simulate(temperature=300).map(
            defect_density=[0.01, 0.02],
            output_path=per_branch("results/{temperature}_{defect_density}.json"),
        )

        paths = [expr.args["output_path"] for expr in result]
        assert paths == ["results/300_0.01.json", "results/300_0.02.json"]

    def test_per_branch_is_not_an_axis_or_a_label(self):
        simulate = self._simulate()

        result = simulate().product_map(
            temperature=[300],
            defect_density=[0.01],
            output_path=per_branch("results/{temperature}_{defect_density}.json"),
        )

        assert len(result) == 1
        assert result[0].display_label_parts == ("temperature=300", "defect_density=0.01")

    def test_per_branch_renders_from_chained_branch_values(self):
        simulate = self._simulate()

        result = (
            simulate()
            .map(temperature=[300, 400])
            .product_map(
                defect_density=[0.01],
                output_path=per_branch("results/{temperature}_{defect_density}.json"),
            )
        )

        paths = [expr.args["output_path"] for expr in result]
        assert paths == ["results/300_0.01.json", "results/400_0.01.json"]

    def test_per_branch_unknown_placeholder_raises(self):
        simulate = self._simulate()

        with pytest.raises(ValueError, match="does not take"):
            simulate().product_map(
                temperature=[300],
                defect_density=[0.01],
                output_path=per_branch("results/{pressure}.json"),
            )

    def test_per_branch_placeholder_not_set_by_branch_raises(self):
        simulate = self._simulate()

        with pytest.raises(ValueError, match="this branch does not set"):
            simulate().product_map(
                temperature=[300],
                output_path=per_branch("results/{temperature}_{defect_density}.json"),
            )

    def test_per_branch_placeholder_on_task_result_raises(self):
        @task()
        def upstream() -> float:
            return 1.0

        @task()
        def downstream(temperature: float, output_path: str) -> str:
            return output_path

        with pytest.raises(ValueError, match="task result"):
            downstream().product_map(
                temperature=[upstream()],
                output_path=per_branch("results/{temperature}.json"),
            )

    def test_per_branch_only_call_raises(self):
        simulate = self._simulate()

        with pytest.raises(ValueError, match="only per_branch"):
            simulate(temperature=300, defect_density=0.01).product_map(
                output_path=per_branch("results/{temperature}.json"),
            )

    def test_string_varying_argument_raises_instead_of_fanning_over_characters(self):
        simulate = self._simulate()

        with pytest.raises(TypeError, match="individual characters"):
            simulate(temperature=300).product_map(
                defect_density=[0.01, 0.02],
                output_path="results/out.json",
            )


class TestTaskThreadsContract:
    def test_threads_default_is_one(self):
        @task()
        def f() -> int:
            return 0

        assert f.threads == 1
        assert f.export_thread_env is False

    def test_threads_decorator_value(self):
        @task(threads=4)
        def f() -> int:
            return 0

        assert f.threads == 4

    def test_threads_must_be_at_least_one(self):
        with pytest.raises(ValueError, match="threads must be at least 1"):

            @task(threads=0)
            def f() -> int:
                return 0

    def test_threads_kwarg_at_call_warns(self):
        @task(threads=2)
        def f(x: int = 1, threads: int = 1) -> int:
            return x

        with pytest.warns(UserWarning, match="passing 'threads' as a function argument"):
            f(threads=4)

    def test_threads_in_map_warns(self):
        @task(threads=2)
        def f(x: int, threads: int = 1) -> int:
            return x

        with pytest.warns(UserWarning, match="passing 'threads' as a fan-out argument"):
            f().map(x=[1, 2], threads=[1, 2])
