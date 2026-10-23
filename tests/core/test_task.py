"""Unit tests for @task decorator, TaskDef, and PartialCall."""

from pathlib import Path
from typing import Annotated, Any

import pytest

from ginkgo import evaluate
from ginkgo.plugins.registry import PluginNotFoundError
from ginkgo.runtime.task_kinds import task_kind
from ginkgo import (
    Expr,
    ExprList,
    Out,
    PartialCall,
    TaskDef,
    expand,
    file,
    folder,
    per_branch,
    task,
    tmp_dir,
    zip_expand,
)


@task()
def process(item: str, scale: int) -> str:
    return item * scale


@task()
def scaled(x: int, scale: int = 2) -> int:
    return x * scale


@task()
def with_scratch(item: str, scratch: tmp_dir) -> str:
    return item


@task()
def grid(sample: str, lr: float, epochs: int) -> str:
    return f"{sample}:{lr}:{epochs}"


@task()
def simulate(temperature: float, defect_density: float, output_path: str) -> str:
    return output_path


def _args(result: ExprList, *names: str) -> list[tuple]:
    return [tuple(expr.args[name] for name in names) for expr in result]


class TestTaskDecorator:
    def test_records_its_settings_and_defaults(self):
        @task(env="my_env", version=3, kind="shell", on_failure="ignore")
        def configured(x: int) -> int:
            return x

        @task()
        def plain(x: int) -> int:
            return x

        assert isinstance(plain, TaskDef)
        assert plain.fn.__name__ == "plain"
        assert (configured.env, configured.version, configured.kind) == ("my_env", 3, "shell")
        assert configured.on_failure == "ignore"
        assert (plain.env, plain.version, plain.kind, plain.on_failure) == (
            None,
            1,
            "python",
            "fail_fast",
        )
        assert (plain.threads, plain.export_thread_env) == (1, False)

    @pytest.mark.parametrize(
        ("args", "kwargs", "error", "match"),
        [
            ((), {"on_failure": "carry_on"}, ValueError, "on_failure must be one of"),
            ((), {"kind": " "}, ValueError, "kind must be a non-empty string"),
            (("shell",), {"kind": "notebook"}, ValueError, "kind specified twice"),
            ((), {"threads": 0}, ValueError, "threads must be at least 1"),
        ],
    )
    def test_rejects_invalid_settings(self, args, kwargs, error, match):
        with pytest.raises(error, match=match):

            @task(*args, **kwargs)
            def my_fn(x: int) -> int:
                return x

    def test_bare_task_decorator_points_at_the_parenthesised_form(self):
        with pytest.raises(TypeError, match=r"use @task\(\)"):

            @task
            def bare(x: int) -> int:
                return x

    @pytest.mark.parametrize("kind", ["shell", "notebook", "script"])
    def test_task_positional_kind(self, kind: str) -> None:
        @task(kind, kind=kind)
        def run(x: int) -> int:
            return x

        assert run.kind == kind
        if kind != "notebook":
            assert task_kind(run).execution_mode == "driver"

    @pytest.mark.parametrize(
        ("kwargs", "error", "match"),
        [
            ({"kind": "bash"}, PluginNotFoundError, "task kind 'bash'"),
            (
                {"kind": "script", "retries": 1, "retry_on_exit_codes": (1,)},
                ValueError,
                "kind='script' does not support",
            ),
        ],
    )
    def test_kind_problems_are_refused_when_the_graph_is_built(self, kwargs, error, match):
        @task(**kwargs)
        def my_fn(x: int) -> int:
            return x

        with pytest.raises(error, match=match):
            evaluate(my_fn(x=1))

    def test_threads_passed_as_an_argument_warns(self):
        @task(threads=2)
        def f(x: int, threads: int = 1) -> int:
            return x

        assert f.threads == 2
        with pytest.warns(UserWarning, match="passing 'threads' as a function argument"):
            f(x=1, threads=4)
        with pytest.warns(UserWarning, match="passing 'threads' as a fan-out argument"):
            f().map(x=[1, 2], threads=[1, 2])


class TestTaskDefCall:
    def test_a_full_call_is_an_expr_and_a_partial_one_is_not(self):
        assert isinstance(process(item="a"), PartialCall)
        assert isinstance(process(), PartialCall)
        assert scaled(x=5).args == {"x": 5}
        assert scaled(x=5, scale=10).args == {"x": 5, "scale": 10}
        upstream = scaled(x=1)
        assert scaled(x=upstream).args["x"] is upstream

    def test_tmp_dir_is_auto_managed(self):
        assert with_scratch(item="a").args == {"item": "a"}
        with pytest.raises(TypeError, match="auto-managed by ginkgo"):
            with_scratch(item="a", scratch="/tmp/custom")
        with pytest.raises(TypeError, match="auto-managed by ginkgo"):
            with_scratch().product_map(item=["a"], scratch=["/tmp/a"])

    def test_unknown_kwarg_raises(self):
        with pytest.raises(TypeError, match="unexpected keyword arguments"):
            scaled(x=1, bogus=2)

    def test_positional_call_explains_keyword_only_calls(self):
        with pytest.raises(TypeError, match=r"keyword-only(.|\n)*process\(item=\.\.\., scale"):
            process("a", 2)


class TestPartialCallMap:
    def test_map_zips_varying_arguments_and_keeps_fixed_ones(self):
        result = process(scale=3).map(item=["a", "b", "c"])

        assert isinstance(result, ExprList)
        assert _args(result, "item", "scale") == [("a", 3), ("b", 3), ("c", 3)]
        zipped = grid(epochs=1).map(sample=["s1", "s2"], lr=[0.1, 0.2])
        assert _args(zipped, "sample", "lr") == [("s1", 0.1), ("s2", 0.2)]

    def test_map_accepts_an_upstream_fan_out(self):
        result = process(scale=10).map(item=scaled().map(x=[1, 2, 3]))

        assert len(result) == 3
        assert all(isinstance(expr.args["item"], Expr) for expr in result)

    @pytest.mark.parametrize(
        ("build", "error", "match"),
        [
            (
                lambda: process(item="x").map(item=["a"], scale=[1]),
                TypeError,
                r"'item' given both on the call and to \.map\(\)",
            ),
            (lambda: process().map(item=["a", "b"], scale=[1]), ValueError, "mismatched lengths"),
            (lambda: process().map(), ValueError, "at least one varying"),
            (lambda: process().product_map(), ValueError, "at least one varying"),
            (lambda: process().map(bogus=["a"]), TypeError, "unexpected keyword arguments"),
            (lambda: process().product_map(bogus=["a"]), TypeError, "unexpected keyword"),
            (
                lambda: process(item="s1", scale=1).map(scale=[1, 2]),
                AttributeError,
                r"process\(fixed=\.\.\.\)\.map\(varying=",
            ),
            (lambda: process.map(scale=[1]), AttributeError, "task definition, not a call"),
            (
                lambda: ExprList(exprs=[scaled(x=1), process(item="a", scale=1)]).map(x=[3]),
                TypeError,
                "share one task",
            ),
        ],
    )
    def test_invalid_fan_outs_explain_themselves(self, build, error, match):
        with pytest.raises(error, match=match):
            build()

    def test_a_scalar_varying_argument_suggests_a_fixed_argument(self):
        with pytest.raises(
            TypeError, match="argument 'scale' is a single(.|\n)*scale=3 as a fixed"
        ):
            process(item="a").map(scale=3)

    def test_product_map_is_cartesian_and_composes_with_map(self):
        product = process().product_map(item=["a", "b"], scale=[1, 2])
        assert _args(product, "item", "scale") == [("a", 1), ("a", 2), ("b", 1), ("b", 2)]

        crossed = grid(epochs=1).map(sample=["s1", "s2"]).map(lr=[0.01, 0.1])
        assert _args(crossed, "sample", "lr") == [
            ("s1", 0.01),
            ("s1", 0.1),
            ("s2", 0.01),
            ("s2", 0.1),
        ]

        after_map = grid().map(sample=["s1", "s2"]).product_map(lr=[0.01, 0.1], epochs=[10, 50])
        assert _args(after_map, "sample", "lr", "epochs") == [
            (sample, lr, epochs)
            for sample in ("s1", "s2")
            for lr in (0.01, 0.1)
            for epochs in (10, 50)
        ]

        before_map = grid().product_map(lr=[0.01, 0.1], epochs=[10, 50]).map(sample=["s1", "s2"])
        assert _args(before_map, "lr", "epochs", "sample") == [
            (lr, epochs, sample)
            for lr in (0.01, 0.1)
            for epochs in (10, 50)
            for sample in ("s1", "s2")
        ]

    @pytest.mark.parametrize(
        ("build", "expected"),
        [
            (
                lambda: grid(epochs=1).product_map(sample=["s1"], lr=[0.01]),
                [("sample=s1", "lr=0.01")],
            ),
            (
                lambda: grid().map(sample=["s1"]).product_map(lr=[0.01], epochs=[10]),
                [("s1", "lr=0.01", "epochs=10")],
            ),
            (lambda: process(scale=1).map(item=["chr1", "chr2"]), [("chr1",), ("chr2",)]),
            # A path-like first key yields to the short scalar after it.
            (
                lambda: simulate(defect_density=0.1).map(
                    output_path=["results/raw/chr1.csv", "results/raw/chr2.csv"],
                    temperature=["chr1", "chr2"],
                ),
                [("chr1",), ("chr2",)],
            ),
            # A short decimal-looking scalar (a version) is not path-like.
            (
                lambda: simulate(defect_density=0.1).map(
                    temperature=["v2.1", "v3.0"],
                    output_path=["results/raw/chr1.csv", "results/raw/chr2.csv"],
                ),
                [("v2.1",), ("v3.0",)],
            ),
            # Every varying value is an Expr: inherit the upstream branch label.
            (
                lambda: process(scale=1).map(item=process(scale=1).map(item=["alpha", "beta"])),
                [("alpha",), ("beta",)],
            ),
            # Chained zip .map() keeps every part (the site axis repeats).
            (
                lambda: (
                    grid()
                    .map(epochs=[1, 2])
                    .map(sample=["north", "north", "south"], lr=[0.1, 0.2, 0.1])
                ),
                [
                    (epochs, sample, lr)
                    for epochs in ("1", "2")
                    for sample, lr in (("north", "0.1"), ("north", "0.2"), ("south", "0.1"))
                ],
            ),
        ],
    )
    def test_display_label_parts(self, build, expected):
        assert [expr.display_label_parts for expr in build()] == expected

    def test_map_over_selected_outputs_inherits_upstream_label(self):
        """F15: a selected output labels like its call, never by its repr."""

        @task()
        def split(item: str) -> tuple[str, str]:
            return item, item

        halves = split().map(item=["alpha", "beta"])
        result = process(scale=1).map(item=halves.output[0])

        assert [expr.display_label_parts for expr in result] == [("alpha",), ("beta",)]

    def test_zip_map_labels_name_every_axis_only_when_needed(self):
        """Issue #286: zipping two axes must not collapse every label to the first."""
        sites = ["north_fen", "north_fen", "south_bog", "south_bog"]
        species = ["sedge", "sphagnum", "sedge", "sphagnum"]
        repeating = grid(epochs=1).map(sample=sites, lr=species)
        distinct = grid(epochs=1).map(sample=["a", "b"], lr=[10, 20])

        assert [expr.display_label for expr in repeating] == [
            f"grid[{site},{kind}]" for site, kind in zip(sites, species, strict=True)
        ]
        assert [expr.display_label for expr in distinct] == ["grid[a]", "grid[b]"]


class TestFanOutDerivedArguments:
    """Per-branch derived values must never become a grid axis (issue #198)."""

    def _grid(self, output_path: Any, **kwargs) -> ExprList:
        return simulate().product_map(
            temperature=[300, 400],
            defect_density=[0.01, 0.02],
            output_path=output_path,
            **kwargs,
        )

    def test_product_map_rejects_an_expand_column_and_suggests_per_branch(self):
        template = "results/{temperature}_{defect_density}.json"
        with pytest.raises(ValueError) as excinfo:
            self._grid(expand(template, temperature=[300, 400], defect_density=[0.01, 0.02]))
        assert "'output_path'" in str(excinfo.value)
        assert f"output_path=per_branch({template!r})" in str(excinfo.value)

        with pytest.raises(ValueError, match="zip_expand"):
            self._grid(zip_expand("results/{t}_{d}.json", t=[300, 400], d=[0.01, 0.02]))

    def test_rejection_offers_no_template_when_a_placeholder_names_no_argument(self):
        """A suggestion a user cannot copy is worse than none (PR #216 review)."""
        with pytest.raises(ValueError) as excinfo:
            self._grid(expand("results/{t}.json", t=[300, 400]))

        message = str(excinfo.value)
        assert "per_branch('results/{t}.json')" not in message
        assert "placeholder 't' names no varying argument of this call" in message

    def test_per_branch_derives_one_path_per_branch(self):
        template = per_branch("results/{temperature}_{defect_density}.json")
        result = self._grid(template)

        assert _args(result, "temperature", "defect_density", "output_path") == [
            (t, d, f"results/{t}_{d}.json") for t in (300, 400) for d in (0.01, 0.02)
        ]
        assert result[0].display_label_parts == ("temperature=300", "defect_density=0.01")

        mapped = simulate(temperature=300).map(defect_density=[0.01, 0.02], output_path=template)
        assert [e.args["output_path"] for e in mapped] == [
            "results/300_0.01.json",
            "results/300_0.02.json",
        ]

        chained = (
            simulate()
            .map(temperature=[300, 400])
            .product_map(defect_density=[0.01], output_path=template)
        )
        assert [e.args["output_path"] for e in chained] == [
            "results/300_0.01.json",
            "results/400_0.01.json",
        ]

    def test_map_expand_grid_keeps_paths_aligned_with_parameters(self):
        """Guards the ordering `expand()` and `.map()` silently share."""
        temperatures, densities = [300, 400], [0.01, 0.02]
        result = simulate().map(
            temperature=[t for t in temperatures for _ in densities],
            defect_density=[d for _ in temperatures for d in densities],
            output_path=expand("results/{t}_{d}.json", t=temperatures, d=densities),
        )

        for expr in result:
            expected = f"results/{expr.args['temperature']}_{expr.args['defect_density']}.json"
            assert expr.args["output_path"] == expected

    def test_per_branch_placeholder_problems_raise(self):
        @task()
        def upstream() -> tuple[float, str]:
            return 1.0, "note"

        @task()
        def downstream(temperature: float, note: str, output_path: str) -> str:
            return output_path

        with pytest.raises(ValueError, match="does not take"):
            self._grid(per_branch("results/{pressure}.json"))
        with pytest.raises(ValueError, match="this branch does not set"):
            simulate().product_map(temperature=[300], output_path=per_branch("{defect_density}"))
        with pytest.raises(ValueError, match="task result"):
            downstream(note="n").product_map(
                temperature=[scaled(x=1)], output_path=per_branch("{temperature}.json")
            )
        # F12: ``t.output[1]`` is a task result too, not a value to render.
        with pytest.raises(ValueError, match="task result"):
            downstream(note=upstream().output[1]).map(
                temperature=[300], output_path=per_branch("results/{note}.json")
            )
        with pytest.raises(ValueError, match="only per_branch"):
            simulate(temperature=300, defect_density=0.01).product_map(
                output_path=per_branch("results/{temperature}.json"),
            )

    def test_string_varying_argument_raises_instead_of_fanning_over_characters(self):
        with pytest.raises(TypeError, match="individual characters"):
            simulate(temperature=300).product_map(
                defect_density=[0.01, 0.02], output_path="results/out.json"
            )


class TestOutAnnotation:
    """``Out[...]`` — a direction wrapper for task path parameters."""

    @pytest.mark.parametrize("inner", [file, folder, list[file], file | None])
    def test_path_shapes_are_accepted(self, inner):
        @task()
        def produce(reads: file, target: Out[inner], note: Annotated[file, "meta"]) -> file:
            return reads

        assert produce.output_params == frozenset({"target"})
        assert produce.type_hints["target"] == inner
        assert produce.type_hints["note"] is file
        assert produce.type_hints["return"] is file

    @pytest.mark.parametrize(
        ("annotation", "match"),
        [
            (Out[str], "path-shaped"),
            (Out[int], "path-shaped"),
            (Out[Path], "pathlib.Path"),
            (list[Out[file]], "outermost"),
            (tuple[Out[file], file], "outermost"),
            (Path, "pathlib.Path"),
            (Path | None, "pathlib.Path"),
            (type("MyPath", (Path,), {}), "pathlib.Path"),
        ],
    )
    def test_invalid_parameter_annotations_are_rejected(self, annotation, match):
        def bad(x) -> file:
            return file("x")

        bad.__annotations__["x"] = annotation
        with pytest.raises(TypeError, match=match):
            task()(bad)

    def test_out_on_return_is_rejected(self):
        with pytest.raises(TypeError, match="return value is already an output"):

            @task()
            def bad(x: file) -> Out[file]:
                return file(x)

    def test_return_annotation_is_kept_in_type_hints(self):
        @task()
        def plain(reads: file) -> tuple[file, file | None]:
            return (reads, None)

        assert plain.type_hints["return"] == tuple[file, file | None]
