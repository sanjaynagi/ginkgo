"""Unit tests for Expr, ExprList, and related expression tree primitives."""

import copy

import pytest

from ginkgo import ExprList, Out, file, task
from ginkgo.core.expr import OutputIndex


@task()
def dummy(x: int) -> int:
    return x + 1


@task()
def path_task(x: int) -> file | None:
    return None


@task()
def write_report(report: Out[file]) -> None:
    return None


@task()
def pair_task(x: int) -> tuple[int, int]:
    return (x, x + 1)


class TestExpr:
    def test_repr_names_the_task_and_its_arguments(self):
        assert "dummy" in repr(dummy(x=42)) and "x=42" in repr(dummy(x=42))
        assert "x=<Expr>" in repr(dummy(x=dummy(x=1)))
        assert f"{dummy(x=1)}".startswith("Expr(")

    def test_unpacking_names_the_task_and_the_output_idiom(self):
        with pytest.raises(TypeError) as excinfo:
            first, second = pair_task(x=1)  # noqa: F841 - the unpack itself is the subject
        message = str(excinfo.value)
        assert "pair_task()" in message
        assert "r.output[0]" in message
        # TaskDef.name carries the synthetic module prefix; the message must not.
        assert pair_task.name not in message

    @pytest.mark.parametrize(
        ("operation", "match"),
        [
            (lambda expr: [_ for _ in expr], r"pair_task\(\).*output\[0\]"),
            (lambda expr: expr[0], r"pair_task\(\).*cannot be indexed.*output\[0\]"),
            (lambda expr: len(expr), r"pair_task\(\).*measured with len\(\).*output\[0\]"),
        ],
    )
    def test_container_operations_point_at_output(self, operation, match):
        with pytest.raises(TypeError, match=match):
            operation(pair_task(x=1))

    def test_expr_stays_truthy_despite_refusing_len(self):
        assert bool(pair_task(x=1)) is True

    @pytest.mark.parametrize(
        "operation",
        [
            pytest.param(lambda s: s == 5, id="eq"),
            pytest.param(lambda s: s != 5, id="ne"),
            pytest.param(lambda s: 5 == s, id="reflected-eq"),
            pytest.param(lambda s: s in [5, 6], id="contains"),
            pytest.param(lambda s: s > 2, id="gt"),
            pytest.param(lambda s: s >= 2, id="ge"),
            pytest.param(lambda s: s < 2, id="lt"),
            pytest.param(lambda s: s <= 2, id="le"),
            pytest.param(lambda s: 2 < s, id="reflected-lt"),
            pytest.param(lambda s: int(s), id="int"),
            pytest.param(lambda s: float(s), id="float"),
            pytest.param(lambda s: [0, 1][s], id="index"),
            pytest.param(lambda s: f"{s:d}", id="format"),
            pytest.param(lambda s: s + 1, id="add"),
            pytest.param(lambda s: 1 + s, id="radd"),
            pytest.param(lambda s: s - 1, id="sub"),
            pytest.param(lambda s: s * 2, id="mul"),
            pytest.param(lambda s: s / 2, id="truediv"),
            pytest.param(lambda s: s // 2, id="floordiv"),
            pytest.param(lambda s: s % 2, id="mod"),
            pytest.param(lambda s: s**2, id="pow"),
            pytest.param(lambda s: -s, id="neg"),
            pytest.param(lambda s: s | 1, id="or"),
            pytest.param(lambda s: 1 | s, id="ror"),
            pytest.param(lambda s: s & 1, id="and"),
            pytest.param(lambda s: 1 & s, id="rand"),
            pytest.param(lambda s: s ^ 1, id="xor"),
            pytest.param(lambda s: 1 ^ s, id="rxor"),
            pytest.param(lambda s: s << 1, id="lshift"),
            pytest.param(lambda s: 1 << s, id="rlshift"),
            pytest.param(lambda s: s >> 1, id="rshift"),
            pytest.param(lambda s: 1 >> s, id="rrshift"),
            pytest.param(lambda s: ~s, id="invert"),
            pytest.param(lambda s: round(s), id="round"),
            pytest.param(lambda s: round(s, 2), id="round-ndigits"),
            pytest.param(lambda s: complex(s), id="complex"),
        ],
    )
    def test_value_operations_name_the_task(self, operation):
        """#334: a deferred value must not be silently compared or raise a bare TypeError."""
        with pytest.raises(TypeError, match=r"dummy\(\) returns one deferred result"):
            operation(dummy(x=1))

    def test_expr_compares_with_expr_by_identity(self):
        expr = dummy(x=1)
        assert expr == expr
        assert dummy(x=1) != dummy(x=1)
        assert expr in [dummy(x=2), expr]

    def test_comparing_calls_with_mixed_nested_arguments_does_not_raise(self):
        """A nested call in one and a plain value in the other must not reach the refusal."""
        nested = dummy(x=dummy(x=1))
        plain = dummy(x="s")
        calls = [plain, nested]
        assert nested != plain
        assert calls.index(nested) == 1
        calls.remove(nested)
        assert calls == [plain]

    def test_out_parameter_attribute_points_at_output(self):
        with pytest.raises(AttributeError, match=r"""r\.output\['report'\]"""):
            write_report(report="r.txt").report

    def test_missing_attributes_still_raise_attribute_error(self):
        """copy and pickle probe dunders, and hasattr() must still answer False."""
        expr = dummy(x=1)
        assert not hasattr(expr, "anything")
        assert not hasattr(expr, "__setstate__")
        assert copy.copy(expr).args == {"x": 1}


class TestOutputIndex:
    def test_expr_output_selects_one_element(self):
        expr = pair_task(x=1)
        second = expr.output[1]

        assert isinstance(second, OutputIndex)
        assert (second.expr, second.index) == (expr, 1)
        assert "OutputIndex" in repr(second) and "pair_task" in repr(second)

    def test_exprlist_output_selects_one_element_of_every_branch(self):
        exprs = [pair_task(x=i) for i in range(3)]
        indexed = ExprList(exprs=exprs, task_def=pair_task).output[1]

        assert isinstance(indexed, ExprList)
        assert indexed.task_def is pair_task
        assert [(item.expr, item.index) for item in indexed] == [(expr, 1) for expr in exprs]

    @pytest.mark.parametrize(
        "call",
        [
            lambda: dummy(x=1),
            lambda: path_task(x=1),
            lambda: ExprList(exprs=[dummy(x=1)], task_def=dummy),
        ],
    )
    def test_a_single_value_return_cannot_be_indexed(self, call):
        with pytest.raises(TypeError, match="returns a single value"):
            call().output[0]
