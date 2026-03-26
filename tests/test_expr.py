"""Unit tests for Expr, ExprList, and related expression tree primitives."""

import pytest

from ginkgo import Expr, ExprList, task
from ginkgo.core.expr import OutputIndex, _OutputProxy


@task()
def dummy(x: int) -> int:
    return x + 1


@task()
def pair_task(x: int) -> tuple[int, int]:
    return (x, x + 1)


class TestExpr:
    def test_expr_stores_task_def_and_args(self):
        expr = dummy(x=10)
        assert isinstance(expr, Expr)
        assert expr.task_def is dummy
        assert expr.args == {"x": 10}

    def test_expr_is_frozen(self):
        expr = dummy(x=10)
        try:
            expr.args = {}  # type: ignore[misc]
            assert False, "Should have raised"
        except AttributeError:
            pass

    def test_expr_repr_with_concrete_args(self):
        expr = dummy(x=42)
        r = repr(expr)
        assert "dummy" in r
        assert "x=42" in r

    def test_expr_repr_with_expr_args(self):
        inner = dummy(x=1)
        outer = dummy(x=inner)
        r = repr(outer)
        assert "x=<Expr>" in r

    def test_nested_expr_preserves_structure(self):
        a = dummy(x=1)
        b = dummy(x=a)
        assert isinstance(b.args["x"], Expr)
        assert b.args["x"].args["x"] == 1


class TestExprList:
    def test_len(self):
        exprs = [dummy(x=i) for i in range(5)]
        el = ExprList(exprs=exprs)
        assert len(el) == 5

    def test_getitem(self):
        exprs = [dummy(x=i) for i in range(3)]
        el = ExprList(exprs=exprs)
        assert el[0].args["x"] == 0
        assert el[2].args["x"] == 2

    def test_iter(self):
        exprs = [dummy(x=i) for i in range(3)]
        el = ExprList(exprs=exprs)
        collected = list(el)
        assert len(collected) == 3
        assert all(isinstance(e, Expr) for e in collected)

    def test_map_requires_shared_task_for_mixed_exprlist(self):
        @task()
        def other(y: int) -> int:
            return y * 2

        exprs = [dummy(x=1), other(y=2)]
        el = ExprList(exprs=exprs)
        with pytest.raises(TypeError, match="share one task"):
            el.map(x=[3])


class TestOutputIndex:
    def test_expr_output_returns_proxy(self):
        expr = pair_task(x=1)
        assert isinstance(expr.output, _OutputProxy)

    def test_expr_output_getitem_returns_output_index(self):
        expr = pair_task(x=1)
        idx = expr.output[0]
        assert isinstance(idx, OutputIndex)
        assert idx.expr is expr
        assert idx.index == 0

    def test_expr_output_getitem_preserves_index(self):
        expr = pair_task(x=1)
        assert expr.output[0].index == 0
        assert expr.output[1].index == 1

    def test_exprlist_output_returns_proxy(self):
        exprs = [pair_task(x=i) for i in range(3)]
        el = ExprList(exprs=exprs)
        assert isinstance(el.output, _OutputProxy)

    def test_exprlist_output_getitem_returns_exprlist(self):
        exprs = [pair_task(x=i) for i in range(3)]
        el = ExprList(exprs=exprs, task_def=pair_task)
        result = el.output[0]
        assert isinstance(result, ExprList)
        assert len(result) == 3
        assert result.task_def is pair_task

    def test_exprlist_output_getitem_wraps_each_expr(self):
        exprs = [pair_task(x=i) for i in range(3)]
        el = ExprList(exprs=exprs)
        indexed = el.output[1]
        for i, item in enumerate(indexed):
            assert isinstance(item, OutputIndex)
            assert item.index == 1
            assert item.expr is exprs[i]

    def test_output_index_repr(self):
        expr = pair_task(x=5)
        idx = expr.output[0]
        r = repr(idx)
        assert "OutputIndex" in r
        assert "pair_task" in r
