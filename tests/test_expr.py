"""Unit tests for Expr, ExprList, and related expression tree primitives."""

from ginkgo import Expr, ExprList, task


@task()
def dummy(x: int) -> int:
    return x + 1


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
