"""Tests for workflow parameter declaration and resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo
from ginkgo.cli.workflow_params import params_table
from ginkgo.config import config_session
from ginkgo.params import (
    ParamContext,
    ParamDecl,
    ParamError,
    extract_flag_values,
    find_global_param_reads,
    flag_for,
    format_param_help,
    resolve_param,
)


def _resolve(*, cli: list[str] | None = None, config: dict | None = None, **declaration):
    """Declare one parameter named ``p`` in a fresh session and return its value."""
    with config_session(param_config=config or {}, cli_extras=cli or []):
        return ginkgo.param("p", **declaration)


def test_flag_for_dashes_underscores():
    assert flag_for("n_replicates") == "--n-replicates"
    assert flag_for("region") == "--region"


def test_cli_beats_config_beats_default_and_sources_are_recorded():
    with config_session(param_config={"a": 5, "b": 6}, cli_extras=["--a", "1"]) as session:
        assert ginkgo.param("a", type=int, default=99) == 1
        assert ginkgo.param("b", type=int, default=99) == 6
        assert ginkgo.param("c", type=int, default=99) == 99
        assert session.param_sources() == {"a": "cli", "b": "config", "c": "default"}
        assert session.resolved_params() == {"a": 1, "b": 6, "c": 99}


@pytest.mark.parametrize(
    ("cli", "config", "declaration", "expected"),
    [
        (["--p=wide"], None, {"default": ""}, "wide"),
        (["--p", "-3"], None, {"type": int, "default": 0}, -3),
        (["--p", "-1.5e3"], None, {"type": float, "default": 0.0}, -1500.0),
        (
            ["--p", "results/x.txt"],
            None,
            {"type": Path, "default": Path(".")},
            Path("results/x.txt"),
        ),
        # The type is inferred from the default.
        (["--p", "2.5"], None, {"default": 40.0}, 2.5),
        (["--p", "7"], None, {"default": 3}, 7),
        (["--p", "true"], None, {"default": False}, True),
        (["--p", "7"], None, {"default": "a"}, "7"),
        (None, {"p": "12"}, {"type": int, "default": 0}, 12),
        (None, {"p": 12}, {"type": int, "default": 0}, 12),
        (
            ["--p", "a", "--p=b", "--p", "c"],
            None,
            {"multiple": True, "default": ()},
            ("a", "b", "c"),
        ),
        (None, {"p": ["x", "y"]}, {"multiple": True, "default": ()}, ("x", "y")),
        (None, None, {"multiple": True, "default": ["a"]}, ("a",)),
        (["--p", "slow"], None, {"choices": ["fast", "slow"], "default": "fast"}, "slow"),
        (["--p", "2L"], None, {}, "2L"),
    ],
)
def test_values_are_read_and_coerced(cli, config, declaration, expected):
    assert _resolve(cli=cli, config=config, **declaration) == expected


@pytest.mark.parametrize(
    ("tokens", "expected"),
    [
        (["--p"], True),
        (["--p", "true"], True),
        (["--p", "yes"], True),
        (["--p", "1"], True),
        (["--p", "false"], False),
        (["--p", "no"], False),
        (["--p", "0"], False),
    ],
)
def test_bool_literals(tokens, expected):
    assert _resolve(cli=tokens, type=bool, default=None) is expected


@pytest.mark.parametrize(
    ("cli", "config", "declaration", "match"),
    [
        (["--p", "-x"], None, {"default": ""}, "--p expects a value"),
        (["--p"], None, {"default": ""}, "--p expects a value"),
        (["--p", "1", "--p", "2"], None, {"default": ""}, "not declared multiple=True"),
        (None, {"p": [1, 2]}, {"default": ""}, "not declared multiple=True"),
        (
            ["--p=maybe"],
            None,
            {"type": bool, "default": False},
            "Cannot read 'maybe' as a boolean",
        ),
        (["--p", "oops"], None, {"type": int, "default": 0}, "Cannot read 'oops' as int for"),
        (
            ["--p", "sideways"],
            None,
            {"choices": ["fast", "slow"], "default": "f"},
            "is not one of",
        ),
        (None, None, {"choices": [], "default": "x"}, "empty choices"),
        (None, None, {}, "is required but was not supplied"),
    ],
)
def test_bad_values_and_declarations_are_errors(cli, config, declaration, match):
    with pytest.raises(ParamError, match=match):
        _resolve(cli=cli, config=config, **declaration)


def test_bare_bool_flag_does_not_swallow_following_token():
    with config_session(cli_extras=["--on", "--label", "x", "--nope", "2"]) as session:
        assert ginkgo.param("on", type=bool, default=False) is True
        assert ginkgo.param("label", default="") == "x"
        assert session.unconsumed_extras() == ["--nope", "2"]


def test_lenient_session_resolves_required_to_none():
    with config_session(require_params=False):
        assert ginkgo.param("region") is None


@pytest.mark.parametrize("name", ["9bad", "has-dash", "", "has space"])
def test_invalid_names_rejected(name):
    with config_session():
        with pytest.raises(ParamError, match="Invalid parameter name"):
            ginkgo.param(name, default=1)


def test_redeclaration_must_be_identical():
    with config_session(cli_extras=["--a", "3"]) as session:
        assert ginkgo.param("a", type=int, default=1) == 3
        assert ginkgo.param("a", type=int, default=1) == 3
        assert len(session.declarations) == 1
        with pytest.raises(ParamError, match="declared twice with different settings"):
            ginkgo.param("a", type=str, default="x")


def test_without_a_session_a_default_is_returned_and_required_explains_itself():
    assert ginkgo.param("a", type=int, default=7) == 7
    with pytest.raises(ParamError, match="no ginkgo run context is active"):
        ginkgo.param("region")


def test_nested_sessions_do_not_leak_declarations():
    with config_session(cli_extras=["--a", "1"]) as outer:
        ginkgo.param("a", type=int, default=0)
        with config_session(cli_extras=["--b", "2"]) as inner:
            ginkgo.param("b", type=int, default=0)
            assert set(inner.declarations) == {"b"}
        assert set(outer.declarations) == {"a"}


def test_extract_flag_values_only_claims_its_own_tokens():
    decl = ParamDecl(name="a", default="")
    values, consumed = extract_flag_values(["--b", "1", "--a", "2", "--c=3"], decl)
    assert (values, consumed) == (["2"], {2, 3})
    assert extract_flag_values(["--ab", "1", "--a-b", "2"], decl)[0] == []


def test_resolve_param_require_false_keeps_default_source():
    resolution = resolve_param(ParamDecl(name="a"), cli_values=(), config_values={}, require=False)
    assert (resolution.value, resolution.source) == (None, "default")


def test_param_context_round_trip():
    context = ParamContext(config={"a": 1}, cli_extras=("--a", "2"))
    assert ParamContext.from_payload(context.to_payload()) == context
    assert ParamContext.from_payload({}) == ParamContext(config={}, cli_extras=())


def test_decl_payload_describes_declaration():
    decl = ParamDecl(
        name="n_reps", value_type=int, default=12, help_text="Replicates", choices=(6, 12)
    )
    assert decl.to_payload() == {
        "name": "n_reps",
        "flag": "--n-reps",
        "type": "int",
        "required": False,
        "default": 12,
        "help": "Replicates",
        "choices": [6, 12],
        "multiple": False,
    }
    # Non-JSON defaults render as strings; a required parameter has no default.
    assert ParamDecl(name="out", value_type=Path, default=Path("a/b.txt")).to_payload()[
        "default"
    ] == ("a/b.txt")
    assert ParamDecl(name="region").to_payload()["default"] is None


def test_format_param_help_lines():
    lines = format_param_help(
        [
            ParamDecl(name="n_reps", value_type=int, default=12, help_text="Replicates"),
            ParamDecl(name="region", help_text="Genome region"),
            ParamDecl(name="verbose", value_type=bool, default=False),
            ParamDecl(name="item", multiple=True, default=()),
        ]
    )
    assert lines[0].strip() == "--n-reps INT  Replicates (default: 12)"
    assert lines[1].strip() == "--region STR  Genome region (required)"
    assert "bare --verbose is true" in lines[2]
    assert "repeatable; the value is a tuple" in lines[3]
    assert format_param_help([]) == []


def test_format_param_help_shows_config_default_and_origin():
    lines = format_param_help(
        [ParamDecl(name="n_reps", value_type=int, default=12), ParamDecl(name="region")],
        config_defaults={"n_reps": (7, "ginkgo.toml"), "region": ("2L", "extra.toml")},
    )
    assert lines[0].strip() == "--n-reps INT  (default: 7, from ginkgo.toml)"
    assert lines[1].strip() == "--region STR  (default: '2L', from extra.toml)"


def _function(source: str, namespace: dict):
    exec(compile(source, "<generated>", "exec"), namespace)
    return next(value for value in namespace.values() if callable(value))


class TestFindGlobalParamReads:
    def test_flags_a_global_read_of_a_declared_param(self):
        param_globals: dict = {}
        function = _function("def reads_tag():\n    return tag\n", param_globals)

        findings = find_global_param_reads(
            declaration_globals={"tag": param_globals}, tasks=[("write_it", function)]
        )

        assert [(item.task_name, item.param_name) for item in findings] == [("write_it", "tag")]
        assert "Pass it as an argument" in findings[0].message()
        assert "--tag" in findings[0].message()

    @pytest.mark.parametrize(
        ("source", "same_module"),
        [
            ("def uses_arg(tag):\n    return tag\n", True),
            # Detection reads LOAD_GLOBAL, not co_names.
            ("def uses_attr(obj):\n    return obj.tag\n", True),
            # A global of the same name in a module that did not declare it.
            ("def reads_tag():\n    return tag\n", False),
        ],
    )
    def test_ignores_what_is_not_a_global_read(self, source, same_module):
        declaring: dict = {}
        function = _function(source, declaring if same_module else {})

        assert (
            find_global_param_reads(
                declaration_globals={"tag": declaring}, tasks=[("t", function)]
            )
            == []
        )

    def test_tolerates_no_declarations_and_non_python_callables(self):
        function = _function("def reads_tag():\n    return tag\n", {})
        assert find_global_param_reads(declaration_globals={}, tasks=[("t", function)]) == []
        assert (
            find_global_param_reads(
                declaration_globals={"tag": {}}, tasks=[("t", len), ("u", None)]
            )
            == []
        )

    def test_declaring_globals_recorded_on_the_session(self):
        with config_session() as session:
            ginkgo.param("tag", default="a")
            assert session.declaration_globals["tag"] is globals()


def test_params_table_layers_across_sources():
    """An override that sets one parameter must leave the rest of the base table intact."""
    assert params_table([{"params": {"a": 1, "b": 2}}, {"params": {"a": 9}}]) == {"a": 9, "b": 2}
    assert params_table([{"other": 1}, {"params": {"a": 1}}, {}]) == {"a": 1}
    assert params_table([{"other": 1}, {}]) == {}
    with pytest.raises(TypeError, match="must be a mapping"):
        params_table([{"params": ["a", "b"]}])
