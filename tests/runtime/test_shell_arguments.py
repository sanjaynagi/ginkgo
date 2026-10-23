"""Argument serialization for task kinds that call an external process.

Script and notebook tasks forward their resolved arguments to a separate
process as command-line options, which carry text rather than Python
objects. These tests pin what a live Python payload becomes at that
boundary — a refusal naming the parameter and the task kind, instead of
falling through to ``json.dumps`` — and how lists and flags render as CLI
tokens.
"""

from __future__ import annotations

import datetime
import shlex
from pathlib import Path

import pandas as pd
import pytest

import ginkgo
from ginkgo import file, script, task
from ginkgo.runtime.task_runners.shell import (
    render_cli_tokens,
    render_repeated_cli_tokens,
    serialize_cli_argument_value,
    stringify_cli_argument,
)


# ---------------------------------------------------------------------------
# End-to-end: script and shell tasks consuming upstream outputs
# ---------------------------------------------------------------------------

_COPY_SCRIPT = """
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--summary", required=True)
parser.add_argument("--script-path", required=True)
parser.add_argument("--output-path", required=True)
args = parser.parse_args()

Path(args.output_path).write_text(
    Path(args.summary).read_text(encoding="utf-8", errors="replace"),
    encoding="utf-8",
)
"""


@task()
def produce_notes(output_path: str) -> file:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("site,count\nnorth,10\n", encoding="utf-8")
    return file(output_path)


@task()
def produce_scores() -> object:
    return pd.DataFrame({"site": ["north"], "count": [10]})


@task(kind="script")
def copy_via_script(summary: file, script_path: str, output_path: str) -> file:
    return script(script_path, output=output_path)


@task(kind="shell")
def count_rows_via_shell(scores: object, csv_path: str, output_path: str) -> file:
    """The sanctioned route: take the payload, write the format the command wants."""
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(scores).to_csv(csv_path, index=False)
    from ginkgo import shell

    return shell(cmd=f"wc -l < {csv_path} | tr -d ' ' > {output_path}", output=output_path)


class TestUpstreamOutputArguments:
    def test_a_file_output_reaches_the_script_as_a_readable_path(self, tmp_path: Path) -> None:
        script_path = tmp_path / "copy_input.py"
        script_path.write_text(_COPY_SCRIPT, encoding="utf-8")

        produced = ginkgo.evaluate(
            copy_via_script(
                summary=produce_notes(output_path="results/notes.csv"),
                script_path=str(script_path),
                output_path="results/copied.csv",
            )
        )

        assert Path(produced).read_text(encoding="utf-8") == "site,count\nnorth,10\n"

    def test_shell_task_writes_the_format_its_command_expects(self) -> None:
        """The route the refusal recommends, ``object`` then write CSV, has to work."""

        produced = ginkgo.evaluate(
            count_rows_via_shell(
                scores=produce_scores(),
                csv_path="work/scores.csv",
                output_path="results/rows.txt",
            )
        )

        # Header plus one data row.
        assert Path(produced).read_text(encoding="utf-8").strip() == "2"


# ---------------------------------------------------------------------------
# Script CLI argument rendering: lists as separate tokens, bools as flags
# (issue #308, #309)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "tokens"),
    [
        (["results/a.tsv", "results/b.tsv"], ["--opt", "results/a.tsv", "results/b.tsv"]),
        (("results/a.tsv", "results/b.tsv"), ["--opt", "results/a.tsv", "results/b.tsv"]),
        ([], ["--opt"]),
        # Lists with no positional CLI form stay JSON.
        ([["a", "b"], ["c"]], ["--opt", shlex.quote('[["a", "b"], ["c"]]')]),
        ([{"a": 1}], ["--opt", shlex.quote('[{"a": 1}]')]),
        (["a", None], ["--opt", shlex.quote('["a", null]')]),
        ([True, False], ["--opt", shlex.quote("[true, false]")]),
        (True, ["--opt"]),
        (False, []),
        (None, ["--opt", "null"]),
        ("north", ["--opt", "north"]),
        (3, ["--opt", "3"]),
    ],
)
def test_render_cli_tokens(value, tokens: list[str]) -> None:
    """An argparse script declares ``nargs="*"`` for lists, ``store_true`` for bools."""
    assert render_cli_tokens(option="--opt", value=value) == tokens


@pytest.mark.parametrize(
    ("value", "tokens"),
    [
        (["results/a.tsv", "results/b.tsv"], ["--opt", "results/a.tsv", "--opt", "results/b.tsv"]),
        ([], []),
        ([[1, 2], [3]], ["--opt", shlex.quote("[[1, 2], [3]]")]),
        # ``mo.cli_args()`` turns "true"/"false" into a bool; a bare flag
        # would come back as an empty string.
        (True, ["--opt", "true"]),
        (False, ["--opt", "false"]),
    ],
)
def test_render_repeated_cli_tokens(value, tokens: list[str]) -> None:
    """``mo.cli_args()`` collects a repeated option into a list, so lists repeat it."""
    assert render_repeated_cli_tokens(option="--opt", value=value) == tokens


@pytest.mark.parametrize("render", [render_cli_tokens, render_repeated_cli_tokens])
def test_items_with_spaces_and_glob_characters_are_individually_quoted(render) -> None:
    # Re-parsing the joined command line recovers each string verbatim rather
    # than letting the shell glob or word-split it.
    items = ["results/a b.tsv", "results/[c].tsv", "results/*.tsv"]
    tokens = shlex.split(" ".join(render(option="--opt", value=items)))
    assert [token for token in tokens if token != "--opt"] == items


# ---------------------------------------------------------------------------
# End-to-end: a fan-in list[file] argument on a script task (issue #308)
# ---------------------------------------------------------------------------

_COMBINE_SCRIPT = """
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--clusters", nargs="*", default=[])
parser.add_argument("--verbose", action="store_true")
parser.add_argument("--script-path", required=True)
parser.add_argument("--output-path", required=True)
args = parser.parse_args()

lines = [Path(p).read_text(encoding="utf-8").strip() for p in args.clusters]
if args.verbose:
    lines.append("verbose")
Path(args.output_path).write_text("\\n".join(lines) + "\\n", encoding="utf-8")
"""


@task()
def write_cluster_file(label: str, output_path: str) -> file:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(label, encoding="utf-8")
    return file(str(out))


@task(kind="script")
def combine_clusters(
    clusters: list[file], script_path: str, output_path: str, verbose: bool = False
) -> file:
    return script(script_path, output=output_path)


def test_list_and_bool_arguments_reach_an_argparse_script(tmp_path: Path) -> None:
    """A ``.map()`` fan-in survives the shell as separate tokens (#308), an empty
    one as the bare option, and a bool as ``store_true`` (#309)."""
    script_path = tmp_path / "combine.py"
    script_path.write_text(_COMBINE_SCRIPT, encoding="utf-8")
    clusters = write_cluster_file().map(
        label=["north", "south"], output_path=["results/a.tsv", "results/b.tsv"]
    )

    def combine(name: str, **kwargs):
        return combine_clusters(
            script_path=str(script_path), output_path=f"results/{name}.txt", **kwargs
        )

    produced = ginkgo.evaluate(
        [
            combine("fan-in", clusters=clusters),
            combine("empty", clusters=[]),
            combine("verbose", clusters=[], verbose=True),
        ]
    )

    assert [Path(path).read_text(encoding="utf-8") for path in produced] == [
        "north\nsouth\n",
        "\n",
        "verbose\n",
    ]


# ---------------------------------------------------------------------------
# A live Python payload has no text form at this boundary (issue #233)
# ---------------------------------------------------------------------------


@task(kind="script")
def eat_payload_via_script(scores: object, script_path: str, output_path: str) -> file:
    """Follows the `object` advice in a task kind that cannot carry a payload."""
    return script(script_path, output=output_path)


class TestLivePythonPayloadArguments:
    """A DataFrame reaching the serializer is refused by name, not by json."""

    def test_payload_refusal_names_parameter_and_task_kind(self) -> None:
        payload = pd.DataFrame({"site": ["north"], "count": [10]})

        with pytest.raises(TypeError) as excinfo:
            serialize_cli_argument_value(payload, label="eat_payload.scores", task_kind="script")

        message = str(excinfo.value)
        assert "eat_payload.scores" in message
        assert "pandas.DataFrame" in message
        assert "`script` task" in message
        assert "JSON serializable" not in message
        # Same guidance as the path-binding refusal: where the worked example
        # is, and that a bare path string is not the way round this.
        assert "Staging A Python Value For A Notebook" in message
        assert "plain `str` is not a safe substitute" in message
        assert "no dependency edge" in message

    @pytest.mark.parametrize(
        ("wrap", "task_kind", "match"),
        [
            (lambda payload: ["ok", payload], "script", r"eat_payload\.scores\[1\]"),
            (lambda payload: {"north": payload}, "notebook", r"eat_payload\.scores\['north'\]"),
        ],
    )
    def test_nested_payload_names_its_position(self, wrap, task_kind: str, match: str) -> None:
        nested = wrap(pd.DataFrame({"count": [10]}))
        with pytest.raises(TypeError, match=match):
            serialize_cli_argument_value(nested, label="eat_payload.scores", task_kind=task_kind)

    def test_stringify_and_unlabelled_refusals_still_name_the_type(self) -> None:
        payload = pd.DataFrame({"count": [10]})
        with pytest.raises(TypeError) as excinfo:
            stringify_cli_argument(payload, label="eat_payload.scores", task_kind="script")
        assert "JSON serializable" not in str(excinfo.value)
        with pytest.raises(TypeError, match="pandas.DataFrame"):
            serialize_cli_argument_value(payload)

    def test_a_date_argument_crosses_as_iso_text(self) -> None:
        """A date has a text form, so it crosses rather than being refused."""
        assert serialize_cli_argument_value(datetime.date(2026, 1, 1)) == "2026-01-01"
        assert stringify_cli_argument(datetime.date(2026, 1, 1)) == "2026-01-01"

    def test_table_payload_into_a_script_task_is_refused_end_to_end(self, tmp_path: Path) -> None:
        script_path = tmp_path / "copy_input.py"
        script_path.write_text(_COPY_SCRIPT, encoding="utf-8")

        with pytest.raises(TypeError) as excinfo:
            ginkgo.evaluate(
                eat_payload_via_script(
                    scores=produce_scores(),
                    script_path=str(script_path),
                    output_path="results/copied.csv",
                )
            )

        message = str(excinfo.value)
        assert "eat_payload_via_script.scores" in message
        assert "pandas.DataFrame" in message
        assert "JSON serializable" not in message
        # Refused before the script ran, so its declared output was never written.
        assert not (tmp_path / "results" / "copied.csv").exists()
