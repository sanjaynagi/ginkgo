"""Argument serialization for task kinds that call an external process.

Script and notebook tasks forward their resolved arguments to a separate
process as CLI options and parameter files, which carry text rather than
Python objects. These tests pin what an ``AssetRef`` becomes at that
boundary — a path for a ``file`` asset, a named refusal for every other
kind — instead of falling through to ``json.dumps``, and what a live Python
payload becomes there: a refusal naming the parameter and the task kind.
"""

from __future__ import annotations

import datetime
import shlex
from pathlib import Path

import pandas as pd
import pytest

import ginkgo
from ginkgo import AssetRef, asset, file, script, table, task, text
from ginkgo.core.asset import AssetKey
from ginkgo.runtime.task_runners.shell import (
    render_cli_tokens,
    serialize_cli_argument_value,
    stringify_cli_argument,
)


def _ref(*, kind: str, artifact_path: str = "/blobs/abc123") -> AssetRef:
    return AssetRef(
        key=AssetKey(namespace=kind, name="producer.summary"),
        version_id="v1",
        kind=kind,
        artifact_id="abc123",
        content_hash="def456",
        artifact_path=artifact_path,
    )


class TestAssetRefArguments:
    def test_file_ref_serializes_to_its_artifact_path(self) -> None:
        ref = _ref(kind="file")
        assert serialize_cli_argument_value(ref) == "/blobs/abc123"
        assert stringify_cli_argument(ref) == "/blobs/abc123"

    def test_nested_file_refs_serialize_to_paths(self) -> None:
        ref = _ref(kind="file")
        assert serialize_cli_argument_value([ref]) == ["/blobs/abc123"]
        assert serialize_cli_argument_value({"reads": ref}) == {"reads": "/blobs/abc123"}
        # A container of refs is JSON-encodable once the refs are paths; before
        # this branch existed the ref reached json.dumps and raised.
        assert stringify_cli_argument([ref]) == '["/blobs/abc123"]'

    def test_native_byte_kinds_serialize_to_paths(self) -> None:
        """A fig is native image bytes and a text asset is raw UTF-8."""
        assert serialize_cli_argument_value(_ref(kind="fig")) == "/blobs/abc123"
        assert serialize_cli_argument_value(_ref(kind="text")) == "/blobs/abc123"

    def test_encoded_kinds_are_refused_by_kind(self) -> None:
        for kind in ("table", "array", "model"):
            with pytest.raises(TypeError) as excinfo:
                serialize_cli_argument_value(_ref(kind=kind))

            message = str(excinfo.value)
            assert f"is a `{kind}` asset" in message
            assert "JSON serializable" not in message
            # The consumer is a driver task, so `object` is not the way out.
            assert "`object`" not in message


# ---------------------------------------------------------------------------
# End-to-end: a script task consuming an asset
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
def produce_file_asset(output_path: str) -> file:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("site,count\nnorth,10\n", encoding="utf-8")
    return asset(out, name="script_args/notes")


@task()
def produce_table_asset(output_path: str) -> object:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": ["north"], "count": [10]}).to_csv(out, index=False)
    return table(out, name="script_args/scores")


@task(kind="script")
def copy_via_script(summary: file | AssetRef, script_path: str, output_path: str) -> file:
    return script(script_path, output=output_path)


@task()
def produce_text_asset() -> object:
    return text("north,10\nsouth,20\neast,30\n", name="script_args/notes")


@task(kind="shell")
def wc_via_shell(notes: file | AssetRef, output_path: str) -> file:
    """A shell task reading a text asset's artifact as the file it is."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    src = Path(notes.artifact_path) if isinstance(notes, AssetRef) else Path(str(notes))
    from ginkgo import shell

    return shell(
        cmd=f"wc -l < {shlex.quote(str(src))} | tr -d ' ' > {shlex.quote(output_path)}",
        output=output_path,
    )


@task(kind="shell")
def count_rows_via_shell(scores: object, csv_path: str, output_path: str) -> file:
    """The sanctioned route: take the payload, write the format the command wants."""
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(scores).to_csv(csv_path, index=False)
    from ginkgo import shell

    return shell(cmd=f"wc -l < {csv_path} | tr -d ' ' > {output_path}", output=output_path)


class TestScriptTaskAssetArguments:
    def test_file_asset_reaches_the_script_as_a_readable_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The documented ``file | AssetRef`` idiom works for a script task."""
        monkeypatch.chdir(tmp_path)
        script_path = tmp_path / "copy_input.py"
        script_path.write_text(_COPY_SCRIPT, encoding="utf-8")

        produced = ginkgo.evaluate(
            copy_via_script(
                summary=produce_file_asset(output_path="results/notes.csv"),
                script_path=str(script_path),
                output_path="results/copied.csv",
            )
        )

        assert Path(produced).read_text(encoding="utf-8") == "site,count\nnorth,10\n"

    def test_table_asset_is_refused_by_kind_not_by_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A table asset names the parameter and kind instead of failing in json."""
        monkeypatch.chdir(tmp_path)
        script_path = tmp_path / "copy_input.py"
        script_path.write_text(_COPY_SCRIPT, encoding="utf-8")

        with pytest.raises(TypeError) as excinfo:
            ginkgo.evaluate(
                copy_via_script(
                    summary=produce_table_asset(output_path="results/scores.csv"),
                    script_path=str(script_path),
                    output_path="results/copied.csv",
                )
            )

        message = str(excinfo.value)
        assert "copy_via_script.summary" in message
        assert "is a `table` asset" in message
        assert "JSON serializable" not in message

    def test_text_asset_reaches_a_shell_command_as_a_readable_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A shell task counts the lines of a text asset through the union idiom."""
        monkeypatch.chdir(tmp_path)

        produced = ginkgo.evaluate(
            wc_via_shell(notes=produce_text_asset(), output_path="results/lines.txt")
        )

        assert Path(produced).read_text(encoding="utf-8").strip() == "3"

    def test_shell_task_writes_the_format_its_command_expects(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The route the refusal recommends: ``object`` annotation, then write CSV.

        This is what the guide tells a shell-task author to do instead of
        interpolating ``artifact_path``, so it has to work.
        """
        monkeypatch.chdir(tmp_path)

        produced = ginkgo.evaluate(
            count_rows_via_shell(
                scores=produce_table_asset(output_path="results/scores.csv"),
                csv_path="work/scores.csv",
                output_path="results/rows.txt",
            )
        )

        # Header plus one data row, read as text rather than as a Parquet blob.
        assert Path(produced).read_text(encoding="utf-8").strip() == "2"


# ---------------------------------------------------------------------------
# Script CLI argument rendering: lists as separate tokens, bools as flags
# (issue #308, #309)
# ---------------------------------------------------------------------------


class TestRenderCliTokens:
    """Pin the argparse-friendly rendering ``render_cli_tokens`` produces.

    A script's own parser is expected to declare the matching idiom:
    ``nargs="*"``/``"+"`` for a scalar/path list, ``action="store_true"`` for
    a bool.
    """

    def test_list_of_paths_renders_as_separate_tokens(self) -> None:
        tokens = render_cli_tokens(option="--clusters", value=["results/a.tsv", "results/b.tsv"])
        assert tokens == ["--clusters", "results/a.tsv", "results/b.tsv"]

    def test_empty_list_renders_as_the_bare_option(self) -> None:
        assert render_cli_tokens(option="--clusters", value=[]) == ["--clusters"]

    def test_tuple_of_paths_renders_the_same_as_a_list(self) -> None:
        tokens = render_cli_tokens(option="--clusters", value=("results/a.tsv", "results/b.tsv"))
        assert tokens == ["--clusters", "results/a.tsv", "results/b.tsv"]

    def test_items_with_spaces_and_glob_characters_are_individually_quoted(self) -> None:
        tokens = render_cli_tokens(
            option="--clusters", value=["results/a b.tsv", "results/[c].tsv", "results/*.tsv"]
        )
        assert tokens[0] == "--clusters"
        # Each item is its own shell-quoted token, so re-parsing the joined
        # command line recovers the original strings verbatim rather than
        # letting the shell glob or word-split them.
        assert shlex.split(" ".join(tokens[1:])) == [
            "results/a b.tsv",
            "results/[c].tsv",
            "results/*.tsv",
        ]

    def test_nested_list_keeps_json_rendering(self) -> None:
        tokens = render_cli_tokens(option="--groups", value=[["a", "b"], ["c"]])
        assert tokens == ["--groups", shlex.quote('[["a", "b"], ["c"]]')]

    def test_list_containing_a_dict_keeps_json_rendering(self) -> None:
        tokens = render_cli_tokens(option="--rows", value=[{"a": 1}])
        assert tokens == ["--rows", shlex.quote('[{"a": 1}]')]

    def test_list_containing_none_keeps_json_rendering(self) -> None:
        """None has no positional CLI form, so a list holding one stays JSON."""
        tokens = render_cli_tokens(option="--items", value=["a", None])
        assert tokens == ["--items", shlex.quote('["a", null]')]

    def test_list_containing_a_bool_keeps_json_rendering(self) -> None:
        """A bool inside a list has no store_true idiom, so it stays JSON too."""
        tokens = render_cli_tokens(option="--flags", value=[True, False])
        assert tokens == ["--flags", shlex.quote("[true, false]")]

    def test_true_renders_as_the_bare_option(self) -> None:
        assert render_cli_tokens(option="--verbose", value=True) == ["--verbose"]

    def test_false_omits_the_option_entirely(self) -> None:
        assert render_cli_tokens(option="--verbose", value=False) == []

    def test_none_keeps_its_current_single_token_rendering(self) -> None:
        assert render_cli_tokens(option="--label", value=None) == ["--label", "null"]

    def test_scalar_still_renders_as_a_single_token(self) -> None:
        assert render_cli_tokens(option="--name", value="north") == ["--name", "north"]
        assert render_cli_tokens(option="--count", value=3) == ["--count", "3"]


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


class TestScriptTaskFanInListArgument:
    def test_map_fan_in_list_of_files_reaches_an_nargs_star_parser(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``.map()`` fan-in of file paths survives the shell as separate tokens."""
        monkeypatch.chdir(tmp_path)
        script_path = tmp_path / "combine.py"
        script_path.write_text(_COMBINE_SCRIPT, encoding="utf-8")

        cluster_a = write_cluster_file(label="north", output_path="results/a.tsv")
        cluster_b = write_cluster_file(label="south", output_path="results/b.tsv")

        produced = ginkgo.evaluate(
            combine_clusters(
                clusters=[cluster_a, cluster_b],
                script_path=str(script_path),
                output_path="results/combined.txt",
            )
        )

        assert Path(produced).read_text(encoding="utf-8").splitlines() == ["north", "south"]

    def test_empty_fan_in_reaches_the_parser_as_no_clusters(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty fan-out renders the bare option, matching nargs="*" default []."""
        monkeypatch.chdir(tmp_path)
        script_path = tmp_path / "combine.py"
        script_path.write_text(_COMBINE_SCRIPT, encoding="utf-8")

        produced = ginkgo.evaluate(
            combine_clusters(
                clusters=[],
                script_path=str(script_path),
                output_path="results/combined.txt",
            )
        )

        assert Path(produced).read_text(encoding="utf-8") == "\n"

    def test_bool_flag_renders_as_store_true(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        script_path = tmp_path / "combine.py"
        script_path.write_text(_COMBINE_SCRIPT, encoding="utf-8")

        produced = ginkgo.evaluate(
            combine_clusters(
                clusters=[],
                script_path=str(script_path),
                output_path="results/combined.txt",
                verbose=True,
            )
        )

        assert Path(produced).read_text(encoding="utf-8").splitlines() == ["verbose"]


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
        assert "Staging A `table`, `array`, Or `model` Asset For A Notebook" in message
        assert "plain `str` is not a safe substitute" in message
        assert "no dependency edge" in message

    def test_nested_payload_names_its_position(self) -> None:
        payload = pd.DataFrame({"count": [10]})

        with pytest.raises(TypeError, match=r"eat_payload\.scores\[1\]"):
            serialize_cli_argument_value(
                ["ok", payload], label="eat_payload.scores", task_kind="script"
            )

        with pytest.raises(TypeError, match=r"eat_payload\.scores\['north'\]"):
            serialize_cli_argument_value(
                {"north": payload}, label="eat_payload.scores", task_kind="notebook"
            )

    def test_stringify_refuses_rather_than_reaching_json(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            stringify_cli_argument(
                pd.DataFrame({"count": [10]}), label="eat_payload.scores", task_kind="script"
            )

        assert "JSON serializable" not in str(excinfo.value)

    def test_unlabelled_refusal_still_names_the_type(self) -> None:
        """Called without a label, the refusal still names the type."""
        with pytest.raises(TypeError, match="pandas.DataFrame"):
            serialize_cli_argument_value(pd.DataFrame({"count": [10]}))

    def test_a_date_argument_crosses_as_iso_text(self) -> None:
        """A date has a text form, so it crosses rather than being refused."""
        assert serialize_cli_argument_value(datetime.date(2026, 1, 1)) == "2026-01-01"
        assert stringify_cli_argument(datetime.date(2026, 1, 1)) == "2026-01-01"

    def test_table_payload_into_a_script_task_is_refused_end_to_end(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        script_path = tmp_path / "copy_input.py"
        script_path.write_text(_COPY_SCRIPT, encoding="utf-8")

        with pytest.raises(TypeError) as excinfo:
            ginkgo.evaluate(
                eat_payload_via_script(
                    scores=produce_table_asset(output_path="results/scores.csv"),
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
