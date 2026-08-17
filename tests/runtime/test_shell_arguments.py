"""Argument serialization for task kinds that call an external process.

Script and notebook tasks forward their resolved arguments to a separate
process as CLI options and parameter files, which carry text rather than
Python objects. These tests pin what an ``AssetRef`` becomes at that
boundary — a path for a ``file`` asset, a named refusal for every other
kind — instead of falling through to ``json.dumps``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import ginkgo
from ginkgo import AssetRef, asset, file, script, table, task
from ginkgo.core.asset import AssetKey
from ginkgo.runtime.task_runners.shell import (
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

    def test_table_ref_is_refused_by_kind(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            serialize_cli_argument_value(_ref(kind="table"))

        message = str(excinfo.value)
        assert "is a `table` asset" in message
        assert "JSON serializable" not in message


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
