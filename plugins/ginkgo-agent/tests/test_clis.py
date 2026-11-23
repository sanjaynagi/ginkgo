"""The command each agent CLI is called with, and what is read back from it."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

from ginkgo_agent.clis import AttemptPaths, build_command, read_final_message, usage_totals
from ginkgo_agent.settings import AgentSettings


def _tokens(cmd: str) -> list[str]:
    return shlex.split(cmd)


def test_codex_runs_in_its_workspace_write_sandbox_with_the_schema(tmp_path: Path) -> None:
    paths = AttemptPaths(root=tmp_path / "attempt")
    schema = tmp_path / "schema.json"
    cmd, env = build_command(
        settings=AgentSettings(runner="codex", command="codex", model="gpt-5-codex"),
        paths=paths,
        reads=(tmp_path / "packet",),
        output_schema=schema,
        network=False,
    )
    tokens = _tokens(cmd)
    assert tokens[tokens.index("exec") - 1] == "codex"
    for flag, value in [
        ("--sandbox", "workspace-write"),
        ("--cd", str(paths.workspace)),
        ("-o", str(paths.final_message)),
        ("--output-schema", str(schema)),
        ("-m", "gpt-5-codex"),
    ]:
        assert tokens[tokens.index(flag) + 1] == value
    assert {"--skip-git-repo-check", "--ephemeral", "--json"} <= set(tokens)
    # The network stays off unless asked for; the prompt comes on stdin.
    assert "sandbox_workspace_write.network_access=true" not in cmd
    assert cmd.endswith(f"- < {paths.prompt} > {paths.transcript}")
    assert env == {}


def test_codex_turns_the_network_on_when_asked(tmp_path: Path) -> None:
    cmd, _ = build_command(
        settings=AgentSettings(runner="codex", command="codex"),
        paths=AttemptPaths(root=tmp_path),
        reads=(),
        output_schema=None,
        network=True,
    )
    assert "-c sandbox_workspace_write.network_access=true" in cmd
    assert "--output-schema" not in cmd
    assert " -m " not in cmd


def test_claude_reads_its_inputs_but_may_not_edit_them(tmp_path: Path) -> None:
    packet = tmp_path / "packet"
    packet.mkdir()
    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({"type": "object"}), encoding="utf-8")
    paths = AttemptPaths(root=tmp_path / "attempt")
    cmd, _ = build_command(
        settings=AgentSettings(runner="claude", command="claude"),
        paths=paths,
        reads=(packet,),
        output_schema=schema,
        network=False,
    )
    tokens = _tokens(cmd)
    assert tokens[:3] == ["cd", str(paths.workspace), "&&"]
    assert tokens[tokens.index("--output-format") + 1] == "stream-json"
    assert tokens[tokens.index("--json-schema") + 1] == '{"type": "object"}'
    assert tokens[tokens.index("--add-dir") + 1] == str(packet)
    refused = tokens[tokens.index("--disallowedTools") + 1 : tokens.index("<")]
    assert f"Edit({packet}/**)" in refused
    assert f"Write({packet}/**)" in refused
    assert {"WebFetch", "WebSearch"} <= set(refused)


def test_claudes_final_message_is_its_structured_output(tmp_path: Path) -> None:
    paths = AttemptPaths(root=tmp_path)
    events = [
        {"type": "assistant", "message": {"usage": {"input_tokens": 999}}},
        {
            "type": "result",
            "result": "done",
            "structured_output": {"verdict": "pass"},
            "usage": {"input_tokens": 40, "output_tokens": 9},
            "total_cost_usd": 0.25,
        },
    ]
    paths.transcript.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    settings = AgentSettings(runner="claude", command="claude")

    message = read_final_message(settings=settings, paths=paths)
    assert message is not None
    assert json.loads(message) == {"verdict": "pass"}
    assert paths.final_message.is_file()
    # Usage nested in a message is a per-message copy, not counted again.
    assert usage_totals(paths.transcript) == {
        "input_tokens": 40,
        "output_tokens": 9,
        "cost_usd": 0.25,
    }


def test_a_transcript_line_that_is_not_json_is_skipped(tmp_path: Path) -> None:
    transcript = tmp_path / "transcript.jsonl"
    transcript.write_text(
        'warning: something\n{"type": "turn.completed", "usage": {"input_tokens": 3}}\n',
        encoding="utf-8",
    )
    assert usage_totals(transcript) == {"input_tokens": 3}
    assert usage_totals(tmp_path / "missing.jsonl") == {}
