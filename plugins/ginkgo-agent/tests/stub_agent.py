#!/usr/bin/env python3
"""A stand-in agent CLI: does what STUB_MODE says, with no model call.

It reads the prompt on stdin, writes it to report.md in its workspace, reports
token usage as Codex does, and leaves a final message. STUB_CALLS, if set,
names a file it appends one line to per call.
"""

import json
import os
import sys
import time
from pathlib import Path

prompt = sys.stdin.read()
workspace = Path(os.environ["GINKGO_AGENT_WORKSPACE"])
mode = os.environ.get("STUB_MODE", "ok")
calls = os.environ.get("STUB_CALLS")
if calls:
    with open(calls, "a", encoding="utf-8") as handle:
        handle.write(f"{Path.cwd()}\n")
    if mode == "fail_first" and len(Path(calls).read_text().splitlines()) == 1:
        mode = "fail"

print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 10, "output_tokens": 5}}))
print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 2}}))
(workspace / "report.md").write_text(prompt, encoding="utf-8")

if mode == "fail":
    print("the stub was told to fail", file=sys.stderr)
    sys.exit(3)
if mode == "sleep":
    time.sleep(60)
if mode == "write_read":
    first_read = os.environ["GINKGO_AGENT_READS"].splitlines()[0]
    (Path(first_read) / "tampered.txt").write_text("x", encoding="utf-8")

final = {
    "bad_json": "this is not JSON",
    "schema_mismatch": json.dumps({"verdict": 3}),
}.get(mode, json.dumps({"verdict": "pass"}))
if mode != "no_message":
    Path(os.environ["GINKGO_AGENT_FINAL_MESSAGE"]).write_text(final, encoding="utf-8")
