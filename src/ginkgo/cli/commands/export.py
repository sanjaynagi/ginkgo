"""``ginkgo export`` — a run's ledger events or its manifest, as a file."""

from __future__ import annotations

from pathlib import Path
import sys

from ginkgo.cli.common import console, open_run
from ginkgo.cli.renderers.jsonl import event_line
from ginkgo.runtime.rundir import manifest_text, write_atomic

__all__ = ["command_export"]


def command_export(args) -> int:
    """Handle ``ginkgo export`` — write one run's record somewhere else.

    Both subcommands print to stdout unless ``--out`` names a file, so an export
    pipes as readily as it saves. Neither invents a format: events go out in the
    shape ``ginkgo run --agent-output`` printed them, and the manifest through
    the same function that wrote the run's own.
    """
    exporting_events = args.export_command == "events"

    with open_run(args.run_id) as (reader, run_id):
        if exporting_events:
            text = "".join(event_line(event.payload) for event in reader.events(run_id))
        else:
            text = manifest_text(reader.run(run_id).to_payload())

    out = Path(args.out) if getattr(args, "out", None) else None
    if out is None:
        sys.stdout.write(text)
        return 0

    write_atomic(text, path=out)
    console(sys.stderr).print(f"Wrote {out}")
    return 0
