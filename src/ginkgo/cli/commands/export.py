"""``ginkgo export`` — a run's ledger events or its manifest, as a file."""

from __future__ import annotations

from pathlib import Path
import sys

from ginkgo.cli.common import console, open_run
from ginkgo.cli.renderers.jsonl import event_line
from ginkgo.runtime.rundir import manifest_text, write_manifest

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
            manifest = None
        else:
            manifest = reader.run(run_id).to_payload()
            text = manifest_text(manifest)

    out = Path(args.out) if getattr(args, "out", None) else None
    if out is None:
        sys.stdout.write(text)
        return 0

    if manifest is not None:
        write_manifest(manifest, path=out)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    console(sys.stderr).print(f"Wrote {out}")
    return 0
