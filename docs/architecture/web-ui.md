# Web UI

The local UI is implemented as a lightweight JSON API server plus a bundled React frontend.

The current UI supports:

- sidebar-first desktop shell with primary navigation (Runs, Assets, Cache, Workspaces)
- multi-workspace session: load any number of local Ginkgo workspaces, switch
  the active workspace via the top bar, and scope runs/cache/workflow-launch to
  that workspace
- local-first workspace loading from any directory: a shallow workspace probe
  keeps startup fast and supports both canonical and non-canonical layouts
- run history and run summaries
- task tables, task-graph visualization using recorded dependencies, and notebook artifact links derived from run provenance
- task detail drawers with full log retrieval
- asset explorer with catalog list, version history, lineage, and metadata
- asset previews for tables/dataframes, figures, PDFs, and text artifacts, plus
  generic metadata views for other asset kinds
- cache browsing and deletion
- live updates via a WebSocket event channel (`/ws`): the server emits
  structured events derived from on-disk provenance changes; the frontend
  applies incremental state updates without full page reloads
- workspace-scoped routes so browser navigation remains stable after switching
  workspaces
- native `Load workspace` integration backed by a local folder picker

When the UI launches a workflow subprocess for an external workspace, it checks
for a `.pixi/` environment directory. If pixi is present, the subprocess
command is `pixi run python -m ginkgo.cli run <workflow>` in that workspace's
own environment so workspace-specific dependencies are importable when the
workflow module is loaded. Workspaces without a pixi environment fall back to
the current interpreter (`sys.executable`).

Each loaded workspace reads directly from that workspace's local `.ginkgo/`
provenance and cache directories. The UI does not depend on a central database
or remote control plane.

DAG layout improvements (fit-to-view, failure focus, richer positioning)
remain future work.
