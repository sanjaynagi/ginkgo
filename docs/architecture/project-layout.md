# Canonical Workflow Project Layout

Ginkgo scaffolds and auto-discovers a single canonical layout. The package
directory is always named `workflow`, whatever the project directory is called,
so `ginkgo init <dir>` never produces a `<dir>/<dir>/` nesting.

**This layout is a discoverable default, not a requirement.** Nothing in the
runtime demands it. An explicit entry path always wins; any directory name works;
`__init__.py` is needed only for relative imports. See "Structure is a convention,
not a contract" and "Discovery" below for exactly what is and is not enforced.

```text
<project-root>/
├── pixi.toml
├── ginkgo.toml
├── workflow/           # fixed package name
│   ├── __init__.py
│   ├── flow.py         # contains flow definition
│   ├── modules/        # contains tasks, grouped in modules
│   ├── envs/           # per-task Pixi manifests
│   ├── notebooks/      # notebook-task source
│   └── scripts/        # script-task source
├── tests/
│   └── workflows/
├── results/            # runtime-created, optional
└── .ginkgo/            # runtime-created, optional
```

Within that layout:

- `workflow/flow.py` is the canonical CLI entrypoint and should remain thin,
  containing flow definitions and graph wiring only.
- Reusable task implementations live under `workflow/modules/`.
- Task-specific Pixi manifests may live under `workflow/envs/`.
- Notebook-task and script-task sources live under `workflow/notebooks/` and
  `workflow/scripts/`.
- `tests/workflows/` holds workflow validation files for `ginkgo test`.

The scaffold is produced by `cli/commands/init.py` from
`src/ginkgo/templates/init/`, where `PACKAGE_NAME` pins the package directory to
`workflow`.

## The `.ginkgo/` directory

Runtime state lives under `.ginkgo/`, one subdirectory per concern:

```
.ginkgo/
├── runs/                 # per-run provenance
├── cache/                # task cache entries
├── assets/               # asset catalog
├── artifacts/            # content-addressed artifact store
├── staging/              # downloaded remote inputs
├── fuse/                 # mount points for streamed remote inputs
├── notebooks/            # notebook artifacts for runs without provenance
├── reports/              # exported HTML report bundles
└── remote-staged.json    # persisted staging state
```

`WorkspaceLayout` (`src/ginkgo/workspace_layout.py`) owns this convention.
Components ask it for a path rather than rebuilding one, so renaming a
directory or relocating the root is a single edit.

Use `WorkspaceLayout.for_cwd()` for the working-directory default,
`.relative()` where the CLI wants workspace-relative paths for display, and
`.sibling_of(path)` where a component holding one root needs another beside it —
which is how a configured cache root reaches its artifact store, without each
call site restating that the two are siblings.

`sibling_of` does not check that its argument sits inside a `.ginkgo`
directory, and callers do pass roots that do not: a store's `root=` is
caller-supplied. It gathers the assumption the bare `.parent` at each call site
already made rather than validating it.

Stores still accept an explicit `root=`, so a caller can point one somewhere
else; the layout supplies the default and the sibling relationships.

## Structure is a convention, not a contract

`ginkgo run` takes an entry file and runs it whatever the surrounding structure.
`runtime/module_loader.py` loads the entry file **by path**, and
`import_roots_for_path` derives `sys.path` roots adaptively: it adds the entry
file's own directory, climbs the `__init__.py` chain and adds the parent of the
topmost package, and adds the nearest `ginkgo.toml` project root. So all of the
following run today:

- **`workflow/` package** (canonical) — `from workflow.modules… import …` and
  relative imports both resolve.
- **flat, no packages** — sibling modules import as top-level; no `__init__.py`
  needed.
- **`src/<pkg>/flow.py`** — `src/` goes on the path, so
  `from <pkg>.modules… import …` resolves.

Only auto-discovery is structure-aware: it is convenience for running without
typing a path, not a structural requirement.

## Entry-file imports

`load_module_from_path` loads the entry file under its **real dotted name** when
an `__init__.py` sits beside it, setting `__package__`. In the canonical layout
the entry is therefore `workflow.flow`, and both forms work:

```python
from .modules.analysis import build_brief          # relative
from workflow.modules.analysis import build_brief   # absolute
```

A bare entry file (no `__init__.py` beside it) is loaded under a synthetic
top-level name instead, so relative imports are unavailable there — as they are
for any standalone script. Attempting one raises a ginkgo error naming the file
and the `__init__.py` that would fix it, rather than Python's generic "attempted
relative import with no known parent package".

`__init__.py` is required only where your own imports need package resolution:
for the canonical layout that means `workflow/__init__.py` and
`workflow/modules/__init__.py`.

Because the package name is fixed, two ginkgo projects imported into one
interpreter would collide on the name `workflow`. This is a non-issue for
run-in-place execution, which is how ginkgo runs workflows.

## Discovery

Discovery runs only when `ginkgo run` is invoked without a workflow argument.
`canonical_workflow_candidates` looks for a file named `flow.py` at the project
root or in one of its **immediate subdirectories**. Concretely:

- **An explicit path always wins.** `ginkgo run <anything>.py` skips discovery
  entirely, so a project may keep any number of entry files anywhere.
- **The directory name is not checked.** `workflow/` is the scaffolded name and
  the documented convention, but `analysis/flow.py` is discovered just the same.
- **`__init__.py` is not required.** It is a Python packaging marker, not a
  ginkgo one; requiring it here would hide an entry file that runs perfectly
  well. See "Entry-file imports" for when you actually need it.
- **One level deep only.** `src/workflow/flow.py` is not discovered — pass it
  explicitly, or run from `src/`. A root-level `./flow.py` is discovered, which
  is what makes the flat layout usable without an argument.
- **`flow.py` is the only name discovery accepts.** There is no fallback to
  any other entry-file name.

When more than one file qualifies, discovery refuses to guess and names the
candidates, asking for an explicit path. When none does, the error says what was
looked for — `flow.py` — rather than naming a file that may already be there
under a different name.

Because the entry file's own directory is on `sys.path`, a module inside the
package that shares the package's name shadows the package. `flow.py` avoids
that for new projects, but `_load_package_module` still puts the package root's
parent first on `sys.path`, so an explicitly-passed `pkg/pkg.py` still loads
correctly.

## Environment discovery

`PixiRegistry` searches `<project_root>/envs/` and one package-local
`<workflow_root>/envs/`. Both `ginkgo run` and `ginkgo doctor` derive that
package-local root from `resolve_envs_workflow_root`, which anchors on the
**discovered canonical package**, not on the directory of the file being run.
So `ginkgo run experiments/alt_flow.py` still resolves `workflow/envs/`, and
doctor validates the same environment set the run will use.
