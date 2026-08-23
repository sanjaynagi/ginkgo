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
- `tests/workflows/` holds workflow validation files, each run by path like
  any other workflow.

The scaffold is produced by `cli/commands/init.py` from
`src/ginkgo/templates/init/`, where `PACKAGE_NAME` pins the package directory to
`workflow`.

Those templates are the only copy of the starter project. Whatever needs a
runnable starter — the example integration test, the example benchmark case —
materialises one with `write_starter_project` in `cli/commands/init.py` rather
than keeping a checked-in duplicate under `examples/`. A duplicate silently
drifted from the templates once already (#217), and nothing in CI compared the
two trees.

## The project root

`src/ginkgo/project.py` owns the answer to "where is the project root": the
nearest ancestor directory holding a config file named in
`PROJECT_CONFIG_NAMES` (`ginkgo.toml`, `ginkgo.yaml`, `ginkgo.yml`).

- `find_project_root(start_dir)` walks upward from an explicit directory and
  returns `None` when nothing above it is a project. `import_roots_for_path`
  uses this to add the project root to `sys.path`.
- `project_root()` is the authoring-facing form, exported as
  `ginkgo.project_root()`. It walks up from the working directory, so a
  workflow run from `workflow/` resolves the same root as one run from the
  project directory. Because a config file is optional, it falls back to the
  working directory when no marker is found.

The start directory is the working directory rather than the calling module's
`__file__` on purpose: inspecting the caller's frame would work from more
places, but the result would then depend on which file asked, which is harder
to explain than a result that depends on where the command was run.

`config.py` reads `PROJECT_CONFIG_NAMES` for the same purpose when it looks for
the default runtime config, so the set of names that marks a project is written
down once.

## The working directory is the project root

Around forty places in the runtime read `Path.cwd()` as the project root:
`WorkspaceLayout.for_cwd()` puts `.ginkgo/` there, config layering and
environment discovery look for their files there, and the CLI renders run paths
relative to it. Run from a subdirectory, every one of those was wrong the same
way.

`_normalize_working_directory` in `cli/app.py` makes the assumption true
instead of teaching each site to discover the root: after parsing arguments and
before dispatching, the CLI resolves `project_root()` and changes directory to
it. Everything downstream keeps reading `Path.cwd()`, and is now right to.

Two consequences worth knowing:

- **Path arguments are resolved before the move**, while they still mean what
  the user typed — the workflow path, each `--config`, and `report --out`. So
  `ginkgo run flow.py --config override.toml` works from inside `workflow/`.
- **A task's relative output paths land at the project root**, wherever ginkgo
  was invoked from. That is what makes the same command reproducible from two
  different directories, and it is the behaviour change to be aware of.

`ginkgo init` is exempt: it creates a project rather than running inside one, so
its directory argument stays relative to where the user stands — including when
the new project is nested inside an existing one.

Driving ginkgo as a library skips the CLI and so skips the normalisation.
Stores accept an explicit `root=` for that case.

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
topmost package, and adds the nearest project root (see "The project root"
above). So all of the following run today:

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
