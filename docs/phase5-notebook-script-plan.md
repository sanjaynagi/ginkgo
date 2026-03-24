# Phase 5 — Notebook and Script Task Contracts: Implementation Plan

## Problem Definition

The current `@notebook(path=...)` decorator is a separate authoring surface that bypasses the
standard task-body contract. The task body of a `@notebook` task is ignored at execution time;
the notebook path is baked into the `TaskDef` at decoration time via `notebook_path`. This
creates three problems:

1. Output paths (declared via `outputs=`) cannot depend on resolved task inputs, because the
   decorator runs at import time before inputs exist.
2. The authoring surface is inconsistent: shell tasks return a `shell(...)` sentinel from
   their body, but notebook tasks cannot — they rely on a separate decorator parameter.
3. There is no equivalent mechanism for script tasks (Python/R scripts with CLI invocation).

Phase 5 fixes this by:
- Adding `notebook(...)` and `script(...)` body primitives modelled on `shell(...)`.
- Routing `@task("notebook")` through a body-based dispatch (task body runs, returns
  `NotebookExpr`), parallel to how `@task("shell")` already works.
- Adding `@task("script")` with a `script(...)` primitive.
- Allowing `kind` as the first positional argument to `@task(...)`.
- Preserving the existing `@notebook(path=...)` decorator for backward compatibility.

---

## Proposed Solution

### 1. New primitives: `notebook(...)` and `script(...)`

**`ginkgo/core/notebook_expr.py`** (new file):

```python
@dataclass(frozen=True)
class NotebookExpr:
    path: str | Path         # source notebook path
    outputs: list[str] | None = None   # optional declared output paths
    log: str | None = None   # optional log path (mirrors ShellExpr)

def notebook(path: str | Path, *, outputs=None, log=None) -> NotebookExpr: ...
```

`path` is the first positional argument so callers can write `notebook("analysis.ipynb")`.
`notebook(path)` computes the source hash of the notebook file at call time and stores it
in `NotebookExpr` for use in cache key construction (see §4).

**`ginkgo/core/script_expr.py`** (new file):

```python
@dataclass(frozen=True)
class ScriptExpr:
    path: str | Path         # script source path
    outputs: list[str] | None = None
    log: str | None = None
    interpreter: str | None = None  # "python"/"rscript"; auto-detected from extension if None

def script(path: str | Path, *, outputs=None, log=None, interpreter=None) -> ScriptExpr: ...
```

`path` is the first positional argument so callers can write `script("fit.py")`.
`script(path)` similarly computes the source hash of the script file at call time.

Interpreter auto-detection: `.py` → `python`, `.R` or `.r` → `Rscript`. Unknown extension
raises `ValueError` at call time.

### 2. Decorator surface: positional `kind`

Change `task()` to accept `kind` as an optional first positional argument:

```python
# current (still supported)
@task(kind="shell")

# new — kind as first positional
@task("shell")
@task("notebook")
@task("script")
```

Implementation: `task()` becomes `task(_kind=None, *, env=None, version=1, retries=0,
kind="python")`. When `_kind` is provided it overrides `kind`. Both spellings remain valid.

### 3. Task kinds: add `"script"`, update `"notebook"` contract

Add `"script"` to `_TASK_KINDS`. Update `execution_mode`:

```python
@property
def execution_mode(self) -> str:
    if self.kind in {"notebook", "shell", "script"}:
        return "driver"
    return "worker"
```

### 4. Cache key: notebook/script source hash

`notebook(nb=path)` and `script(path=path)` compute the file's SHA-256 at call time and
store it as `source_hash` in the sentinel. The evaluator reads this hash when building
the cache key, so notebooks and scripts are re-run when their source changes.

For backward-compatible `@notebook(path=...)` tasks, the existing `NotebookDef.source_hash`
mechanism already handles this — no change needed.

### 5. Evaluator changes

**Dispatch (current `kind="notebook"` block at line ~581):**

The current `kind="notebook"` dispatch submits `_run_notebook()` directly (body is ignored).
After this phase, `kind="notebook"` is dispatched like `kind="shell"` via `_run_driver_task()`:
the body runs and returns `NotebookExpr`, which is then passed to `_run_notebook_expr()`.

Add `kind="script"` dispatch (driver task, body returns `ScriptExpr`).

**`_handle_task_body_result` additions:**

- `kind="notebook"` (new-style): expect `NotebookExpr`; dispatch to `_run_notebook_expr(node, expr)`.
- `kind="script"`: expect `ScriptExpr`; dispatch to `_run_script(node, expr)`.
- Any kind returning the wrong sentinel raises a clear `TypeError`.

**`_run_notebook_expr(node, expr: NotebookExpr)`:**

Mirrors `_run_notebook()` but sources the notebook path and output list from `NotebookExpr`
instead of `task_def.notebook_path`. Output validation: if `expr.outputs` is declared,
validate each path exists after execution. Return value follows §6 semantics.

**`_run_script(node, expr: ScriptExpr)`:**

1. Resolve interpreter from `expr.interpreter` or extension.
2. Forward resolved task inputs as CLI positional arguments (string-ified).
3. Run via `_run_logged_command()`.
4. Validate exit code is 0.
5. If `expr.outputs` is declared, validate each path exists.
6. Return value follows §6 semantics.

### 6. Return-value semantics

Consistent across notebook and script tasks:

| `outputs` | Return value |
|-----------|-------------|
| `None` | For notebooks: HTML path (managed artifact). For scripts: `None`. |
| Single path (`str`) | That path (string). |
| Multiple paths (`list[str]`) | List of paths. |

### 7. Backward compatibility

- `@notebook(path=...)` is removed. All notebook tasks must use `@task("notebook")` returning
  `notebook(path)`.
- `task_def.notebook_path` and `NotebookDef` are removed from `TaskDef`.
- The evaluator's `_run_notebook()` is replaced by `_run_notebook_expr()` which sources
  the notebook path from `NotebookExpr`.
- `@task(kind="shell")` continues to require a `ShellExpr` return.

---

## Risks and Tradeoffs

| Risk | Mitigation |
|------|-----------|
| Source hash computed at call-time (inside task body) rather than definition time | Consistent with how `shell(...)` works — inputs are resolved before the body runs. Hashing happens once per task execution. |
| Script input forwarding as positional CLI args is lossy (no type preservation) | Phase 5 scope only covers string-serialisable scalar inputs (str, int, float, Path). Inputs typed as `file`/`folder` pass their path string. Complex objects are unsupported and raise `TypeError`. |
| Removing `@notebook(path=...)` is a breaking change | Acceptable per the plan's intent to unify the authoring surface. Existing `@notebook(...)` usages in `examples/` will be migrated as part of this phase. |
| Marimo vs ipynb branching now happens inside `_run_notebook_expr()` rather than at decoration time | Identical logic; just sourced from `NotebookExpr.path` instead of `task_def.notebook_path`. |

---

## Implementation Sequence

```
1. notebook_expr.py + script_expr.py  (new primitives)
2. task.py                            (positional kind, add "script", update execution_mode)
3. evaluator.py                       (dispatch, _run_notebook_expr, _run_script, result handling)
4. __init__.py                        (export notebook, NotebookExpr, script, ScriptExpr)
5. Tests
```

Steps 1 and 2 are independent and can proceed in parallel.

---

## Success Criteria (from implementation plan)

- `@task("notebook")` returning `notebook(nb=..., outputs=[...])` validates all declared
  outputs and records them in provenance.
- Notebook task without `outputs` still produces and records managed notebook artifacts
  (executed notebook, HTML).
- `@task("script")` for a Python script forwards resolved inputs as CLI args and validates
  declared outputs.
- `@task("script")` for an R script selects `Rscript` and behaves identically.
- `@task("shell")`, `@task("notebook")`, `@task("script")` as first-positional-kind spellings
  are accepted and equivalent to keyword forms.
- Missing or ambiguous kind fails with a clear error.
- Single-output and multi-output tasks resolve to values matching return annotations.
- Notebook and script tasks are cache-invalidated when their source file changes.
- All `@notebook(path=...)` usages in `examples/` are migrated to the new `@task("notebook")` surface.
