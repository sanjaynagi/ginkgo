# Phase 12 — ML Model Support

## Problem Definition

ML practitioners using Ginkgo can already build training pipelines with `@task`
and `.map()`, but every model artifact is an anonymous cached task output. There
is no way to ask "show me all versions of my classifier", "which one had the
best F1", or "use the production model in my scoring pipeline". Metrics are
buried in task outputs, parameter exploration requires manual `.map()` wiring,
there is no structured comparison across training runs, and stochastic training
behavior is not recorded as an explicit runtime contract.

This phase adds four capabilities through the existing `kind=` extension point:

1. **`kind="model"`** — versioned model assets with serialization and promotion
2. **`kind="eval"`** — structured evaluation records with comparison
3. **`.sweep()`** — parameter exploration with provenance tracking
4. **`rng_policy=`** — explicit determinism policy for stochastic tasks

These require a thin **asset foundation** that does not yet exist. This plan
starts with the minimum asset infrastructure needed, then builds ML features on
top.

---

## Target DSL

```python
from ginkgo import task, flow, model, eval, file

@task(kind="model", rng_policy="seeded")
def train(data: file, *, lr: float, epochs: int):
    clf = fit(load(data), lr=lr, epochs=epochs)
    return model(clf, framework="sklearn")

@task(kind="eval")
def evaluate(m: model, test_data: file):
    clf = m.load()
    preds = clf.predict(load(test_data))
    return eval(metrics={"accuracy": acc, "f1": f1, "auc": auc})

@flow
def main():
    data = prepare_data(raw=file("data/raw.csv"))
    test = prepare_test(raw=file("data/test.csv"))

    models = train.sweep(
        data=data,
        lr=[0.001, 0.01, 0.1],
        epochs=[10, 50, 100],
    )
    evals = evaluate.map(m=models, test_data=test)
    return evals
```

After execution:

```
$ ginkgo eval compare evaluate
version   model_ver  lr      epochs  accuracy  f1     auc
e-a1b2    v-f3c4     0.001   10      0.82      0.79   0.88
e-d5e6    v-g7h8     0.01    50      0.91      0.87   0.94
e-i9j0    v-k1l2     0.1     100     0.88      0.84   0.91

$ ginkgo model promote train v-g7h8 production
```

Downstream consumption by alias:

```python
@flow
def scoring():
    m = model.ref("train@production")
    return batch_score(m=m, input_data=file("data/new.csv"))
```

---

## Part 1 — Asset Foundation

### Goal

Build the minimum asset infrastructure that `kind="model"` and `kind="eval"`
need: stable identity, immutable versioning, and a local file-backed store.
This is a subset of the full Phase 10 asset catalog — no lineage graph, no
catalog API, no UI asset browser yet.

### 1.1 Asset Identity

**File:** `ginkgo/core/asset.py`

```python
@dataclass(frozen=True, kw_only=True)
class AssetKey:
    """Stable identity for a named asset, derived from the producing task."""
    namespace: str          # e.g. "model", "eval"
    name: str               # task function name (or explicit override)

@dataclass(frozen=True, kw_only=True)
class AssetVersion:
    """One immutable materialization of an asset."""
    version_id: str         # content-addressed hash
    asset_key: AssetKey
    run_id: str
    task_id: str
    created_at: str         # ISO timestamp
    content_hash: str       # SHA-256 of the serialized artifact
    metadata: dict          # kind-specific (metrics, params, framework, etc.)
```

- `AssetKey` is derived from the task definition name, prefixed by namespace.
  A `kind="model"` task named `train` produces asset key `model/train`.
- `AssetVersion.version_id` is a short hash of (asset_key + content_hash +
  run_id), giving deterministic but unique version identifiers.

### 1.2 Asset Store

**File:** `ginkgo/runtime/asset_store.py`

A local file-backed store under `.ginkgo/assets/`:

```
.ginkgo/assets/
└── model/
    └── train/
        ├── index.yaml            # version list, alias map
        └── versions/
            └── v-<hash>/
                ├── meta.yaml     # AssetVersion fields
                └── artifact/     # kind-specific payload
```

Interface:

```python
class AssetStore:
    def __init__(self, *, ginkgo_dir: Path): ...

    def register_version(self, *, version: AssetVersion, artifact_dir: Path) -> None:
        """Atomically write a new immutable version."""

    def get_version(self, *, key: AssetKey, version_id: str) -> AssetVersion: ...
    def get_latest(self, *, key: AssetKey) -> AssetVersion | None: ...
    def list_versions(self, *, key: AssetKey) -> list[AssetVersion]: ...
    def list_keys(self, *, namespace: str | None = None) -> list[AssetKey]: ...

    def set_alias(self, *, key: AssetKey, alias: str, version_id: str) -> None:
        """Point a mutable alias at an immutable version."""

    def resolve_alias(self, *, key: AssetKey, alias: str) -> AssetVersion | None: ...
    def resolve_ref(self, *, ref: str) -> AssetVersion:
        """Parse 'name@alias' or 'name@version_id' and resolve."""
```

Design notes:

- Writes are atomic (write to temp dir, rename into place).
- `index.yaml` is the single mutable file per asset key — it holds the version
  list and alias map. Version directories are immutable once written.
- The store is deliberately simple: no locking beyond atomic rename, no
  concurrent-writer support. This matches Ginkgo's single-scheduler model.

### 1.3 Provenance Integration

Extend run manifests to record asset materialization when a task produces one:

```yaml
task_0003:
  # ... existing fields ...
  asset:
    key: "model/train"
    version_id: "v-f3c4a1"
    content_hash: "sha256:..."
```

This is a new optional field on the task manifest entry. No changes to existing
provenance fields.

### 1.4 Validation

- Write an `AssetVersion` to the store and read it back; assert immutability.
- Write two versions of the same key; assert `get_latest` returns the second.
- Set an alias, resolve it, re-point it, and assert resolution changes.
- Assert `resolve_ref("train@production")` and `resolve_ref("train@v-abc123")`
  both work.
- Assert atomic write: interrupt mid-write and verify no partial version exists.

---

## Part 2 — Model Assets (`kind="model"`)

### Goal

A new task kind that produces versioned, serialized model artifacts stored in
the asset store with structured metadata.

### 2.1 Model Sentinel and Builder

**File:** `ginkgo/core/model.py`

Following the `shell()` / `ShellExpr` pattern:

```python
@dataclass(frozen=True, kw_only=True)
class ModelResult:
    """Sentinel returned from a kind='model' task body."""
    obj: Any                 # the model object to serialize
    framework: str           # serializer selection key
    metrics: dict | None = None
    metadata: dict | None = None

def model(obj, *, framework: str, metrics: dict | None = None,
          metadata: dict | None = None) -> ModelResult:
    """Build a model result inside a kind='model' task body."""
    return ModelResult(obj=obj, framework=framework, metrics=metrics,
                       metadata=metadata)
```

`model` is also used as a **type annotation** on task parameters to indicate
"this input is a model asset". When used as an annotation, it behaves like
`file` — a marker type that the codec and cache systems recognize.

```python
@dataclass(frozen=True, kw_only=True)
class ModelRef:
    """Resolved model reference passed to consuming tasks."""
    asset_key: AssetKey
    version_id: str
    artifact_path: Path
    metrics: dict | None
    params: dict | None

    def load(self) -> Any:
        """Deserialize the model artifact."""
```

For referencing a model by alias in a flow:

```python
@staticmethod
def model.ref(ref: str) -> Expr[model]:
    """Resolve 'task_name@alias' or 'task_name@version_id' at graph build time."""
```

### 2.2 Serializer Protocol

**File:** `ginkgo/runtime/model_serializers.py`

```python
class ModelSerializer(Protocol):
    name: str
    def save(self, obj: Any, path: Path) -> None: ...
    def load(self, path: Path) -> Any: ...
```

Initial implementations:

| Serializer | Framework | Method |
|---|---|---|
| `PickleSerializer` | `"pickle"` | `pickle.dumps/loads` (universal fallback) |
| `SklearnSerializer` | `"sklearn"` | `joblib.dump/load` |
| `TorchSerializer` | `"torch"` | `torch.save/load` (state_dict convention) |

Serializers are registered by name in a simple dict. Unknown framework names
raise at task completion time with a clear error message.

### 2.3 Task Kind Registration and Execution

**Changes to existing files:**

`ginkgo/core/task.py`:
- Add `"model"` to `_TASK_KINDS`
- `execution_mode` for `kind="model"` → `"driver"` (same as shell — task body
  runs on scheduler, produces a sentinel, then the evaluator handles
  serialization and storage)

`ginkgo/runtime/evaluator.py`:
- In `_handle_task_body_result()`: detect `ModelResult` sentinel (parallel to
  `ShellExpr` detection)
- New `_handle_model_result()` method:
  1. Serialize the model object via the framework serializer to a temp dir
  2. Compute content hash of the serialized artifact
  3. Build `AssetVersion` with metadata (metrics, params from resolved task
     inputs, framework, run_id, task_id)
  4. Register the version in the asset store
  5. Return a `ModelRef` as the task's resolved output value

`ginkgo/runtime/cache.py`:
- When a task input is a `ModelRef`, hash the `version_id` (not the serialized
  bytes). This gives cheap, stable cache keys for model consumers.

`ginkgo/runtime/value_codec.py`:
- Add codec support for `ModelRef` so it can cross process boundaries and be
  persisted in cache/provenance.

### 2.4 Param Capture

When a `kind="model"` task completes, the evaluator records the task's resolved
input arguments as `params` in the `AssetVersion.metadata`. This happens
automatically — the practitioner does not need to pass params explicitly. The
metrics (passed in `model(...)`) and params (captured from inputs) are both
stored in `meta.yaml`.

This is what enables eval comparison to show sweep parameters without manual
logging — the params trace through the DAG automatically.

### 2.5 RNG Policy

ML training and evaluation make the need obvious, but RNG control should not be
an ML-only feature. This phase introduces a general task-level RNG policy in
the core runtime, with `kind="model"` and `kind="eval"` as the first strong
consumers:

```python
@task(kind="model", rng_policy="off")
@task(kind="model", rng_policy="seeded")
@task(kind="model", rng_policy="strict")
```

Policy semantics:

- `off`
  - Ginkgo does not manage RNG state.
  - The task may remain nondeterministic.
- `seeded`
  - Ginkgo derives a stable per-task seed from cache identity and task
    identity.
  - The seed is exported as `GINKGO_TASK_SEED` and recorded in provenance.
  - For driver-executed Python task bodies, Ginkgo seeds supported backends
    such as `random` and NumPy when available.
- `strict`
  - Same behavior as `seeded`.
  - In addition, Ginkgo fails task completion if strict seeding cannot be
    applied for supported backends, or if the runtime cannot uphold the
    requested determinism contract.

Design notes:

- `rng_policy` is folded into cache identity so changing policy invalidates
  prior cache entries.
- Seed derivation is deterministic for a given logical task execution. Retries
  reuse the same seed unless Ginkgo later adds an explicit retry-randomization
  option.
- The applied `rng_policy`, derived seed, and seeded backends are recorded in
  general run provenance for any task kind that uses the feature. Model and
  eval assets additionally copy this metadata into `AssetVersion.metadata`.
- The initial enforcement scope is intentionally narrow: the runtime guarantees
  seeded behavior only for supported execution paths and supported RNG
  backends. It does not promise universal interception of every RNG source in
  arbitrary third-party code.

Execution changes:

- `ginkgo/core/task.py`:
  - Add `rng_policy` as a task option with allowed values `"off"`,
    `"seeded"`, and `"strict"`.
- `ginkgo/runtime/evaluator.py`:
  - Derive per-task seed metadata before invoking supported task execution
    paths.
  - Export `GINKGO_TASK_SEED` and apply supported Python RNG seeding helpers.
  - Persist RNG metadata into provenance for all task kinds.
- `ginkgo/runtime/cache.py`:
  - Fold `rng_policy` into the cache key payload.
- `ginkgo/runtime/value_codec.py` / worker execution paths:
  - Preserve access to `GINKGO_TASK_SEED` where task kinds cross process
    boundaries.
- ML asset handling:
  - Copy general RNG provenance into model and eval asset metadata when those
    task kinds materialize assets.

This gives Ginkgo a general reproducibility contract for stochastic tasks,
while Phase 9 uses it to make ML training and evaluation runs inspectable and
repeatable without forcing users to plumb seeds manually through every task.

### 2.6 CLI

```
ginkgo model ls                              # list model asset keys
ginkgo model versions <name>                 # list versions with metrics summary
ginkgo model inspect <name>@<ver|alias>      # full metadata, params, lineage
ginkgo model promote <name> <ver> <alias>    # move alias pointer
```

### 2.7 Validation

- A `kind="model"` task with `framework="sklearn"` serializes and registers an
  immutable model version with correct metadata.
- Re-running with identical inputs hits the cache and does not create a
  duplicate version.
- Re-running with changed inputs creates a new version; `get_latest` updates.
- A downstream task consuming `model` receives a `ModelRef` with a working
  `.load()` method.
- `model.ref("train@production")` resolves to the promoted version and
  invalidates downstream cache when the alias moves.
- Provenance records the asset key and version id on the producing task.
- A `rng_policy="seeded"` model task records a stable seed in provenance and
  asset metadata.
- Changing `rng_policy` from `"off"` to `"seeded"` invalidates cache
  deterministically.
- A `rng_policy="strict"` task fails clearly when Ginkgo cannot uphold the
  requested determinism contract.

---

## Part 3 — Sweep (`.sweep()`)

### Goal

Reduce parameter exploration boilerplate and record sweep metadata in
provenance.

### 3.1 Implementation

**File:** `ginkgo/core/task.py` (method on `TaskDef`)

`.sweep()` is a method on `TaskDef` / `PartialCall`, parallel to `.map()`:

```python
def sweep(self, *, strategy: str = "grid", **kwargs) -> ExprList:
    """Fan out over parameter combinations with sweep provenance.

    Parameters
    ----------
    strategy
        'grid' for Cartesian product, 'zip' for positional pairing.
    **kwargs
        Fixed args (single values) and swept args (lists).
    """
```

Implementation:

1. Partition kwargs into fixed (scalar) and swept (list) arguments.
2. Compute combinations:
   - `"grid"` → `itertools.product` of all swept arg lists
   - `"zip"` → `zip` of all swept arg lists (must be equal length)
3. Build the concrete arg dicts for each combination.
4. Call the existing `.map()` machinery to produce an `ExprList`.
5. Attach sweep metadata to the `ExprList` (new optional field):

```python
@dataclass(frozen=True, kw_only=True)
class SweepMeta:
    strategy: str
    axes: dict[str, list]       # swept parameter names → values
    n_combinations: int
```

### 3.2 Provenance

When the evaluator processes an `ExprList` with `SweepMeta`, it records the
sweep metadata on each constituent task's manifest entry:

```yaml
task_0003:
  # ... existing fields ...
  sweep:
    strategy: grid
    axes: {lr: [0.001, 0.01, 0.1], epochs: [10, 50, 100]}
    index: 4            # which combination this task represents
    n_combinations: 9
```

### 3.3 Validation

- `train.sweep(data=d, lr=[0.01, 0.1], epochs=[10, 50], strategy="grid")`
  produces 4 tasks with correct parameter combinations.
- `strategy="zip"` with equal-length lists produces N tasks (not N*M).
- `strategy="zip"` with unequal-length lists raises a clear error.
- Sweep metadata appears in the run manifest for each swept task.
- Caching works unchanged — each combination is an independent task with its
  own cache key.

---

## Part 4 — Eval Records (`kind="eval"`)

### Goal

A new task kind that produces structured evaluation metrics, linked to the
model version being evaluated, stored for comparison.

### 4.1 Eval Sentinel and Builder

**File:** `ginkgo/core/eval.py`

```python
@dataclass(frozen=True, kw_only=True)
class EvalResult:
    """Sentinel returned from a kind='eval' task body."""
    metrics: dict[str, float]
    artifacts: dict[str, str] | None = None   # name → file path

def eval(*, metrics: dict[str, float],
         artifacts: dict[str, str] | None = None) -> EvalResult:
    """Build an eval result inside a kind='eval' task body."""
    return EvalResult(metrics=metrics, artifacts=artifacts)
```

Resolved output type:

```python
@dataclass(frozen=True, kw_only=True)
class EvalRecord:
    """Resolved eval record passed to downstream tasks or stored."""
    asset_key: AssetKey
    version_id: str
    metrics: dict[str, float]
    params: dict                   # captured from task inputs
    model_version: str | None      # if any input was a ModelRef
    artifact_paths: dict[str, Path] | None
```

### 4.2 Execution

`kind="eval"` → `execution_mode = "driver"` (same pattern as shell and model).

In `_handle_task_body_result()`, detect `EvalResult`:
1. Capture metrics from the sentinel.
2. Inspect resolved task inputs — if any input is a `ModelRef`, record its
   `version_id` as `model_version`. This links the eval to the model
   automatically.
3. Capture all resolved task inputs as params (same as model param capture).
4. Copy any declared artifact files to the eval version directory.
5. Build `AssetVersion` (namespace `"eval"`) and register in the asset store.
6. Return an `EvalRecord` as the task's resolved output.

`kind="eval"` also supports `rng_policy=` with the same semantics as model
tasks. This is mainly useful for stochastic evaluation procedures such as
bootstrap confidence intervals, randomized data subsampling, or Monte Carlo
metrics. Eval asset metadata records the applied policy and derived seed when
enabled.

### 4.3 CLI

```
ginkgo eval ls                              # list eval asset keys
ginkgo eval compare <name>                  # tabular comparison of all versions
ginkgo eval inspect <name>@<ver>            # full detail
```

`ginkgo eval compare` is the key command. It reads all versions of an eval
asset key and renders a table:

```
version   model_ver  lr      epochs  accuracy  f1     auc
e-a1b2    v-f3c4     0.001   10      0.82      0.79   0.88
e-d5e6    v-g7h8     0.01    50      0.91      0.87   0.94
e-i9j0    v-k1l2     0.1     100     0.88      0.84   0.91
```

The params columns (lr, epochs) come from the captured task inputs. The
model_ver column comes from the linked `ModelRef`. No manual logging required.

### 4.4 Validation

- A `kind="eval"` task stores structured metrics and links to the upstream
  model version automatically.
- `ginkgo eval compare` renders correct columns from metrics and inherited
  params.
- Re-running with identical inputs hits cache and does not duplicate the eval
  record.
- Eval artifacts (confusion matrix plots, etc.) are stored and retrievable.

---

## Part 5 — UI Integration

### Goal

Add Models and Evals sections to the sidebar for browsing and comparison.

### 5.1 Models View

New sidebar section: **Models**

- **List view:** model asset keys with latest version summary (metrics, alias
  badges).
- **Version detail:** metrics table, params, producing run link, framework,
  alias badges.
- **Promote action:** move an alias pointer from the version detail view.

### 5.2 Evals View

New sidebar section: **Evals**

- **Comparison table:** rows = eval versions, columns = metric names + params.
  Sortable by any column.
- **Run linkage:** each row links to the producing run and model version.
- **Detail view:** full metrics, artifacts, upstream lineage.

### 5.3 API Routes

New workspace-scoped routes:

```
GET /workspaces/{id}/models                  # list model keys
GET /workspaces/{id}/models/{name}           # list versions
GET /workspaces/{id}/models/{name}/{ver}     # version detail
POST /workspaces/{id}/models/{name}/promote  # move alias

GET /workspaces/{id}/evals                   # list eval keys
GET /workspaces/{id}/evals/{name}            # comparison table data
GET /workspaces/{id}/evals/{name}/{ver}      # version detail
```

### 5.4 Validation

- Models sidebar lists model keys and versions with metrics.
- Promote action updates the alias and is reflected immediately.
- Evals comparison table renders sortable columns for all metric names.
- Each eval row links to its model version and producing run.

---

## Part 6 — Example Workflow

Update `examples/ml/` to demonstrate the full ML workflow:

```python
@task(kind="model")
def train(data: file, *, lr: float, epochs: int):
    clf = fit(load(data), lr=lr, epochs=epochs)
    return model(clf, framework="sklearn")

@task(kind="eval")
def evaluate(m: model, test_data: file):
    clf = m.load()
    preds = clf.predict(load(test_data))
    return eval(metrics={"accuracy": acc, "f1": f1})

@flow
def main():
    data = prepare_data(raw=file("data/raw.csv"))
    test = prepare_test(raw=file("data/test.csv"))
    models = train.sweep(data=data, lr=[0.001, 0.01, 0.1], epochs=[10, 50])
    evals = evaluate.map(m=models, test_data=test)
    return evals
```

End-to-end test asserts:
- Model versions created with correct params and metrics
- Eval records link to model versions with inherited sweep params
- `ginkgo eval compare` renders the expected table
- Cache reuse on rerun
- Alias promotion changes downstream cache invalidation

---

## Implementation Sequence

| Step | Scope | Dependencies |
|---|---|---|
| 1 | Asset identity (`AssetKey`, `AssetVersion`) | None |
| 2 | Asset store (local file-backed) | Step 1 |
| 3 | `kind="model"` — sentinel, serializers, evaluator dispatch | Steps 1–2 |
| 4 | Model codec, cache integration, param capture | Step 3 |
| 5 | `.sweep()` method on TaskDef | None (can parallel with 3–4) |
| 6 | `kind="eval"` — sentinel, evaluator dispatch, model linkage | Steps 3–4 |
| 7 | CLI commands (model ls/versions/promote, eval ls/compare) | Steps 3–6 |
| 8 | Provenance integration (asset + sweep fields) | Steps 3–6 |
| 9 | UI — Models sidebar | Step 7 |
| 10 | UI — Evals sidebar with comparison table | Step 7 |
| 11 | Example workflow update | Steps 3–6 |

---

## Risks and Tradeoffs

| Risk | Mitigation |
|---|---|
| Asset store is thinner than full Phase 10 | Clean interface; Phase 10 wraps it later without breaking changes |
| Serializer plugins add framework dependencies | Lazy imports; only `pickle` is zero-dep. Serializers fail clearly if framework missing |
| `.sweep()` overlaps with external HPO (Optuna) | Deliberately simple (grid/zip only); not building Bayesian optimization |
| Param capture from task inputs may include large objects | Capture scalar inputs only; skip file/folder/model refs in param dict |
| Eval comparison at scale needs a DB | Index files work for local-first; Phase 9 handles scale |

---

## Success Criteria

- Train a model with `kind="model"`, consume it downstream, and assert
  versioned storage with auto-captured params and metrics.
- Run `.sweep()` with grid strategy and assert correct Cartesian expansion with
  sweep metadata in provenance.
- Record `kind="eval"` results that automatically link to the upstream model
  version and inherit sweep params.
- `ginkgo eval compare` renders the comparison table without any manual metric
  logging.
- Promote a model alias and assert downstream `model.ref("name@alias")`
  invalidates cache correctly.
- UI sidebar shows Models and Evals sections with browsable data.
