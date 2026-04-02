# Config

Base project config normally lives in `ginkgo.toml` and is loaded in the
workflow module with `ginkgo.config(...)`.

Config pattern:

```python
import ginkgo

cfg = ginkgo.config("ginkgo.toml")
```

Load config near the top of `workflow.py` and thread values from `cfg` into flow
composition instead of hard-coding project settings.

Use `--config` to apply one or more overlays at command time without editing the
base file:

```bash
ginkgo run --config overrides/dev.toml
ginkgo run --config overrides/dev.toml --config overrides/local.toml
```

`--config` overlays merge on top of the base config loaded by
`ginkgo.config("ginkgo.toml")`.

Commands such as `ginkgo run`, `ginkgo doctor`, and `ginkgo inspect workflow`
accept `--config` when you need environment- or run-specific overrides.
