# Task patterns

Use normal Python tasks for orchestration and Python logic:

```python
from ginkgo import file, task

@task()
def summarize(*, input_path: file, output_path: str) -> file:
    ...
```

Use shell tasks when the real unit of work is a command with declared outputs:

```python
from ginkgo import shell, task

@task(kind="shell")
def normalize(*, input_path: str, output_path: str):
    return shell(cmd=f"tr a-z A-Z < {input_path} > {output_path}", output=output_path)
```

Use script tasks when a standalone script should run in a task-local Pixi env:

```python
from pathlib import Path
from ginkgo import script, task

@task("script", env="analysis_tools")
def build_report(*, output_path: str):
    return script(Path("scripts/build_report.py"), outputs=output_path)
```

Use notebook tasks when the notebook is part of the workflow output:

```python
from pathlib import Path
from ginkgo import notebook, task

@task("notebook")
def render_report():
    return notebook(Path("notebooks/report.ipynb"))
```

Notebook guidance:

- keep notebooks deterministic and parameter-driven
- avoid relying on hidden cell state
- avoid large checked-in output blobs unless they are deliberate workflow inputs

Remote-backed inputs such as `s3://bucket/data.csv` or `oci://registry/path:tag`
should flow through Ginkgo task inputs. Let the runtime stage them locally;
avoid manual download code inside tasks.
