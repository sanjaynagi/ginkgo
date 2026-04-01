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
    return script(path="scripts/build_report.py", outputs=output_path)
```

Use notebook tasks when the notebook is part of the workflow output:

```python
from pathlib import Path
from ginkgo import notebook, task

@task("notebook")
def render_report(sample_id: str):
    output=f"report_summary_{sample_id}.csv"
    return notebook(path=f"notebooks/report_{sample_id}.ipynb", output=output)
```

Remote-backed inputs such as `s3://bucket/data.csv` or `oci://registry/path:tag`
should flow through Ginkgo task inputs. Let the runtime stage them locally;
avoid manual download code inside tasks.

Use `.map()` for zip-style fan-out across aligned inputs:

```python
reports = build_report(type='html').map(
    sample_id=["s1", "s2", "s3"],
    output_path=[
        "results/s1.txt",
        "results/s2.txt",
        "results/s3.txt",
    ],
)
```

Use `.product_map()` for Cartesian fan-out when every value on one axis should
pair with every value on another:

```python
comparisons = compare_thresholds(metrics=['accuracy', 'f1']).product_map(
    sample_id=["s1", "s2"],
    threshold=[0.1, 0.2, 0.3],
)
```

Choose `.map()` when lists are meant to line up positionally. Choose
`.product_map()` when you want all combinations.
