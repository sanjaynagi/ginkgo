```{raw} html
<section class="hero">
  <h1>Ginkgo</h1>
  <p>
    Ginkgo is a Python workflow orchestrator with a deferred-expression DSL,
    content-addressed caching, and reproducible task environments.
  </p>
  <div class="hero-actions">
    <a href="getting-started/quickstart/">Quickstart</a>
    <a href="examples/bioinfo-workflow/">Bioinformatics example</a>
    <a href="guide/cli/">CLI</a>
  </div>
</section>
```

```{raw} html
<section class="quick-grid">
  <article class="quick-card">
    <h3>Dynamic</h3>
    <p>Build workflows that can expand at runtime when task results determine what should happen next.</p>
  </article>
  <article class="quick-card">
    <h3>Pythonic</h3>
    <p>Author workflows in ordinary Python with <code>@flow</code>, <code>@task()</code>, and explicit typed task boundaries.</p>
  </article>
  <article class="quick-card">
    <h3>Reproducible</h3>
    <p>Reuse prior work through content-addressed caching and run shell steps in declared environments.</p>
  </article>
  <article class="quick-card">
    <h3>Agent-friendly</h3>
    <p>Inspect runs, logs, artifacts, and workflow structure clearly through the CLI and run records.</p>
  </article>
</section>
```

### Overview

Ginkgo is intended for workflows that begin as ordinary Python but need clearer
execution boundaries over time. You write task functions, compose them inside a
flow, and let the runtime schedule, cache, and record the resulting graph.

It works especially well when you need all of the following at once:

- a Python-native authoring model
- mixed local Python and shell-based execution
- repeatable environments for selected tasks
- output reuse across reruns

### A Minimal Workflow

```python
from pathlib import Path

from ginkgo import flow, task


@task()
def write_text(message: str, output_path: str) -> str:
    output = Path(output_path)
    output.write_text(message, encoding="utf-8")
    return str(output)


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="results/hello.txt")
```

Run it with:

```bash
ginkgo run workflow.py
```

### Contents

```{raw} html
<section class="contents-grid">
  <article class="contents-block">
    <h3>Getting started</h3>
    <ul>
      <li><a href="getting-started/installation/">Installation</a></li>
      <li><a href="getting-started/quickstart/">Quickstart</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>User guide</h3>
    <ul>
      <li><a href="guide/concepts/">Core concepts</a></li>
      <li><a href="guide/tasks-and-flows/">Tasks and flows</a></li>
      <li><a href="guide/environments/">Environments</a></li>
      <li><a href="guide/notebooks/">Notebook tasks</a></li>
      <li><a href="guide/caching-and-provenance/">Caching and provenance</a></li>
      <li><a href="guide/cli/">CLI</a></li>
      <li><a href="guide/notifications/">Notifications</a></li>
      <li><a href="guide/remote-execution/">Remote execution</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Examples</h3>
    <ul>
      <li><a href="examples/bioinfo-workflow/">Canonical bioinformatics workflow</a></li>
      <li><a href="examples/notebook-task/">Notebook task example</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Reference</h3>
    <ul>
      <li><a href="reference/api/">API reference</a></li>
      <li><a href="explanation/architecture/">Architecture overview</a></li>
    </ul>
  </article>
</section>
<div class="section-note">
  The docs site is organized for end users first. Use the quickstart and the
  bioinformatics example as the main path through the material.
</div>
```

```{toctree}
:hidden:

getting-started/installation
getting-started/quickstart
guide/concepts
guide/tasks-and-flows
guide/environments
guide/notebooks
guide/caching-and-provenance
guide/cli
guide/notifications
guide/remote-execution
examples/bioinfo-workflow
examples/notebook-task
reference/api
explanation/architecture
```
