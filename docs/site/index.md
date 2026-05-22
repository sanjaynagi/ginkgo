```{raw} html
<section class="hero">
  <span class="hero-eyebrow">Python workflow orchestrator</span>
  <h1>Ginkgo</h1>
  <p>
    Write workflows as ordinary Python. Ginkgo defers each task into a graph it
    can schedule, cache, and reproduce &mdash; so reruns reuse prior work
    instead of repeating it.
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
    <p>Inspect runs, logs, artifacts, and workflow structure through the CLI and run records.</p>
  </article>
</section>
```

### Overview

You write task functions, compose them inside a flow, and hand the resulting
graph to a runtime that schedules, caches, and records every step.

Calling a task does not run it &mdash; it returns a deferred expression. The
flow assembles those expressions into a graph, and the runtime decides what to
execute, what to reuse from cache, and what to record.

Ginkgo fits workflows that need:

- a Python-native authoring model
- mixed local Python and shell-based execution
- repeatable environments for selected tasks
- output reuse across reruns

New to Ginkgo? Read [Why Ginkgo](motivation/) for the motivation, or jump
straight to the [Quickstart](getting-started/quickstart/).

### A Minimal Workflow

```python
from ginkgo import flow, task


@task()
def write_text(message: str, output_path: str) -> str:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)
    return output_path


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="hello.txt")
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
      <li><a href="motivation/">Why Ginkgo</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>User guide</h3>
    <ul>
      <li><a href="guide/concepts/">Core concepts</a></li>
      <li><a href="guide/tasks-and-flows/">Tasks and flows</a></li>
      <li><a href="guide/resources/">Resources and scheduling</a></li>
      <li><a href="guide/environments/">Environments</a></li>
      <li><a href="guide/assets/">Assets and reports</a></li>
      <li><a href="guide/caching-and-provenance/">Caching and provenance</a></li>
      <li><a href="guide/cli/">CLI</a></li>
      <li><a href="guide/coding-agents/">Working with coding agents</a></li>
      <li><a href="guide/notifications/">Notifications</a></li>
      <li><a href="guide/remote-execution/">Remote execution</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Examples</h3>
    <ul>
      <li><a href="examples/bioinfo-workflow/">Canonical bioinformatics workflow</a></li>
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
motivation
guide/concepts
guide/tasks-and-flows
guide/resources
guide/environments
guide/assets
guide/caching-and-provenance
guide/cli
guide/coding-agents
guide/notifications
guide/remote-execution
examples/bioinfo-workflow
reference/api
explanation/architecture
```
