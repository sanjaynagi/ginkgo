# Ginkgo

### Overview

A workflow engine earns its keep by doing what plain scripts do badly: running
independent steps in parallel, skipping work that has already been done,
re-running only what changed when data or code moves, and keeping a record of
how every result was produced. Ginkgo brings those benefits to ordinary Python
without asking you to rewrite your code around them.

Most workflow tools make you express a pipeline as an explicit dataflow, which
means giving up the control flow, recursion, and ordinary function calls that
Python already gives you. Ginkgo takes a different route: you write normal
Python functions, mark them with `@task()`, and compose them in a `@flow`.
Calling a task does not run it &mdash; it returns a deferred expression. The
flow assembles those expressions into a graph, and a scheduler evaluates it,
handling parallelism, caching, and provenance for you.

Ginkgo's main features:

- **Deferred expressions.** Task calls build a graph instead of running
  immediately, so the whole workflow can be validated before anything executes.
- **Dynamic DAGs.** A task can inspect its resolved inputs and return new
  expressions, so the graph expands as intermediate results come in.
- **Content-addressed caching.** Results are reused across runs, keyed on task
  source, inputs, and environment; a failed run resumes where it stopped.
- **Reproducible environments.** Shell, script, and notebook tasks can run in
  declared Pixi or container environments.
- **Provenance.** Every run records task status, timing, logs, and artifacts
  for debugging and auditing.
- **Remote execution.** Selected tasks can run on Kubernetes or GCP Batch while
  the rest of the workflow stays local.

New to Ginkgo? Read [Why Ginkgo](motivation/) for the motivation, or jump
straight to the [Quickstart](getting-started/quickstart/).

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
    </ul>
  </article>
</section>
<div class="section-note">
  The quickstart and the bioinformatics example are the best place to start.
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
```
