# Ginkgo

### Overview

Ginkgo is a Python-native way to write scientific workflows. You write each
step as a normal Python function, and Ginkgo runs them as a graph, handling
parallelism, caching, and a record of what ran. Calling a task doesn't run it
&mdash; it returns a deferred expression, so Ginkgo can build and check the
whole graph before anything executes. Because that graph is built from ordinary
Python, it can change shape while it runs: a task can look at its inputs and
add new steps, which is how Ginkgo handles dynamic DAGs. You get all of this
without rewriting your code into a separate workflow language.

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
      <li><a href="faq/">FAQ</a></li>
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
faq
examples/bioinfo-workflow
reference/api
```
