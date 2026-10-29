# Ginkgo

### Overview

Ginkgo is a Python-native way to write scientific workflows. You write each
task as a normal Python function. Ginkgo runs the tasks in the right order, runs
them in parallel where it can, caches their results, and records what ran. That
record is called [provenance](guide/concepts.md#glossary).

Calling a task doesn't run it. The call returns an
[expression](guide/concepts.md#glossary), which is a placeholder for the
result. Ginkgo links the expressions into a graph of tasks, and checks the
whole graph before anything runs.

The graph can grow while the workflow runs. A task can look at its inputs and
add new tasks. For example, a task can read a sample list and add one task per
sample.

New to Ginkgo? Install it, run the [Quickstart](getting-started/quickstart.md),
then read [Core Concepts](guide/concepts.md). [Why Ginkgo](motivation.md)
explains the principles Ginkgo is built on.

```{raw} html
<video controls muted playsinline preload="none" poster="../_static/walkthrough-poster.jpg" style="width: 100%; border-radius: 8px;">
  <source src="../_static/walkthrough.mp4" type="video/mp4">
</video>
```

The video runs a bacterial genome-wide association study (GWAS). It shows the
graph growing as the workflow runs, tasks running in Pixi environments,
caching, provenance, measured resource usage, and resuming after a failure.
Pixi is a package manager that builds the environments your tools run in.

```{raw} html
<section class="quick-grid">
  <article class="quick-card">
    <h3>Plain Python</h3>
    <p>You write workflows as ordinary Python functions, with <code>@flow</code> and <code>@task()</code>. There is no separate workflow language to learn.</p>
  </article>
  <article class="quick-card">
    <h3>Graphs that grow</h3>
    <p>A task can look at its inputs and add new tasks. The workflow takes the shape the data needs.</p>
  </article>
  <article class="quick-card">
    <h3>Built for agents</h3>
    <p>AI agents can write, run and inspect workflows as well as people can, through plain Python, JSON output and a queryable record of every run.</p>
  </article>
</section>
```

### A Minimal Workflow

Save this as `flow.py`:

```python
from ginkgo import Out, file, flow, task


@task()
def write_text(message: str, output_path: Out[file]):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="hello.txt")
```

`Out[file]` marks `output_path` as a file the task writes. The flow returns
the task call, and that tells Ginkgo what to run.

Run it from the same directory:

```bash
ginkgo run flow.py
```

Ginkgo runs `write_text`, and `hello.txt` appears in the current directory.

### Contents

```{raw} html
<section class="contents-grid">
  <article class="contents-block">
    <h3>Getting started</h3>
    <ul>
      <li><a href="../getting-started/installation/">Installation</a></li>
      <li><a href="../getting-started/quickstart/">Quickstart</a></li>
      <li><a href="../motivation/">Why Ginkgo</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Writing workflows</h3>
    <ul>
      <li><a href="../guide/concepts/">Core concepts</a></li>
      <li><a href="../guide/tasks-and-flows/">Tasks and flows</a></li>
      <li><a href="../guide/outputs/">Declaring outputs</a></li>
      <li><a href="../guide/environments/">Environments</a></li>
      <li><a href="../guide/resources/">Resources and scheduling</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Running and inspecting</h3>
    <ul>
      <li><a href="../guide/cli/">CLI</a></li>
      <li><a href="../guide/caching-and-provenance/">Caching and provenance</a></li>
      <li><a href="../guide/querying-provenance/">Querying provenance</a></li>
      <li><a href="../guide/maintenance/">Maintaining the database and cache</a></li>
      <li><a href="../guide/coding-agents/">Working with coding agents</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Examples</h3>
    <ul>
      <li><a href="../examples/bioinfo-workflow/">Bioinformatics workflow</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Plugins</h3>
    <ul>
      <li><a href="../guide/notebooks/">Notebook tasks</a></li>
      <li><a href="../guide/reports/">Reports</a></li>
      <li><a href="../guide/notifications/">Notifications</a></li>
      <li><a href="../guide/remote-execution/">Remote execution</a></li>
      <li><a href="../guide/remote-oke/">Running on OKE</a></li>
    </ul>
  </article>
  <article class="contents-block">
    <h3>Reference</h3>
    <ul>
      <li><a href="../faq/">FAQ</a></li>
      <li><a href="../reference/api/">API reference</a></li>
    </ul>
  </article>
</section>
```

```{toctree}
:hidden:
:caption: Getting started

getting-started/installation
getting-started/quickstart
motivation
```

```{toctree}
:hidden:
:caption: Writing workflows

guide/concepts
guide/tasks-and-flows
guide/outputs
guide/environments
guide/resources
```

```{toctree}
:hidden:
:caption: Running and inspecting

guide/cli
guide/caching-and-provenance
guide/querying-provenance
guide/maintenance
guide/coding-agents
```

```{toctree}
:hidden:
:caption: Examples

examples/bioinfo-workflow
```

```{toctree}
:hidden:
:caption: Plugins

guide/notebooks
guide/reports
guide/notifications
guide/remote-execution
guide/remote-oke
```

```{toctree}
:hidden:
:caption: Reference

faq
reference/api
```
