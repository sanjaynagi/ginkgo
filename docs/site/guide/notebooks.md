# Notebook Tasks

Notebook tasks let you promote a notebook from an ad hoc analysis artifact into
a first-class workflow step with typed inputs, outputs, provenance, and caching.

## The Authoring Model

Use `@notebook(...)` to bind a notebook file into the workflow graph.

```python
from ginkgo import file, notebook


@notebook(path="../notebooks/channel_performance.ipynb")
def render_channel_performance_notebook(
    channel_metrics: file,
    report_title: str,
) -> file:
    """Render a notebook view of channel performance trends."""
```

The decorated function defines the parameter schema and the output contract. The
notebook file itself is treated as source material for execution and cache
identity.

## What Ginkgo Records

Notebook tasks currently support:

- `.ipynb` execution through Papermill
- marimo notebook execution
- stable run-scoped notebook artifacts
- HTML export recorded in provenance
- cache invalidation when the notebook source changes

## Example: Retail Notebook Task

The current repository's notebook example lives in
`examples/retail/retail_analytics/modules/reporting.py` and is wired into the
retail workflow entrypoint.

That example is intentionally separate from the canonical bioinformatics
walkthrough. The main docs narrative stays centered on the bioinfo workflow, but
the notebook example shows how analysis-friendly outputs can be turned into
tracked workflow artifacts.

## When To Use A Notebook Task

Notebook tasks are a good fit when you need:

- a report-oriented artifact for people to review
- stable HTML output as part of a run
- typed workflow inputs into a notebook
- caching and provenance for notebook execution

They are a weaker fit when the notebook is only temporary scratch work and does
not belong in the reproducible workflow contract.
