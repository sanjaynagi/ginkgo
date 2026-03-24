# Example: Notebook Task

The notebook guide uses the retail analytics example because that is where the
repository currently demonstrates a first-class notebook task.

Relevant sources:

- `examples/retail/retail_analytics/modules/reporting.py`
- `examples/retail/retail_analytics/workflow.py`

## The Notebook Definition

```python
@notebook(path="../notebooks/channel_performance.ipynb")
def render_channel_performance_notebook(
    *,
    channel_metrics: file,
    report_title: str,
) -> file:
    """Render a notebook view of channel performance trends."""
```

This keeps the notebook under workflow control while still letting the notebook
own the final presentation of the report.

## How It Fits Into The Workflow

The retail flow produces structured CSV outputs first, then passes one of those
artifacts into the notebook task:

```python
channel_metrics = write_channel_metrics(enriched_orders=enriched_orders)
channel_notebook = render_channel_performance_notebook(
    channel_metrics=channel_metrics,
    report_title="Retail Channel Performance Notebook",
)
```

That pattern is a good one to copy:

- produce stable intermediate data first
- use the notebook task to render a report or exploratory artifact
- treat the notebook output as a tracked workflow result

## Why This Matters

Notebook support works best when notebooks are part of a larger reproducible
pipeline rather than disconnected side files. Ginkgo's notebook tasks preserve
that connection.
