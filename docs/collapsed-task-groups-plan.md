# Collapsed Task Groups with Multi-State Progress Bars

## Problem

When a workflow fans out a task across many samples (e.g. `align` mapped over 200 FASTQ files), the CLI currently renders one row per invocation. This produces a table that far exceeds terminal height, making it impossible to see the overall run at a glance.

## Proposed Solution

Collapse multiple invocations of the **same task definition** into a single row in the task table. Instead of a per-task status icon, render a **multi-segment progress bar** where each segment's colour represents a task state. The bar dynamically updates as invocations transition through their lifecycle.

### Visual Design

```
┌──────────────┬──────────────────────────────────────────────┬─────────────┬────────┐
│ Task         │ Status                                       │ Environment │ Time   │
├──────────────┼──────────────────────────────────────────────┼─────────────┼────────┤
│ prepare      │ ✓ succeeded                                  │ local       │ 1.2s   │
│ align (×200) │ ████████████████████░░░░░░░░░░░░░  67/200    │ local       │ 34s    │
│ merge        │ • waiting                                    │ local       │ --     │
└──────────────┴──────────────────────────────────────────────┴─────────────┴────────┘
```

The progress bar segments use the existing status colour palette:

| State     | Colour          | Existing Style     |
|-----------|----------------|--------------------|
| waiting   | yellow         | `yellow`           |
| staging   | magenta        | `bold magenta`     |
| running   | cyan           | `bold cyan`        |
| cached    | green (light)  | `bold green`       |
| succeeded | green          | `green`            |
| failed    | red            | `bold red`         |

Segment order (left-to-right): succeeded → cached → running → staging → waiting → failed.
This keeps "done" states on the left and "pending" on the right, with failures always visible at the trailing edge.

### Grouping Rules

- **Group key**: `task_name` (the fully-qualified task def name, e.g. `my_pipeline.align`).
- A group is created when **2 or more** invocations share the same `task_name`.
- A single invocation of a task renders exactly as it does today (icon + status text) — no progress bar.
- The label shows the base task name plus the count: `align (×200)`.
- The environment column shows the common environment if all invocations share one, otherwise `mixed`.
- The time column shows elapsed wall-clock time from the earliest `started_at` to `now` (or latest `finished_at` if all are terminal).

### Data Model Changes

New model in `renderers/models.py`:

```python
@dataclass
class _TaskGroup:
    """Render state for a collapsed group of same-task invocations."""
    task_name: str
    label: str
    env_label: str
    rows: list[_TaskRow]  # individual invocations

    def status_counts(self) -> Counter[str]: ...
    def is_terminal(self) -> bool: ...
    def elapsed(self, *, now: float) -> float | None: ...
```

### Renderer Changes (`renderers/run.py`)

1. **Grouping logic**: After `_row_order` is populated, build a `list[_TaskGroup | _TaskRow]` for display. A `_TaskGroup` is created when multiple `_TaskRow`s share the same `task_name`. Single-invocation tasks remain as `_TaskRow`.

2. **`_render_task_table`**: Iterate over the grouped display list. For `_TaskRow`, render as today. For `_TaskGroup`, render the multi-segment bar in the Status column.

3. **Multi-segment bar**: A custom Rich renderable (`_MultiStateBar`) that takes status counts and a total width, and renders a sequence of coloured `█` characters proportional to each state's count.

4. **Progress text**: Append a compact summary after the bar: `67/200` (terminal count / total).

### New Renderable: `_MultiStateBar`

```python
class _MultiStateBar:
    """A Rich renderable that draws a segmented bar coloured by task state."""

    def __init__(self, *, counts: Counter[str], total: int, width: int) -> None: ...

    def __rich_console__(self, console, options):
        # Build a Text object with segments of block characters,
        # each segment styled with the corresponding _status_style().
        ...
```

Segment widths are computed proportionally (`round(count / total * width)`), with a minimum of 1 char for any non-zero count and a final adjustment pass to ensure widths sum to exactly `width`.

### Interaction with Dynamic Task Registration

Tasks registered dynamically (via `GraphNodeRegistered` events mid-run) are added to existing groups if the `task_name` matches, or create a new row/group. The group's `(×N)` count updates live.

### Failure Handling

- If any invocation in a group fails, the failed segment appears in red at the right edge of the bar.
- End-of-run failure details (`_render_failure_details`) continue to list each failed invocation individually — collapsing is purely a live-display concern.

## Scope

### In scope
- Grouping logic and `_TaskGroup` model
- `_MultiStateBar` renderable
- Updated `_render_task_table` to handle groups
- Updated progress/summary counts

### Out of scope (future)
- Expandable/collapsible groups (drill-down into individual invocations)
- Filtering or searching within groups
- Verbose mode showing all rows even for groups

## Risks & Tradeoffs

| Risk | Mitigation |
|------|-----------|
| Bar too narrow on small terminals | Minimum width of 16 chars; fall back to text-only summary (`67/200 ✓120 ↺30 ◐17`) below threshold |
| Proportional rounding hides small counts | Minimum 1-char segment for any non-zero state ensures visibility |
| Mixed environments in a group | Show `mixed` and accept the loss of per-invocation detail in collapsed view |
| Dynamic task registration changes group size mid-render | Group count updates live; bar re-proportions on each refresh |

## Success Criteria

1. A workflow with 200 invocations of the same task renders as a single row with a multi-state progress bar.
2. The bar correctly reflects the proportion of invocations in each state and updates live at 12 Hz.
3. Single-invocation tasks render identically to today (no visual regression).
4. Failure details at end-of-run still list individual failed invocations.
5. Terminal widths down to 80 columns produce a readable (if compact) display.
