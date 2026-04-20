"""Static HTML report export for completed Ginkgo runs.

Public entry points:

- :func:`build_report_data` — build the typed :class:`ReportData` view from
  a run directory.
- :func:`export_report` — write a bundle directory (or a single HTML file)
  to disk.
"""

from __future__ import annotations

from .model import ReportData, build_report_data
from .render import export_report
from .sizing import SizingPolicy

__all__ = ["ReportData", "SizingPolicy", "build_report_data", "export_report"]
