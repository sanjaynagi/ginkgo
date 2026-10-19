"""ginkgo-report: the ``ginkgo report`` command and its static HTML report."""

from __future__ import annotations

from ginkgo.plugins.api import CliCommand, Plugin

from ginkgo_report.command import add_report_arguments, command_report

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-report",
    contributions=(
        CliCommand(
            name="report",
            help="Generate HTML run reports",
            add_arguments=add_report_arguments,
            run=command_report,
        ),
    ),
)
