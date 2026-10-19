"""ginkgo-slack: posts run notifications to a Slack incoming webhook."""

from __future__ import annotations

from ginkgo.plugins.api import EventSubscriberType, Plugin
from ginkgo_slack.subscriber import build_slack_subscriber

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-slack",
    contributions=(EventSubscriberType(name="slack", build=build_slack_subscriber),),
)
