"""ginkgo-pixi: Pixi environments for tasks' bare ``env=`` names, and ``ginkgo env``."""

from __future__ import annotations

from ginkgo.plugins.api import CliCommand, EnvironmentType, Plugin
from ginkgo_pixi.command import add_env_arguments, command_env
from ginkgo_pixi.environment import build_pixi_environment

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-pixi",
    contributions=(
        EnvironmentType(name="pixi", schemes=("",), label="pixi", build=build_pixi_environment),
        CliCommand(
            name="env",
            help="Manage task execution environments",
            add_arguments=add_env_arguments,
            run=command_env,
        ),
    ),
)
