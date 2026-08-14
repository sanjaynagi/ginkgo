"""Declarative task resource requirements.

``Resources`` states what a task needs — CPU threads, memory, GPUs — and
never implies *where* the task runs. Placement is decided by the evaluator
from the requirement and the capabilities available (local budgets and any
configured remote executor).
"""

from __future__ import annotations

import fnmatch
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass, field, fields

_MEMORY_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*(Gi|Mi|G|M|Ti|Ki)$")

_MEMORY_MULTIPLIERS: dict[str, float] = {
    "Ki": 1 / (1024 * 1024),
    "Mi": 1 / 1024,
    "Gi": 1.0,
    "Ti": 1024.0,
    "M": 1_000_000 / (1024**3),
    "G": 1_000_000_000 / (1024**3),
}


def parse_memory(value: str | None) -> int:
    """Parse a Kubernetes-style memory string to whole GiB.

    Parameters
    ----------
    value : str | None
        Memory specification (e.g. ``"4Gi"``, ``"512Mi"``).

    Returns
    -------
    int
        Memory in GiB, rounded up. Returns 0 when *value* is ``None``.

    Raises
    ------
    ValueError
        If the string cannot be parsed.
    """
    if value is None:
        return 0

    match = _MEMORY_PATTERN.match(value.strip())
    if match is None:
        raise ValueError(
            f"Invalid memory specification {value!r}. "
            "Use Kubernetes resource notation, e.g. '4Gi', '512Mi', '8G'."
        )

    amount = float(match.group(1))
    unit = match.group(2)
    gib = amount * _MEMORY_MULTIPLIERS[unit]
    return max(1, math.ceil(gib)) if gib > 0 else 0


@dataclass(frozen=True)
class Resources:
    """Resource requirements declared by a task.

    Parameters
    ----------
    threads : int
        CPU footprint for the scheduler. Reserved against the ``--cores``
        budget wherever the task runs.
    memory : str | None
        Memory footprint in Kubernetes resource notation (e.g. ``"4Gi"``,
        ``"512Mi"``). Reserved against ``--memory`` locally and mapped to
        resource requests by remote executors.
    gpu : int
        Number of GPUs the task requires. Reserved against the ``--gpus``
        budget locally, or mapped to accelerator requests by remote
        executors.
    gpu_type : str | None
        Accelerator type for remote execution (e.g. ``"nvidia-tesla-t4"``).
        Overrides any executor-level default. Only meaningful together with
        ``gpu > 0``.
    memory_retry_multiplier : float
        Factor applied to ``memory`` on each retry, for tasks whose first
        attempt may run out of memory. Attempt *k* (0-indexed) is scheduled
        with ``memory_gb * memory_retry_multiplier ** k``, capped at the
        run's ``--memory`` budget. ``1.0`` (default) disables escalation.
        Requires ``memory`` to be set.
    custom : dict[str, int]
        User-defined resource demands (e.g. ``{"api_calls": 2}``), scheduled
        against run-level budgets from ``[resources.budgets]`` config or
        repeated ``--resource name=value`` flags. A dimension no budget
        names is unconstrained. Unlike the built-in dimensions, custom
        demands also count for remote-placed tasks — budgets such as API
        quotas or database connections apply wherever the task runs.
    """

    threads: int = 1
    memory: str | None = None
    gpu: int = 0
    gpu_type: str | None = None
    memory_retry_multiplier: float = 1.0
    custom: dict[str, int] = field(default_factory=dict)
    _memory_gb: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.threads < 1:
            raise ValueError(f"threads must be at least 1, got {self.threads}")
        for name, demand in self.custom.items():
            if name in _RESERVED_DIMENSIONS:
                raise ValueError(
                    f"custom resource {name!r} collides with a built-in resource field; "
                    f"declare it with the {name!r} argument instead"
                )
            if isinstance(demand, bool) or not isinstance(demand, int) or demand < 1:
                raise ValueError(
                    f"custom resource {name!r} must be a positive integer, got {demand!r}"
                )
        if self.gpu < 0:
            raise ValueError(f"gpu must be at least 0, got {self.gpu}")
        if self.gpu_type is not None and self.gpu == 0:
            raise ValueError("gpu_type requires gpu > 0")
        if self.memory_retry_multiplier < 1:
            raise ValueError(
                f"memory_retry_multiplier must be at least 1, got {self.memory_retry_multiplier}"
            )
        if self.memory_retry_multiplier > 1 and self.memory is None:
            raise ValueError("memory_retry_multiplier requires memory to be set")
        object.__setattr__(self, "_memory_gb", parse_memory(self.memory))

    @property
    def memory_gb(self) -> int:
        """Parsed memory footprint in whole GiB (0 when unset)."""
        return self._memory_gb

    def memory_gb_for_attempt(self, attempt: int) -> int:
        """Memory footprint in GiB for a 0-indexed retry attempt.

        Applies ``memory_retry_multiplier`` exponentially so OOM-prone tasks
        can retry with more memory (attempt 0 is the first execution).
        """
        if attempt <= 0 or self.memory_retry_multiplier == 1.0:
            return self.memory_gb
        return math.ceil(self.memory_gb * self.memory_retry_multiplier**attempt)


# The overridable fields are exactly the declared (init) fields of Resources.
_OVERRIDE_KEYS = frozenset(f.name for f in fields(Resources) if f.init)

# Built-in dimension names a custom resource must not shadow.
_RESERVED_DIMENSIONS = _OVERRIDE_KEYS - {"custom"}

# Run options that budget the built-in dimensions; the rest have no
# run-level budget at all.
_BUILTIN_BUDGET_OPTIONS = {"threads": "--cores", "memory": "--memory", "gpu": "--gpus"}


def _builtin_dimension_hint(name: str) -> str:
    option = _BUILTIN_BUDGET_OPTIONS.get(name)
    if option is not None:
        return f"set its budget with the dedicated {option} run option instead"
    return "it has no run-level budget"


def parse_resource_budget_args(args: Iterable[str]) -> dict[str, int]:
    """Parse repeated ``--resource name=value`` flags into budget mappings.

    Raises
    ------
    ValueError
        If an entry is not ``name=value`` with a positive integer value, or
        names a built-in resource dimension.
    """
    budgets: dict[str, int] = {}
    for arg in args:
        name, separator, raw_value = arg.partition("=")
        name = name.strip()
        if not separator or not name:
            raise ValueError(
                f"invalid --resource {arg!r}: expected name=value, e.g. --resource api_calls=10"
            )
        if name in _RESERVED_DIMENSIONS:
            raise ValueError(
                f"--resource {name!r} is a built-in dimension; {_builtin_dimension_hint(name)}"
            )
        try:
            value = int(raw_value)
        except ValueError:
            value = -1
        if value < 1:
            raise ValueError(f"invalid --resource {arg!r}: budget must be a positive integer")
        budgets[name] = value
    return budgets


def resource_budgets_from_config(config: dict[str, object] | None) -> dict[str, int]:
    """Parse the ``[resources.budgets]`` table of the runtime config.

    Raises
    ------
    ValueError
        If the table is malformed, a budget is not a positive integer, or a
        dimension shadows a built-in resource field.
    """
    if config is not None and not isinstance(config, dict):
        raise ValueError("[resources] must be a table")
    budgets = (config or {}).get("budgets", {})
    if not isinstance(budgets, dict):
        raise ValueError("[resources.budgets] must be a table of dimension budgets")
    parsed: dict[str, int] = {}
    for name, value in budgets.items():
        if not isinstance(name, str):
            raise ValueError(f"[resources.budgets] dimension names must be strings, got {name!r}")
        if name in _RESERVED_DIMENSIONS:
            raise ValueError(
                f"[resources.budgets] {name!r} is a built-in dimension; "
                f"{_builtin_dimension_hint(name)}"
            )
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(
                f"[resources.budgets] {name!r} must be a positive integer, got {value!r}"
            )
        parsed[name] = value
    return parsed


@dataclass(frozen=True)
class ResourceOverrides:
    """Site-level resource overrides keyed by task-name selector.

    Parsed from the ``[resources.overrides]`` runtime-config table. A
    selector matches a task by its fully qualified name (``module.func``),
    its short name (``func``), or an ``fnmatch`` glob over either. Exact
    matches beat glob matches; among globs, the first selector in config
    order wins. Override keys are merged over the task's declared
    :class:`Resources`; keys the override omits keep their declared values.
    """

    selectors: tuple[tuple[str, dict[str, object]], ...] = ()

    @classmethod
    def from_config(cls, config: dict[str, object] | None) -> ResourceOverrides:
        """Parse the ``resources`` table of the runtime config.

        Raises
        ------
        ValueError
            If a selector's overrides are not a table or contain keys that
            are not resource fields.
        """
        if config is not None and not isinstance(config, dict):
            raise ValueError("[resources] must be a table")
        overrides = (config or {}).get("overrides", {})
        if not isinstance(overrides, dict):
            raise ValueError("[resources.overrides] must be a table of task selectors")
        selectors: list[tuple[str, dict[str, object]]] = []
        for selector, values in overrides.items():
            if not isinstance(values, dict):
                raise ValueError(
                    f"[resources.overrides.{selector!r}] must be a table of resource fields"
                )
            unknown = set(values) - _OVERRIDE_KEYS
            if unknown:
                supported = ", ".join(sorted(_OVERRIDE_KEYS))
                raise ValueError(
                    f"[resources.overrides.{selector!r}] has unknown keys "
                    f"{sorted(unknown)}; supported keys are {{{supported}}}"
                )
            selectors.append((selector, dict(values)))
        return cls(selectors=tuple(selectors))

    def apply(self, *, task_name: str, base: Resources) -> Resources:
        """Return *base* with the best-matching override merged over it.

        Validation of the merged values happens in ``Resources.__post_init__``,
        so a config override that produces an invalid combination fails with
        the same message a decorator declaration would.
        """
        values = self._match(task_name=task_name)
        if values is None:
            return base
        merged: dict[str, object] = {key: getattr(base, key) for key in _OVERRIDE_KEYS}
        merged.update(values)
        try:
            return Resources(**merged)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid resource override for {task_name}: {exc}") from exc

    def _match(self, *, task_name: str) -> dict[str, object] | None:
        short_name = task_name.rsplit(".", 1)[-1]
        glob_match: dict[str, object] | None = None
        for selector, values in self.selectors:
            if selector == task_name or selector == short_name:
                return values
            if glob_match is None and (
                fnmatch.fnmatchcase(task_name, selector)
                or fnmatch.fnmatchcase(short_name, selector)
            ):
                glob_match = values
        return glob_match
