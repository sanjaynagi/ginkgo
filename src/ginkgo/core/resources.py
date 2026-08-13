"""Declarative task resource requirements.

``Resources`` states what a task needs — CPU threads, memory, GPUs — and
never implies *where* the task runs. Placement is decided by the evaluator
from the requirement and the capabilities available (local budgets and any
configured remote executor).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

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
    """

    threads: int = 1
    memory: str | None = None
    gpu: int = 0
    gpu_type: str | None = None
    _memory_gb: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.threads < 1:
            raise ValueError(f"threads must be at least 1, got {self.threads}")
        if self.gpu < 0:
            raise ValueError(f"gpu must be at least 0, got {self.gpu}")
        if self.gpu_type is not None and self.gpu == 0:
            raise ValueError("gpu_type requires gpu > 0")
        object.__setattr__(self, "_memory_gb", parse_memory(self.memory))

    @property
    def memory_gb(self) -> int:
        """Parsed memory footprint in whole GiB (0 when unset)."""
        return self._memory_gb
