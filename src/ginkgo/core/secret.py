"""Secret reference helpers for workflow construction."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class SecretRef:
    """Reference to a runtime-resolved secret value.

    Parameters
    ----------
    name : str
        Logical secret name or path.
    backend : str
        Resolver backend identifier.
    """

    name: str
    backend: str = "env"

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("secret name must not be empty")
        if not self.backend:
            raise ValueError("secret backend must not be empty")

    def __str__(self) -> str:
        """Return a safe placeholder string for display contexts."""
        return f"<secret:{self.backend}:{self.name}>"


def secret(name: str, *, backend: str = "env") -> SecretRef:
    """Return a runtime secret reference.

    Parameters
    ----------
    name : str
        Logical secret name or path.
    backend : str, default="env"
        Resolver backend identifier.

    Returns
    -------
    SecretRef
        Deferred secret reference to resolve at execution time.
    """

    return SecretRef(name=name, backend=backend)
