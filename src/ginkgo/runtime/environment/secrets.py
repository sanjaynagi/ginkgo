"""Runtime secret resolution and redaction support."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from ginkgo.core.secret import SecretRef
from ginkgo.core.types import rebuild_sequence
from ginkgo.errors import GinkgoError
from ginkgo.plugins.api import SecretBackendContext, SecretBackendType
from ginkgo.plugins.registry import plugin_registry


class SecretResolutionError(GinkgoError, RuntimeError):
    """Raised when a declared secret cannot be resolved."""

    def __init__(self, ref: SecretRef) -> None:
        self.ref = ref
        super().__init__(f"Unresolvable secret {ref.backend}:{ref.name}")


class SecretBackend(Protocol):
    """Protocol for concrete secret backends."""

    def resolve(self, *, ref: SecretRef) -> str | None:
        """Resolve *ref* to a concrete secret value."""


@dataclass(kw_only=True)
class EnvSecretBackend:
    """Resolve secrets from a process environment mapping."""

    environ: Mapping[str, str]

    def resolve(self, *, ref: SecretRef) -> str | None:
        """Return the environment value for *ref* if present."""
        return self.environ.get(ref.name)


@dataclass(kw_only=True)
class DotenvSecretBackend:
    """Resolve secrets from a parsed `.env` file."""

    values: Mapping[str, str]

    def resolve(self, *, ref: SecretRef) -> str | None:
        """Return the `.env` value for *ref* if present."""
        return self.values.get(ref.name)


@dataclass(kw_only=True)
class SecretResolver:
    """Resolve secret references through the backend chain each one names.

    A backend's chain is built from its plugin the first time a ref names it,
    so a run that resolves only ``env`` secrets never builds (or authenticates)
    any other backend.

    Parameters
    ----------
    context : SecretBackendContext
        Handed to each backend's ``build``.
    """

    context: SecretBackendContext
    _chains: dict[str, tuple[SecretBackend, ...]] = field(
        default_factory=dict, init=False, repr=False
    )

    def resolve(self, *, ref: SecretRef) -> str:
        """Resolve *ref* or raise ``SecretResolutionError``.

        Raises
        ------
        SecretResolutionError
            If no backend in the chain has the secret.
        PluginNotFoundError
            If no installed plugin provides the backend *ref* names.
        """
        for backend in self._chain(ref.backend):
            value = backend.resolve(ref=ref)
            if value is not None:
                return value
        raise SecretResolutionError(ref)

    def _chain(self, name: str) -> tuple[SecretBackend, ...]:
        """Return the backend chain for *name*, building it on first use."""
        chain = self._chains.get(name)
        if chain is None:
            chain = plugin_registry().get(SecretBackendType, name).build(self.context)
            self._chains[name] = chain
        return chain

    def can_resolve(self, *, ref: SecretRef) -> bool:
        """Return whether *ref* is resolvable."""
        try:
            self.resolve(ref=ref)
        except SecretResolutionError:
            return False
        return True

    def validate(self, *, refs: set[SecretRef]) -> list[SecretRef]:
        """Return secret refs that cannot be resolved."""
        return sorted((ref for ref in refs if not self.can_resolve(ref=ref)), key=_secret_sort_key)


def build_secret_resolver(
    *,
    project_root: Path,
    config: Mapping[str, Any] | None,
    environ: Mapping[str, str],
) -> SecretResolver:
    """Build a resolver over the secret backends installed plugins provide."""
    return SecretResolver(
        context=SecretBackendContext(
            project_root=project_root, config=config or {}, environ=environ
        )
    )


def build_env_backends(context: SecretBackendContext) -> tuple[SecretBackend, ...]:
    """Return the ``env`` backend chain: the process environment, then ``.env``.

    The ``.env`` file is consulted only when ``[secrets.dotenv]`` enables it.
    """
    config = context.config
    project_root = context.project_root
    environ = context.environ
    secrets_config = config.get("secrets", {}) if isinstance(config, Mapping) else {}
    dotenv_config = secrets_config.get("dotenv", {}) if isinstance(secrets_config, Mapping) else {}

    env_chain: list[SecretBackend] = [EnvSecretBackend(environ=environ)]
    if _dotenv_enabled(dotenv_config):
        dotenv_path = _dotenv_path(project_root=project_root, dotenv_config=dotenv_config)
        env_chain.append(DotenvSecretBackend(values=_load_dotenv(path=dotenv_path)))
    return tuple(env_chain)


def collect_secret_refs(value: Any) -> set[SecretRef]:
    """Collect all ``SecretRef`` values nested within *value*."""
    refs: set[SecretRef] = set()
    _collect_secret_refs(value=value, refs=refs)
    return refs


def contains_secret_refs(value: Any) -> bool:
    """Return whether *value* contains any ``SecretRef`` values."""
    return bool(collect_secret_refs(value))


def resolve_secret_refs(*, value: Any, resolver: SecretResolver) -> Any:
    """Resolve all secret references nested within *value*.

    A rewritten sequence keeps its own type, so a ``namedtuple`` is not turned
    into the builtin. Only an exact ``dict`` is rebuilt: a subclass such as a
    ``defaultdict`` is passed through whole, as the value codec carries it.
    """
    if isinstance(value, SecretRef):
        return resolver.resolve(ref=value)
    if isinstance(value, (list, tuple)):
        return rebuild_sequence(
            like=value,
            items=(resolve_secret_refs(value=item, resolver=resolver) for item in value),
        )
    if type(value) is dict:
        return {
            resolve_secret_refs(value=key, resolver=resolver): resolve_secret_refs(
                value=item, resolver=resolver
            )
            for key, item in value.items()
        }
    return value


def secret_identity(ref: SecretRef) -> dict[str, str]:
    """Return a stable serializable identity for a secret reference."""
    return {"backend": ref.backend, "name": ref.name, "type": "secret"}


def collect_resolved_secret_values(*, template: Any, resolved: Any) -> tuple[str, ...]:
    """Collect resolved values corresponding to secret references in *template*."""
    values: list[str] = []
    _collect_resolved_secret_values(template=template, resolved=resolved, values=values)
    unique = sorted({item for item in values if item}, key=len, reverse=True)
    return tuple(unique)


def redact_text(*, text: str, secret_values: tuple[str, ...]) -> str:
    """Replace resolved secret values with a redaction marker."""
    redacted = text
    for value in secret_values:
        redacted = redacted.replace(value, "[REDACTED]")
    return redacted


def redact_value(value: Any) -> Any:
    """Render a value safely for provenance and cache metadata."""
    if isinstance(value, SecretRef):
        return {
            "secret": secret_identity(value),
            "redacted": True,
        }
    # Exact containers only, as ``resolve_secret_refs`` rebuilds them: a
    # ``namedtuple`` or ``defaultdict`` keeps its type for the summary.
    if type(value) in (list, tuple):
        return [redact_value(item) for item in value]
    if type(value) is dict:
        return {str(redact_value(key)): redact_value(item) for key, item in value.items()}
    return value


def _collect_secret_refs(*, value: Any, refs: set[SecretRef]) -> None:
    if isinstance(value, SecretRef):
        refs.add(value)
        return
    if isinstance(value, list | tuple):
        for item in value:
            _collect_secret_refs(value=item, refs=refs)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _collect_secret_refs(value=key, refs=refs)
            _collect_secret_refs(value=item, refs=refs)


def _collect_resolved_secret_values(*, template: Any, resolved: Any, values: list[str]) -> None:
    if isinstance(template, SecretRef):
        values.append(str(resolved))
        return
    if isinstance(template, list | tuple) and isinstance(resolved, list | tuple):
        for template_item, resolved_item in zip(template, resolved, strict=False):
            _collect_resolved_secret_values(
                template=template_item,
                resolved=resolved_item,
                values=values,
            )
        return
    if isinstance(template, dict) and isinstance(resolved, dict):
        for key, item in template.items():
            if key in resolved:
                _collect_resolved_secret_values(
                    template=item,
                    resolved=resolved[key],
                    values=values,
                )


def _dotenv_enabled(dotenv_config: Any) -> bool:
    if isinstance(dotenv_config, bool):
        return dotenv_config
    if isinstance(dotenv_config, Mapping):
        return bool(dotenv_config.get("enabled", False))
    return False


def _dotenv_path(*, project_root: Path, dotenv_config: Any) -> Path:
    if isinstance(dotenv_config, Mapping):
        raw_path = dotenv_config.get("path", ".env")
    else:
        raw_path = ".env"
    return (project_root / str(raw_path)).resolve()


def _load_dotenv(*, path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _secret_sort_key(ref: SecretRef) -> tuple[str, str]:
    return (ref.backend, ref.name)
