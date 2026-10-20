"""Pixi environment registry and subprocess helpers.

Environments are resolved from ``envs/<env_name>/`` under the project root, or
from an explicit path to a ``pixi.toml``. A named environment directory may
instead carry a ``pyproject.toml`` with a ``[tool.pixi]`` section, which Pixi
accepts as a manifest natively. Explicit conda environment specs
(``environment.yml`` / ``environment.yaml``) are imported into a generated
neighboring Pixi workspace under ``.ginkgo-pixi/`` and then executed through
the normal Pixi path.
"""

from __future__ import annotations

import subprocess
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Sequence

from ginkgo.core.hashing import hash_file
from ginkgo.core.shell import shell_argv
from ginkgo.envs.manifest import find_pixi_manifest
from ginkgo.errors import FailureKind, GinkgoError

if TYPE_CHECKING:
    from ginkgo.envs.mounts import Mount
    from ginkgo.plugins.api import EnvironmentContext


class PixiEnvNotFoundError(GinkgoError, RuntimeError):
    """Raised when a declared environment cannot be located.

    Parameters
    ----------
    env : str
        The environment name or path that was not found.
    searched : Path | tuple[Path, ...] | None
        The directory (or directories, in discovery order) that were
        searched for a named env. A bare ``Path`` is accepted for backwards
        compatibility and treated as a single-directory search.
    """

    failure_kind: ClassVar[FailureKind] = "env_mismatch"
    diagnostic_code: ClassVar[str] = "missing_env"
    diagnostic_suggestion: ClassVar[str] = (
        "Create the environment manifest, or correct the env= name on the task."
    )

    def __init__(self, *, env: str, searched: Path | tuple[Path, ...] | None = None) -> None:
        if searched is not None:
            searched_dirs = (searched,) if isinstance(searched, Path) else tuple(searched)
            expected = "\n".join(
                f"  - {envs_dir / env / 'pixi.toml'} (available: {list_envs(envs_dir)})"
                for envs_dir in searched_dirs
            )
            msg = f"Pixi environment {env!r} not found. Looked for a pixi.toml at:\n{expected}"
        else:
            msg = f"Pixi environment path {env!r} does not point to a pixi.toml file."
        super().__init__(msg)


class PixiEnvImportError(GinkgoError, RuntimeError):
    """Raised when a conda environment spec cannot be imported into Pixi."""

    failure_kind: ClassVar[FailureKind] = "env_mismatch"

    def __init__(self, *, source: Path, output: str) -> None:
        details = output.strip() or "pixi did not provide any error output"
        super().__init__(f"Failed to import conda env spec {str(source)!r} into Pixi: {details}")


class PixiEnvPrepareError(GinkgoError, RuntimeError):
    """Raised when a Pixi environment cannot be materialized."""

    failure_kind: ClassVar[FailureKind] = "env_mismatch"

    def __init__(self, *, manifest: Path, output: str) -> None:
        details = output.strip() or "pixi did not provide any error output"
        super().__init__(f"Failed to prepare Pixi env {str(manifest)!r}: {details}")


def list_envs(envs_dir: Path) -> list[str]:
    """Return sorted names of discoverable environments under envs_dir."""
    if not envs_dir.is_dir():
        return []
    return sorted(
        child.name
        for child in envs_dir.iterdir()
        if child.is_dir() and find_pixi_manifest(child) is not None
    )


def _is_conda_env_file(path: Path) -> bool:
    """Return whether *path* names a supported conda env spec file."""
    return path.name in {"environment.yml", "environment.yaml"}


def _is_explicit_path(env: str) -> bool:
    """Return True when env looks like a filesystem path rather than a name."""
    return "/" in env or env.startswith(".")


@dataclass(kw_only=True)
class PixiRegistry:
    """Locates Pixi environments and builds subprocess arguments for them.

    Parameters
    ----------
    project_root : Path
        Root of the workflow project. Environments are discovered under
        ``project_root/envs/`` and, when provided, ``workflow_root/envs/``.
        Defaults to the current working directory.
    workflow_root : Path | None
        Directory containing the resolved workflow entrypoint. Canonical
        package-local envs are discovered from ``workflow_root/envs/``.

    Raises
    ------
    RuntimeError
        If ``pixi`` is not found on PATH when the registry is first used.
    """

    project_root: Path = field(default_factory=Path.cwd)
    workflow_root: Path | None = None
    _envs_dirs: tuple[Path, ...] = field(init=False, repr=False)
    _lock_cache: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _identity_cache: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _prepared_manifests: set[Path] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        env_dirs: list[Path] = []
        if self.workflow_root is not None:
            workflow_envs = self.workflow_root / "envs"
            env_dirs.append(workflow_envs)
        env_dirs.append(self.project_root / "envs")

        unique_dirs: list[Path] = []
        seen: set[Path] = set()
        for env_dir in env_dirs:
            resolved = env_dir.resolve(strict=False)
            if resolved in seen:
                continue
            seen.add(resolved)
            unique_dirs.append(resolved)

        object.__setattr__(self, "_envs_dirs", tuple(unique_dirs))

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, *, env: str) -> Path:
        """Return the absolute path to the ``pixi.toml`` for *env*.

        Parameters
        ----------
        env : str
            Environment name (resolved from ``envs/<name>/``, where the
            manifest may be ``pixi.toml`` or a ``pyproject.toml`` with a
            ``[tool.pixi]`` section) or an explicit path to a manifest file.

        Returns
        -------
        Path
            Absolute path to the resolved manifest.

        Raises
        ------
        PixiEnvNotFoundError
            If the environment cannot be located.
        """
        if _is_explicit_path(env):
            manifest = Path(env)
            if _is_conda_env_file(manifest):
                return self._resolve_conda_env_file(manifest=manifest)
            if not manifest.is_file():
                raise PixiEnvNotFoundError(env=env)
            return manifest.resolve()

        for envs_dir in self._envs_dirs:
            manifest = find_pixi_manifest(envs_dir / env)
            if manifest is not None:
                return manifest.resolve()

        raise PixiEnvNotFoundError(env=env, searched=self._envs_dirs or None)

    @property
    def env_directories(self) -> tuple[Path, ...]:
        """Return the environment discovery roots for this project."""
        return self._envs_dirs

    def _resolve_conda_env_file(self, *, manifest: Path) -> Path:
        """Import a conda env spec into a generated neighboring Pixi workspace."""
        if not manifest.is_file():
            raise PixiEnvNotFoundError(env=str(manifest))

        _require_pixi()
        generated_dir = manifest.parent / ".ginkgo-pixi"
        generated_manifest = generated_dir / "pixi.toml"
        if self._should_refresh_generated_manifest(
            source_manifest=manifest,
            generated_manifest=generated_manifest,
        ):
            self._import_conda_env_file(source_manifest=manifest, output_dir=generated_dir)
        return generated_manifest.resolve()

    def _should_refresh_generated_manifest(
        self,
        *,
        source_manifest: Path,
        generated_manifest: Path,
    ) -> bool:
        """Return whether the generated Pixi workspace should be recreated."""
        if not generated_manifest.is_file():
            return True
        return source_manifest.stat().st_mtime > generated_manifest.stat().st_mtime

    def _import_conda_env_file(self, *, source_manifest: Path, output_dir: Path) -> None:
        """Run ``pixi init --import`` into *output_dir* for a conda env spec.

        ``pixi init`` refuses a directory that already holds a ``pixi.toml``,
        so the one generated from an earlier version of the spec is removed
        first. The lock and the installed environment are kept for pixi to
        reconcile.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "pixi.toml").unlink(missing_ok=True)
        argv = [
            "pixi",
            "init",
            str(output_dir),
            "--import",
            str(source_manifest),
        ]
        completed = subprocess.run(
            argv,
            shell=False,
            check=False,
            text=True,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise PixiEnvImportError(
                source=source_manifest,
                output=(completed.stdout or "") + (completed.stderr or ""),
            )

        generated_manifest = output_dir / "pixi.toml"
        if not generated_manifest.is_file():
            raise PixiEnvImportError(
                source=source_manifest,
                output="pixi import completed without creating pixi.toml",
            )

    def env_identity(self, *, env: str) -> str:
        """Return a digest of the environment's declaration.

        The identity is a function of the manifest alone, so it reads the same
        before and after :meth:`prepare` installs the environment. It has to:
        cache keys fold it in, and `pixi install` writes the neighbouring
        ``pixi.lock``, so an identity taken from the lock file moved between the
        first and second run of every workflow whose environments were not yet
        installed (issue #194).

        The manifest is also the portable half of the declaration. A lock file
        is solved per machine and per moment, so hashing it made cache entries
        machine-local; hashing the manifest keys on what the workflow asks for.

        Parameters
        ----------
        env : str
            Environment name or path.

        Returns
        -------
        str
            Hex digest of the resolved manifest.
        """
        cached = self._identity_cache.get(env)
        if cached is not None:
            return cached

        digest = hash_file(self.resolve(env=env))
        self._identity_cache[env] = digest
        return digest

    def lock_hash(self, *, env: str) -> str | None:
        """Return the BLAKE3 digest of the environment's ``pixi.lock``, or None.

        This reports what is installed on *this* machine, which only exists once
        :meth:`prepare` has run, and is recorded for provenance. Cache keys use
        :meth:`env_identity` instead.

        The hash is computed once per env name and cached in memory. A missing
        lock file is not remembered: ``prepare`` writes one, and a caller that
        asked before the install has to see the digest afterwards.

        Parameters
        ----------
        env : str
            Environment name or path.

        Returns
        -------
        str | None
            Hex digest, or ``None`` if no lockfile exists alongside the
            ``pixi.toml``.
        """
        cached = self._lock_cache.get(env)
        if cached is not None:
            return cached

        manifest = self.resolve(env=env)
        lock_path = manifest.parent / "pixi.lock"
        if not lock_path.is_file():
            return None

        digest = hash_file(lock_path)
        self._lock_cache[env] = digest
        return digest

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Raise for any environment name that cannot be resolved.

        Env path resolution is checked first so that a missing environment
        raises ``PixiEnvNotFoundError`` regardless of whether pixi itself is
        installed.  The pixi availability check follows once all paths are
        confirmed valid.

        Parameters
        ----------
        env_names : set[str]
            Collection of ``env`` values declared across all registered tasks.

        Raises
        ------
        PixiEnvNotFoundError
            On the first environment that cannot be located.
        RuntimeError
            If ``pixi`` is not found on PATH.
        """
        # Resolve all env paths first — a missing env is a clearer error than
        # "pixi not installed".
        for env in sorted(env_names):
            self.resolve(env=env)

        # Only check pixi availability after confirming all declared envs exist.
        _require_pixi()

    def prepare(self, *, env: str) -> Path:
        """Materialize the Pixi environment for *env* once per registry instance.

        Parameters
        ----------
        env : str
            Environment name or path.

        Returns
        -------
        Path
            Absolute path to the resolved ``pixi.toml``.

        Raises
        ------
        PixiEnvPrepareError
            If Pixi fails to install or update the environment.
        RuntimeError
            If ``pixi`` is not found on PATH.
        """
        manifest = self.resolve(env=env)
        if manifest in self._prepared_manifests:
            return manifest

        _require_pixi()
        argv = [
            "pixi",
            "install",
            "--manifest-path",
            str(manifest),
        ]
        completed = subprocess.run(
            argv,
            shell=False,
            check=False,
            text=True,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise PixiEnvPrepareError(
                manifest=manifest,
                output=(completed.stdout or "") + (completed.stderr or ""),
            )

        self._prepared_manifests.add(manifest)
        return manifest

    # ------------------------------------------------------------------
    # Subprocess argument builders
    # ------------------------------------------------------------------

    def exec_argv(self, *, env: str, cmd: str) -> list[str]:
        """Build argv to run *cmd* inside the Pixi environment.

        The command string is passed verbatim to strict ``bash -c`` (see
        :func:`~ginkgo.core.shell.shell_argv`), which handles shell quoting,
        redirection, and pipes exactly as the user wrote them.

        Parameters
        ----------
        env : str
            Environment name or path.
        cmd : str
            Shell command string (already interpolated by the task body).

        Returns
        -------
        list[str]
            Argument vector suitable for ``subprocess.run(..., shell=False)``.
        """
        manifest = self.resolve(env=env)
        return [
            "pixi",
            "run",
            "--manifest-path",
            str(manifest),
            "--",
            *shell_argv(shell="bash", cmd=cmd),
        ]


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _require_pixi() -> None:
    """Raise RuntimeError when pixi is not found on PATH."""
    if shutil.which("pixi") is None:
        raise RuntimeError(
            "pixi is not installed or not found on PATH. "
            "Install pixi from https://pixi.sh before running ginkgo workflows "
            "with environment-isolated tasks."
        )


@dataclass(kw_only=True)
class PixiEnvironment:
    """Execution environment for named Pixi environments.

    Parameters
    ----------
    pixi_registry : PixiRegistry
        The registry that discovers and materialises environments.
    """

    pixi_registry: PixiRegistry

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Raise for any environment name that cannot be resolved."""
        self.pixi_registry.validate_envs(env_names=env_names)

    def prepare(self, *, env: str) -> None:
        """Materialize the Pixi environment."""
        self.pixi_registry.prepare(env=env)

    def env_identity(self, *, env: str) -> str:
        """Return the BLAKE3 digest of the Pixi manifest."""
        return self.pixi_registry.env_identity(env=env)

    def materialized_digest(self, *, env: str) -> str | None:
        """Return the BLAKE3 digest of the installed environment's lock file."""
        return self.pixi_registry.lock_hash(env=env)

    def exec_argv(
        self,
        *,
        env: str,
        cmd: str,
        mounts: Sequence[Mount] = (),
        env_vars: Sequence[str] = (),
    ) -> list[str]:
        """Build argv to run *cmd* through the Pixi environment.

        Pixi runs on the host, where every declared path is already reachable
        and the subprocess environment is inherited, so *mounts* and *env_vars*
        are accepted for protocol conformance and ignored.
        """
        return self.pixi_registry.exec_argv(env=env, cmd=cmd)

    def exec_failure_hint(self, *, env: str, exit_code: int, output: str) -> str | None:
        """Pixi failures surface the tool's own error; nothing to add."""
        return None

    def env_lock_path(self, *, env: str) -> Path | None:
        """Return the path to the Pixi lock file for provenance capture."""
        manifest = self.pixi_registry.resolve(env=env)
        lock_path = manifest.parent / "pixi.lock"
        return lock_path if lock_path.is_file() else None

    def provenance_fields(self, *, env: str) -> dict[str, Any]:
        """Record the backend; the lock file is copied separately."""
        return {"backend": "pixi"}


def build_pixi_environment(context: EnvironmentContext) -> PixiEnvironment:
    """Build the Pixi environment for a run, searching its workflow's ``envs/``."""
    return PixiEnvironment(
        pixi_registry=PixiRegistry(
            project_root=context.project_root,
            workflow_root=context.workflow_root,
        )
    )
