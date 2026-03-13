"""Pixi environment registry and subprocess helpers.

Environments are resolved from ``envs/<env_name>/`` under the project root, or
from an explicit path to a ``pixi.toml``. Explicit conda environment specs
(``environment.yml`` / ``environment.yaml``) are imported into a generated
neighboring Pixi workspace under ``.ginkgo-pixi/`` and then executed through
the normal Pixi path.
"""

from __future__ import annotations

import hashlib
import subprocess
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence


class PixiEnvNotFoundError(RuntimeError):
    """Raised when a declared environment cannot be located.

    Parameters
    ----------
    env : str
        The environment name or path that was not found.
    searched : Path
        The directory that was searched (for named envs).
    """

    def __init__(self, *, env: str, searched: Path | None = None) -> None:
        if searched is not None:
            msg = (
                f"Pixi environment {env!r} not found. "
                f"Expected a pixi.toml at {searched / env / 'pixi.toml'}. "
                f"Available environments: {_list_envs(searched)}"
            )
        else:
            msg = f"Pixi environment path {env!r} does not point to a pixi.toml file."
        super().__init__(msg)


class PixiEnvImportError(RuntimeError):
    """Raised when a conda environment spec cannot be imported into Pixi."""

    def __init__(self, *, source: Path, output: str) -> None:
        details = output.strip() or "pixi did not provide any error output"
        super().__init__(f"Failed to import conda env spec {str(source)!r} into Pixi: {details}")


class PixiEnvPrepareError(RuntimeError):
    """Raised when a Pixi environment cannot be materialized."""

    def __init__(self, *, manifest: Path, output: str) -> None:
        details = output.strip() or "pixi did not provide any error output"
        super().__init__(f"Failed to prepare Pixi env {str(manifest)!r}: {details}")


def _list_envs(envs_dir: Path) -> list[str]:
    """Return sorted names of discoverable environments under envs_dir."""
    if not envs_dir.is_dir():
        return []
    return sorted(
        child.name
        for child in envs_dir.iterdir()
        if child.is_dir() and (child / "pixi.toml").is_file()
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
        ``project_root/envs/``. Defaults to the current working directory.

    Raises
    ------
    RuntimeError
        If ``pixi`` is not found on PATH when the registry is first used.
    """

    project_root: Path = field(default_factory=Path.cwd)
    _envs_dir: Path = field(init=False, repr=False)
    _lock_cache: dict[str, str | None] = field(default_factory=dict, init=False, repr=False)
    _prepared_manifests: set[Path] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_envs_dir", self.project_root / "envs")

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, *, env: str) -> Path:
        """Return the absolute path to the ``pixi.toml`` for *env*.

        Parameters
        ----------
        env : str
            Environment name (resolved from ``envs/<name>/``) or an explicit
            path to a ``pixi.toml``.

        Returns
        -------
        Path
            Absolute path to the ``pixi.toml``.

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

        manifest = self._envs_dir / env / "pixi.toml"
        if not manifest.is_file():
            raise PixiEnvNotFoundError(env=env, searched=self._envs_dir)
        return manifest.resolve()

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
        """Run ``pixi init --import`` into *output_dir* for a conda env spec."""
        output_dir.mkdir(parents=True, exist_ok=True)
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

    def lock_hash(self, *, env: str) -> str | None:
        """Return the SHA-256 of the environment's ``pixi.lock``, or None.

        The hash is computed once per env name and cached in memory.

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
        if env in self._lock_cache:
            return self._lock_cache[env]

        manifest = self.resolve(env=env)
        lock_path = manifest.parent / "pixi.lock"
        digest = _hash_file(lock_path) if lock_path.is_file() else None
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

    def shell_argv(self, *, env: str, cmd: str) -> list[str]:
        """Build argv to run *cmd* inside the Pixi environment.

        The command string is passed verbatim to ``bash -c``, which handles
        shell quoting, redirection, and pipes exactly as the user wrote them.

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
            "bash",
            "-c",
            cmd,
        ]

    def python_argv_c(self, *, env: str, code: str, args: Sequence[str] = ()) -> list[str]:
        """Build argv to run Python *code* with ``-c`` inside the Pixi environment.

        Using ``-c`` instead of running a script file avoids adding the script's
        directory to ``sys.path``, which would cause module-name collisions with
        stdlib modules (e.g. ``ginkgo/types.py`` shadowing ``types``).

        Parameters
        ----------
        env : str
            Environment name or path.
        code : str
            Python source code to pass to ``python -c``.
        args : Sequence[str]
            Positional arguments available as ``sys.argv[1:]`` inside *code*.

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
            "python",
            "-c",
            code,
            *args,
        ]


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _hash_file(path: Path) -> str:
    """Return the SHA-256 hex digest of a file's contents."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_pixi() -> None:
    """Raise RuntimeError when pixi is not found on PATH."""
    if shutil.which("pixi") is None:
        raise RuntimeError(
            "pixi is not installed or not found on PATH. "
            "Install pixi from https://pixi.sh before running ginkgo workflows "
            "with environment-isolated tasks."
        )
