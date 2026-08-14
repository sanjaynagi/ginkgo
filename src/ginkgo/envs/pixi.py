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


import os
import subprocess
import shutil
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.core.hashing import hash_bytes, hash_file


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


def _names_a_relative_location(node: Any, *, key: str | None = None) -> bool:
    """Return whether *node* holds a location written relative to its manifest.

    Scans the whole parsed document rather than an enumerated set of dependency
    tables, because Pixi accepts the same dependency spec in many positions:
    top level, ``[feature.<name>.*]``, ``[target.<platform>.*]``, and the two
    nested. Enumerating positions means every new one Pixi adds is a silent
    hole, so this matches on the spec's own shape instead.

    Parameters
    ----------
    node : Any
        A parsed TOML value.
    key : str | None
        The table key *node* was found under, used to recognise
        ``[activation] scripts``.
    """
    if isinstance(node, dict):
        # `{ path = ... }` and `{ editable = true }` dependency specs.
        if isinstance(node.get("path"), str) or node.get("editable") is True:
            return True
        return any(_names_a_relative_location(value, key=name) for name, value in node.items())

    # Activation scripts are manifest-relative script paths, not a spec table.
    if key == "scripts" and isinstance(node, list):
        return any(isinstance(entry, str) for entry in node)
    return False


def _blocks_relocation(manifest: Path) -> bool:
    """Return whether *manifest* cannot be copied into the shared prefix.

    A manifest that names anything by a relative path — a dependency, an
    activation script — resolves that path against its own directory, so moving
    it breaks the reference. Reported conservatively: an unreadable or
    unparseable manifest counts as non-relocatable so it stays local.
    """
    # A pyproject.toml manifest always builds the surrounding project, which is
    # itself a path reference, so it can never be relocated.
    if manifest.name == "pyproject.toml":
        return True

    try:
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        return True

    return _names_a_relative_location(data)


def _is_pixi_pyproject(path: Path) -> bool:
    """Return whether *path* is a ``pyproject.toml`` carrying ``[tool.pixi]``.

    Parameters
    ----------
    path : Path
        Candidate manifest path.

    Returns
    -------
    bool
        True only when *path* is a readable ``pyproject.toml`` whose parsed
        contents contain a ``[tool.pixi]`` table. Unreadable or malformed TOML
        is treated as a non-match rather than an error.
    """
    if path.name != "pyproject.toml" or not path.is_file():
        return False
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return False
    tool = data.get("tool")
    return isinstance(tool, dict) and "pixi" in tool


def _env_manifest(env_dir: Path) -> Path | None:
    """Return the Pixi manifest within *env_dir*, or None if absent.

    Prefers ``pixi.toml``; falls back to a ``pyproject.toml`` that carries a
    ``[tool.pixi]`` section.

    Parameters
    ----------
    env_dir : Path
        Directory of a named environment (``envs/<name>/``).

    Returns
    -------
    Path | None
        Path to the manifest file, or ``None`` when neither manifest exists.
    """
    pixi_toml = env_dir / "pixi.toml"
    if pixi_toml.is_file():
        return pixi_toml
    pyproject = env_dir / "pyproject.toml"
    if _is_pixi_pyproject(pyproject):
        return pyproject
    return None


def _list_envs(envs_dir: Path) -> list[str]:
    """Return sorted names of discoverable environments under envs_dir."""
    if not envs_dir.is_dir():
        return []
    return sorted(
        child.name
        for child in envs_dir.iterdir()
        if child.is_dir() and _env_manifest(child) is not None
    )


def _is_conda_env_file(path: Path) -> bool:
    """Return whether *path* names a supported conda env spec file."""
    return path.name in {"environment.yml", "environment.yaml"}


def _is_explicit_path(env: str) -> bool:
    """Return True when env looks like a filesystem path rather than a name."""
    return "/" in env or env.startswith(".")


def resolve_shared_env_root(
    *,
    cli_value: str | None = None,
    config: dict[str, Any] | None = None,
    project_root: Path | None = None,
) -> Path | None:
    """Return the shared environment prefix, or ``None`` when not configured.

    Sharing is opt-in. The CLI flag wins over ``[envs] shared_prefix`` in
    ``ginkgo.toml``; when neither is set, environments install beside their
    declaring manifest as before.

    Parameters
    ----------
    cli_value : str | None
        Value of ``--env-prefix``, when given. A relative value resolves
        against the working directory, as command-line paths do.
    config : dict | None
        Parsed runtime config.
    project_root : Path | None
        Directory holding the ``ginkgo.toml`` that supplied the config. A
        relative ``shared_prefix`` resolves against it, so the prefix names the
        same directory no matter where ginkgo was invoked from. Defaults to the
        working directory.

    Returns
    -------
    Path | None
        Absolute prefix directory, with ``~`` expanded.
    """
    if cli_value:
        return Path(cli_value).expanduser().resolve()

    envs_config = (config or {}).get("envs")
    configured = envs_config.get("shared_prefix") if isinstance(envs_config, dict) else None
    if not isinstance(configured, str) or not configured:
        return None

    prefix = Path(configured).expanduser()
    if prefix.is_absolute():
        return prefix.resolve()
    return ((project_root or Path.cwd()) / prefix).resolve()


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
    shared_env_root : Path | None
        When set, environments are installed under
        ``shared_env_root/<content-hash>/`` instead of beside the declaring
        manifest, so workflows declaring byte-identical environments install
        once and share. ``None`` (default) keeps the per-workflow behaviour.

    Raises
    ------
    RuntimeError
        If ``pixi`` is not found on PATH when the registry is first used.
    """

    project_root: Path = field(default_factory=Path.cwd)
    workflow_root: Path | None = None
    shared_env_root: Path | None = None
    _envs_dirs: tuple[Path, ...] = field(init=False, repr=False)
    _lock_cache: dict[str, str | None] = field(default_factory=dict, init=False, repr=False)
    _prepared_manifests: set[Path] = field(default_factory=set, init=False, repr=False)
    _shared_manifests: dict[Path, Path] = field(default_factory=dict, init=False, repr=False)

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
            manifest = _env_manifest(envs_dir / env)
            if manifest is not None:
                return manifest.resolve()

        searched = self._envs_dirs[0] if self._envs_dirs else None
        raise PixiEnvNotFoundError(env=env, searched=searched)

    @property
    def env_directories(self) -> tuple[Path, ...]:
        """Return the environment discovery roots for this project."""
        return self._envs_dirs

    def install_manifest(self, *, env: str) -> Path:
        """Return the manifest Pixi should install and run *env* from.

        With no shared prefix configured this is the declaring manifest, so
        Pixi installs into ``.pixi/envs/`` beside it. With a shared prefix, the
        manifest is copied into ``shared_env_root/<content-hash>/`` and that
        copy is returned, so two workflows declaring byte-identical
        environments resolve to one directory and install once.

        Falls back to the declaring manifest when the environment cannot be
        shared safely — see :func:`_blocks_relocation`.

        Parameters
        ----------
        env : str
            Environment name or path.

        Returns
        -------
        Path
            Absolute path to the manifest to hand to ``pixi``.
        """
        return self.install_manifest_for(manifest=self.resolve(env=env))

    def install_manifest_for(self, *, manifest: Path) -> Path:
        """Return the manifest Pixi installs from, given the declaring one.

        The by-name entry point is :meth:`install_manifest`; this variant serves
        callers that have already resolved a manifest and must not resolve the
        name a second time, such as ``ginkgo env ls`` walking the discovery
        roots.

        Parameters
        ----------
        manifest : Path
            The declaring manifest.

        Returns
        -------
        Path
        """
        if self.shared_env_root is None:
            return manifest
        if manifest in self._shared_manifests:
            return self._shared_manifests[manifest]

        shared = self._materialize_shared_manifest(source=manifest)
        self._shared_manifests[manifest] = shared
        return shared

    def install_dir_for(self, *, manifest: Path) -> Path:
        """Return the directory Pixi installs *manifest*'s environments into.

        Pixi installs into ``.pixi/`` beside the manifest it is given, so under
        a shared prefix this is inside the prefix rather than in the workflow.

        Parameters
        ----------
        manifest : Path
            The declaring manifest.

        Returns
        -------
        Path
        """
        return self.install_manifest_for(manifest=manifest).parent / ".pixi"

    def _materialize_shared_manifest(self, *, source: Path) -> Path:
        """Copy *source* into the shared prefix, keyed by its content."""
        if _blocks_relocation(source):
            return source

        lock_path = source.parent / "pixi.lock"
        # The lock joins the key so a manifest pinned to different resolutions
        # never collides, and so a shared env is reproducible from what the
        # workflow actually committed.
        manifest_bytes = source.read_bytes()
        lock_bytes = lock_path.read_bytes() if lock_path.is_file() else b""
        # Length-prefixed so that moving the boundary between the two cannot
        # produce the same digest from a different pair.
        payload = b"%d\0%s%d\0%s" % (
            len(manifest_bytes),
            manifest_bytes,
            len(lock_bytes),
            lock_bytes,
        )
        target_dir = self.shared_env_root / hash_bytes(payload)
        target_manifest = target_dir / source.name
        if target_manifest.is_file():
            return target_manifest

        self.shared_env_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(dir=self.shared_env_root, prefix=".staging-"))
        try:
            shutil.copy2(source, staging / source.name)
            if lock_path.is_file():
                shutil.copy2(lock_path, staging / "pixi.lock")
            try:
                os.replace(staging, target_dir)
            except OSError:
                if target_manifest.is_file():
                    # Another process materialized the same key first. Its copy
                    # is byte-identical by construction, so prefer it.
                    shutil.rmtree(staging, ignore_errors=True)
                elif target_dir.is_dir():
                    # The directory exists without the manifest: a half-removed
                    # entry, or one pruned by hand. Fill it in rather than
                    # failing, since the contents are known.
                    for child in staging.iterdir():
                        shutil.copy2(child, target_dir / child.name)
                    shutil.rmtree(staging, ignore_errors=True)
                else:
                    raise
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

        return target_manifest

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
        """Return the BLAKE3 digest of the environment's ``pixi.lock``, or None.

        The hash is computed once per env name and cached in memory.

        Parameters
        ----------
        env : str
            Environment name or path.

        Returns
        -------
        str | None
            Hex digest, or ``None`` if no lockfile exists yet.
        """
        cached = self._lock_cache.get(env)
        if cached is not None:
            return cached

        lock_path = self.lock_path(env=env)
        digest = hash_file(lock_path) if lock_path is not None else None
        # A miss is not memoized: the lock may not exist until `prepare` solves
        # it, and callers ask for the identity on both sides of that.
        if digest is not None:
            self._lock_cache[env] = digest
        return digest

    def lock_path(self, *, env: str) -> Path | None:
        """Return the ``pixi.lock`` governing *env*, or ``None`` if none exists.

        Prefers the lock the workflow committed beside its manifest. Falls back
        to the one in the shared prefix, which is where Pixi writes the solved
        lock for an environment that shipped without one — without this the
        environment's identity and provenance would be lost precisely when
        sharing is enabled.

        Parameters
        ----------
        env : str
            Environment name or path.

        Returns
        -------
        Path | None
        """
        candidates = [self.resolve(env=env).parent / "pixi.lock"]
        if self.shared_env_root is not None:
            candidates.append(self.install_manifest(env=env).parent / "pixi.lock")

        return next((path for path in candidates if path.is_file()), None)

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
        manifest = self.install_manifest(env=env)
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
        manifest = self.install_manifest(env=env)
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
