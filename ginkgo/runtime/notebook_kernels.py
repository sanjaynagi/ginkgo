"""Managed notebook-kernel helpers for Papermill execution."""

from __future__ import annotations

import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from ginkgo.runtime.hashing import hash_str


class NotebookKernelError(RuntimeError):
    """Base error raised for managed notebook-kernel failures."""


class MissingIpykernelError(NotebookKernelError):
    """Raised when ``ipykernel`` is unavailable in the selected interpreter."""


class KernelInstallError(NotebookKernelError):
    """Raised when a managed kernelspec cannot be installed."""


@dataclass(kw_only=True, frozen=True)
class ExecutionCommand:
    """One concrete subprocess invocation."""

    argv: str | list[str]
    use_shell: bool
    display: str


@dataclass(kw_only=True, frozen=True)
class NotebookKernelSpec:
    """Resolved managed-kernel metadata for one execution environment."""

    name: str
    display_name: str
    prefix_dir: Path
    jupyter_path: Path
    spec_dir: Path
    env_label: str
    env_identity: str


class NotebookCommandBuilder(Protocol):
    """Build subprocess invocations for notebook-kernel preparation."""

    def command_for_python(self, *, env: str | None, args: list[str]) -> ExecutionCommand:
        """Return a subprocess invocation for one Python command."""
        ...


@dataclass(kw_only=True)
class NotebookKernelManager:
    """Prepare and reuse Ginkgo-managed kernelspecs."""

    runtime_root: Path
    command_builder: NotebookCommandBuilder

    def resolve_spec(self, *, env: str | None, env_identity: str | None) -> NotebookKernelSpec:
        """Return the managed kernelspec metadata for one environment."""

        # Derive a stable environment identity for deterministic kernel naming.
        resolved_identity = env_identity or _base_environment_identity()
        env_label = env or "local"
        kernel_digest = hash_str(f"{env_label}:{resolved_identity}")[:16]
        prefix_dir = self.runtime_root / "jupyter"
        spec_dir = prefix_dir / "share" / "jupyter" / "kernels" / f"ginkgo-{kernel_digest}"
        return NotebookKernelSpec(
            name=f"ginkgo-{kernel_digest}",
            display_name=f"Ginkgo ({env_label})",
            prefix_dir=prefix_dir,
            jupyter_path=prefix_dir / "share" / "jupyter",
            spec_dir=spec_dir,
            env_label=env_label,
            env_identity=resolved_identity,
        )

    def ensure_kernel(
        self,
        *,
        env: str | None,
        env_identity: str | None,
        run_command: Callable[[ExecutionCommand], subprocess.CompletedProcess[str]],
        on_installing: Callable[[NotebookKernelSpec], None] | None = None,
    ) -> NotebookKernelSpec:
        """Validate ``ipykernel`` and install a managed kernelspec when needed."""

        spec = self.resolve_spec(env=env, env_identity=env_identity)

        # Fail early when the selected interpreter does not provide ipykernel.
        probe_command = self.command_builder.command_for_python(
            env=env,
            args=["-c", "import ipykernel"],
        )
        probe_result = run_command(probe_command)
        if probe_result.returncode != 0:
            output = (probe_result.stdout or "") + (probe_result.stderr or "")
            raise MissingIpykernelError(
                "Notebook tasks require ipykernel in the selected interpreter. "
                f"Install it in {spec.env_label!r} and retry. Details: "
                f"{output.strip() or 'import ipykernel failed'}"
            )

        # Reuse deterministic kernelspecs when they are already present.
        kernel_json = spec.spec_dir / "kernel.json"
        if kernel_json.is_file():
            return spec

        # Install the managed kernelspec into .ginkgo rather than user-global paths.
        spec.prefix_dir.mkdir(parents=True, exist_ok=True)
        if on_installing is not None:
            on_installing(spec)
        install_command = self.command_builder.command_for_python(
            env=env,
            args=[
                "-m",
                "ipykernel",
                "install",
                "--prefix",
                str(spec.prefix_dir),
                "--name",
                spec.name,
                "--display-name",
                spec.display_name,
            ],
        )
        install_result = run_command(install_command)
        if install_result.returncode != 0 or not kernel_json.is_file():
            output = (install_result.stdout or "") + (install_result.stderr or "")
            raise KernelInstallError(
                "Failed to install managed notebook kernel "
                f"{spec.name!r} for {spec.env_label!r}: {output.strip() or 'kernel.json missing'}"
            )

        return spec


def build_jupyter_env_prefix(*, jupyter_path: Path) -> str:
    """Return shell-safe environment assignments for managed kernel discovery."""

    return " ".join(
        [
            "env",
            f"JUPYTER_PATH={shlex.quote(str(jupyter_path))}",
        ]
    )


def _base_environment_identity() -> str:
    """Return a best-effort identity for the current interpreter environment."""

    return hash_str(
        "::".join(
            [
                str(Path(sys.executable).resolve()),
                sys.prefix,
                sys.version,
            ]
        )
    )
