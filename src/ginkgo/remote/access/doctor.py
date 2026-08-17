"""Doctor probes for remote-input streaming.

These checks are designed to run in under a second on the driver host.
They do not attempt to actually mount anything — the goal is to catch
config-time mistakes (missing driver binaries, obviously wrong pod
annotations) before a task dispatch fails obscurely.
"""

from __future__ import annotations

import functools
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ginkgo.remote.access.resolver import AccessConfig, load_access_config


def local_streaming_available(*, scheme: str) -> bool:
    """Return whether the local host can FUSE-mount objects for ``scheme``.

    Streaming on the local host requires both the kernel FUSE device
    (``/dev/fuse``) and a healthy driver binary for the URI scheme on
    ``PATH``. Used to gate local (non-dispatched) tasks into the fuse
    path; when it returns ``False`` the resolver degrades to ``stage``.

    The probe runs a driver health check (a subprocess), so results are
    cached per scheme for the lifetime of the process.

    Parameters
    ----------
    scheme : str
        Remote URI scheme (``"s3"`` / ``"gs"`` / ``"oci"``).

    Returns
    -------
    bool
        ``True`` when ``/dev/fuse`` exists and the scheme's driver passes
        its health check; ``False`` otherwise (including on macOS, where
        ``/dev/fuse`` is absent).
    """
    return _probe_local_streaming(scheme)


@functools.lru_cache(maxsize=None)
def _probe_local_streaming(scheme: str) -> bool:
    """Uncached probe body behind :func:`local_streaming_available`."""
    if not Path("/dev/fuse").exists():
        return False
    from ginkgo.remote.access.drivers.base import resolve_driver

    try:
        resolve_driver(scheme=scheme).health_check()
    except Exception:  # noqa: BLE001 - any driver failure means unavailable
        return False
    return True


@dataclass(frozen=True, kw_only=True)
class AccessDiagnostic:
    """One diagnostic entry produced by the streaming probes."""

    code: str
    severity: str
    message: str
    suggestion: str | None = None


def collect_access_diagnostics(
    *,
    project_root: Path | None = None,
    executor_configs: dict[str, dict[str, Any]] | None = None,
) -> list[AccessDiagnostic]:
    """Run the FUSE-specific doctor probes.

    Parameters
    ----------
    project_root : Path | None
        Project directory used to locate ``ginkgo.toml``. Defaults to the
        current working directory.
    executor_configs : dict[str, dict[str, Any]] | None
        Every configured executor section, keyed by its name in
        ``ginkgo.toml`` (``"[remote.k8s]"``,
        ``"[remote.executors.gpu-k8s]"``). Each is checked for a
        ``fuse_image`` when any task is expected to stream, since a task
        may be pinned to any of them.

    Returns
    -------
    list[AccessDiagnostic]
        Empty when all probes pass.
    """
    diagnostics: list[AccessDiagnostic] = []
    config = load_access_config(project_root=project_root)

    streaming_ever_enabled = _streaming_ever_enabled(config=config)
    if not streaming_ever_enabled:
        return diagnostics

    # Probe 1: FUSE driver binaries reachable on PATH (driver host).
    diagnostics.extend(_probe_driver_binaries())

    # Probe 2: /dev/fuse availability (informational — only matters on
    # the driver host when workers run locally; on a pod it is satisfied
    # by the cluster).
    if not Path("/dev/fuse").exists():
        diagnostics.append(
            AccessDiagnostic(
                code="FUSE_DEVICE_MISSING",
                severity="warning",
                message="/dev/fuse is not present on this host.",
                suggestion=(
                    "This is expected on macOS. On Linux workers, ensure the pod "
                    "has the FUSE device plugin or runs in privileged mode."
                ),
            )
        )

    # Probe 3: every executor's worker image has a dedicated fuse variant.
    # Checked per section, not once for the run: a task pinned to an
    # executor without a fuse_image fails to mount however well the other
    # executors are configured.
    for section, executor_config in (executor_configs or {}).items():
        if isinstance(executor_config, dict) and not executor_config.get("fuse_image"):
            diagnostics.append(
                AccessDiagnostic(
                    code="FUSE_IMAGE_NOT_CONFIGURED",
                    severity="warning",
                    message=(
                        f"Remote streaming is enabled but no fuse_image is "
                        f"configured in {section}."
                    ),
                    suggestion=(
                        f'Set fuse_image = "<registry>/ginkgo-worker-fuse:<tag>" in {section}.'
                    ),
                )
            )

    return diagnostics


def _streaming_ever_enabled(*, config: AccessConfig) -> bool:
    """Return True when any config setting could trigger streaming."""
    if config.auto_fuse:
        return True
    if config.default == "fuse":
        return True
    return any(mode == "fuse" for _glob, mode in config.pattern_defaults)


def _probe_driver_binaries() -> list[AccessDiagnostic]:
    """Warn about missing driver binaries. Not an error — the worker image
    may carry them instead."""
    results: list[AccessDiagnostic] = []
    missing = []
    if shutil.which("gcsfuse") is None:
        missing.append("gcsfuse")
    if shutil.which("mount-s3") is None and shutil.which("mountpoint-s3") is None:
        missing.append("mountpoint-s3")
    if shutil.which("rclone") is None:
        missing.append("rclone")
    if missing:
        results.append(
            AccessDiagnostic(
                code="FUSE_DRIVERS_NOT_ON_HOST",
                severity="info",
                message=(
                    "These FUSE drivers are not on the driver host's PATH: " + ", ".join(missing)
                ),
                suggestion=(
                    "Install them in the worker image (Dockerfile.worker-fuse). "
                    "They do not need to be available on the driver host."
                ),
            )
        )

    if os.environ.get("GINKGO_FUSE_SKIP_DRIVER_PROBE"):
        return []
    return results
