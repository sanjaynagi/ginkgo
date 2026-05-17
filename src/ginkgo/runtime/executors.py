"""Heterogeneous collection of the concurrent evaluator's executors.

The evaluator runs work on four distinct pools, each with its own role:

* ``python`` — a process pool (or thread-pool fallback, when the host denies
  process creation) for in-process task bodies.
* ``shell`` — a thread pool for shell, notebook, script, and subworkflow
  drivers.
* ``staging`` — a thread pool that pre-stages remote inputs before dispatch.
* ``remote_watcher`` — a thread pool, created lazily on the first remote
  dispatch, whose threads block on remote job handles and convert their
  completion into local futures.

``Executors`` owns all four as a single ``with``-block resource. ``__exit__``
shuts every pool down without waiting for in-flight work, which also closes a
prior leak where the lazily-created ``remote_watcher`` was never released on
the normal exit path.
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from multiprocessing import get_context
from typing import Any


@dataclass(kw_only=True)
class Executors:
    """Context-managed owner of the four executor pools used by the evaluator."""

    jobs: int
    staging_jobs: int
    _python: ProcessPoolExecutor | ThreadPoolExecutor | None = field(
        default=None, init=False, repr=False
    )
    _shell: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)
    _staging: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)
    _remote_watcher: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)

    def __enter__(self) -> "Executors":
        try:
            self._python = ProcessPoolExecutor(
                max_workers=self.jobs,
                mp_context=get_context("spawn"),
            )
        except PermissionError:
            self._python = ThreadPoolExecutor(max_workers=self.jobs)
        self._shell = ThreadPoolExecutor(max_workers=self.jobs)
        self._staging = ThreadPoolExecutor(max_workers=self.staging_jobs)
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown_all()

    @property
    def python(self) -> ProcessPoolExecutor | ThreadPoolExecutor:
        assert self._python is not None, "Executors must be entered before use"
        return self._python

    @property
    def shell(self) -> ThreadPoolExecutor:
        assert self._shell is not None, "Executors must be entered before use"
        return self._shell

    @property
    def staging(self) -> ThreadPoolExecutor:
        assert self._staging is not None, "Executors must be entered before use"
        return self._staging

    def get_or_create_remote_watcher(self) -> ThreadPoolExecutor:
        """Return the watcher pool, creating it on first use."""
        if self._remote_watcher is None:
            self._remote_watcher = ThreadPoolExecutor(
                max_workers=self.jobs or 8,
                thread_name_prefix="ginkgo-remote-watcher",
            )
        return self._remote_watcher

    def shutdown_all(self) -> None:
        """Shut every pool down without waiting for in-flight work."""
        self._shutdown_staging()
        self._shutdown_shell()
        self._shutdown_python()
        self._shutdown_remote_watcher()

    def _shutdown_staging(self) -> None:
        if self._staging is None:
            return
        with suppress(Exception):
            self._staging.shutdown(wait=False, cancel_futures=True)

    def _shutdown_shell(self) -> None:
        if self._shell is None:
            return
        with suppress(Exception):
            self._shell.shutdown(wait=False, cancel_futures=True)

    def _shutdown_python(self) -> None:
        if self._python is None:
            return
        executor = self._python
        with suppress(Exception):
            executor.shutdown(wait=False, cancel_futures=True)
        if isinstance(executor, ProcessPoolExecutor):
            self._terminate_process_pool_workers(executor=executor)

    def _shutdown_remote_watcher(self) -> None:
        if self._remote_watcher is None:
            return
        with suppress(Exception):
            self._remote_watcher.shutdown(wait=False, cancel_futures=True)

    def _terminate_process_pool_workers(self, *, executor: ProcessPoolExecutor) -> None:
        """Force-terminate active workers using the executor's process table."""
        processes = getattr(executor, "_processes", None)
        if not isinstance(processes, dict):
            return

        for process in list(processes.values()):
            with suppress(Exception):
                if process.is_alive():
                    process.terminate()

        for process in list(processes.values()):
            with suppress(Exception):
                process.join(timeout=0.2)

        for process in list(processes.values()):
            with suppress(Exception):
                if process.is_alive():
                    process.kill()
