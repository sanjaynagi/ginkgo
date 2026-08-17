"""The Jupyter environment ginkgo hands its notebook subprocesses.

Regression cover for issue #202 part 1: nbconvert probed the system Jupyter
data directories and raised ``PermissionError`` on a host where
``/usr/local/share/jupyter`` is unreadable, because the render command ran
bare and ``JUPYTER_PATH`` is purely additive.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ginkgo.runtime.notebook_kernels import build_jupyter_env_prefix
from ginkgo.runtime.task_runners.notebook import NotebookRunner


_JUPYTER_PATH_PROBE = (
    "import json, jupyter_core.paths as jp; "
    "print(json.dumps({'path': jp.jupyter_path(), 'system': jp.SYSTEM_JUPYTER_PATH}))"
)


def _probe_jupyter_path(env: dict[str, str]) -> dict[str, list[str]]:
    """Return ``jupyter_path()`` and ``SYSTEM_JUPYTER_PATH`` under ``env``."""
    completed = subprocess.run(
        [sys.executable, "-c", _JUPYTER_PATH_PROBE],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout)


def _clean_env(**overrides: str) -> dict[str, str]:
    """Return the ambient environment without Jupyter variables, plus overrides."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("JUPYTER_", "GINKGO_"))
    }
    env.update(overrides)
    return env


class TestJupyterEnvPrefix:
    """The prefix must isolate the subprocess from system Jupyter directories."""

    def test_prefix_sets_both_variables(self, tmp_path: Path) -> None:
        prefix = build_jupyter_env_prefix(jupyter_path=tmp_path / "share" / "jupyter")

        assert prefix.startswith("env ")
        assert f"JUPYTER_PATH={tmp_path / 'share' / 'jupyter'}" in prefix
        assert "JUPYTER_PLATFORM_DIRS=1" in prefix

    def test_jupyter_path_alone_keeps_the_system_directories(self, tmp_path: Path) -> None:
        """The variable ginkgo used to set on its own cannot fix this."""
        probe = _probe_jupyter_path(_clean_env(JUPYTER_PATH=str(tmp_path)))

        assert str(tmp_path) in probe["path"]
        assert [entry for entry in probe["system"] if entry in probe["path"]]

    def test_prefix_drops_the_system_directories(self, tmp_path: Path) -> None:
        """No path nbconvert probes may come from the unreadable system roots."""
        unprefixed = _probe_jupyter_path(_clean_env())
        prefixed = _probe_jupyter_path(
            _clean_env(JUPYTER_PATH=str(tmp_path), JUPYTER_PLATFORM_DIRS="1")
        )

        assert str(tmp_path) in prefixed["path"]
        assert not [entry for entry in unprefixed["system"] if entry in prefixed["path"]]


class TestRenderCommand:
    """The nbconvert render command must carry the prefix, as papermill does."""

    def test_ipynb_render_command_is_prefixed(self, tmp_path: Path) -> None:
        runner = NotebookRunner.__new__(NotebookRunner)
        jupyter_path = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter"

        command = runner._build_notebook_render_command(
            notebook_path=tmp_path / "overview.ipynb",
            notebook_kind="ipynb",
            executed_path=tmp_path / "task_0000.ipynb",
            html_path=tmp_path / "task_0000.html",
            jupyter_path=jupyter_path,
        )

        assert command.startswith(build_jupyter_env_prefix(jupyter_path=jupyter_path))
        assert "nbconvert" in command


@pytest.mark.skipif(os.name == "nt", reason="POSIX system Jupyter path layout")
class TestUnreadableSystemJupyterPath:
    """End-to-end cover with the unreadable system directory simulated.

    The condition is reproduced without touching the host's real
    ``/usr/local/share/jupyter``: a ``sitecustomize`` module points
    ``SYSTEM_JUPYTER_PATH`` at a temporary directory holding an unreadable
    ``conf.json``, exactly as a locked-down host would, and only when platform
    dirs are off — which is what makes ginkgo's ``JUPYTER_PLATFORM_DIRS=1``
    the lever that escapes it.
    """

    @staticmethod
    def _fake_system_jupyter(tmp_path: Path) -> tuple[Path, dict[str, str]]:
        """Return a sitecustomize dir and the env that activates the simulation."""
        system_dir = tmp_path / "fake-system-jupyter"
        system_dir.mkdir()
        conf = system_dir / "conf.json"
        conf.write_text("{}", encoding="utf-8")
        conf.chmod(0o000)
        if os.access(conf, os.R_OK):  # pragma: no cover - running as root
            pytest.skip("cannot make a file unreadable as this user")

        sitedir = tmp_path / "sitecustomize-dir"
        sitedir.mkdir()
        (sitedir / "sitecustomize.py").write_text(
            "import os\n"
            "import jupyter_core.paths as _jp\n"
            "if not _jp.use_platform_dirs():\n"
            "    _jp.SYSTEM_JUPYTER_PATH = [os.environ['GINKGO_FAKE_SYSTEM_JUPYTER']]\n",
            encoding="utf-8",
        )
        env = _clean_env(
            GINKGO_FAKE_SYSTEM_JUPYTER=str(system_dir),
            PYTHONPATH=os.pathsep.join(
                [str(sitedir), *([os.environ["PYTHONPATH"]] if "PYTHONPATH" in os.environ else [])]
            ),
        )
        return system_dir, env

    @staticmethod
    def _notebook(tmp_path: Path) -> Path:
        notebook = tmp_path / "executed.ipynb"
        notebook.write_text(
            json.dumps(
                {
                    "cells": [
                        {
                            "cell_type": "code",
                            "execution_count": 1,
                            "metadata": {},
                            "outputs": [],
                            "source": "print('hello')",
                        }
                    ],
                    "metadata": {},
                    "nbformat": 4,
                    "nbformat_minor": 5,
                }
            ),
            encoding="utf-8",
        )
        return notebook

    def test_simulation_reproduces_the_failure_without_the_prefix(self, tmp_path: Path) -> None:
        pytest.importorskip("nbconvert")
        _, env = self._fake_system_jupyter(tmp_path)
        notebook = self._notebook(tmp_path)

        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "jupyter",
                "nbconvert",
                "--to",
                "html",
                "--output",
                "bare",
                "--output-dir",
                str(tmp_path),
                str(notebook),
            ],
            env=env,
            capture_output=True,
            text=True,
        )

        assert completed.returncode != 0
        assert "PermissionError" in completed.stderr
        assert not (tmp_path / "bare.html").exists()

    def test_prefixed_render_command_exports_html(self, tmp_path: Path) -> None:
        pytest.importorskip("nbconvert")
        _, env = self._fake_system_jupyter(tmp_path)
        notebook = self._notebook(tmp_path)
        html_path = tmp_path / "prefixed.html"

        runner = NotebookRunner.__new__(NotebookRunner)
        command = runner._build_notebook_render_command(
            notebook_path=notebook,
            notebook_kind="ipynb",
            executed_path=notebook,
            html_path=html_path,
            jupyter_path=tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter",
        )

        completed = subprocess.run(command, shell=True, env=env, capture_output=True, text=True)

        assert completed.returncode == 0, completed.stderr
        assert html_path.is_file()
        assert "PermissionError" not in completed.stderr
