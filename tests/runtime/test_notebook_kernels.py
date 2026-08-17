"""The Jupyter environment ginkgo hands its notebook subprocesses.

Regression cover for issue #202 part 1: nbconvert probed the system Jupyter
data directories and raised ``PermissionError`` on a host where
``/usr/local/share/jupyter`` is unreadable, because the render command ran
bare and ``JUPYTER_PATH`` is purely additive.

The property under test is behavioural, not a variable list: with an unreadable
``conf.json`` planted in a directory the host would otherwise search, the render
command ginkgo builds still exits 0 and writes the HTML. Asserting on the
contents of ``jupyter_path()`` instead would have passed on macOS while the
defect was still live on Linux, which is how the first attempt at this fix got
through review.
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


pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX Jupyter data-path layout")


def _clean_env(**overrides: str) -> dict[str, str]:
    """Return the ambient environment with Jupyter/XDG data vars stripped.

    ``PATH`` is pinned to the running interpreter's own ``bin`` first, because
    ``python -m jupyter nbconvert`` dispatches to whichever
    ``jupyter-nbconvert`` script ``PATH`` finds. Without this the test can
    silently measure a different interpreter's nbconvert.
    """
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("JUPYTER_", "GINKGO_")) and key != "XDG_DATA_DIRS"
    }
    env["PATH"] = os.pathsep.join(
        [str(Path(sys.executable).parent), *([os.environ["PATH"]] if "PATH" in os.environ else [])]
    )
    env.update(overrides)
    return env


def _notebook(tmp_path: Path) -> Path:
    """Write a minimal executed notebook for nbconvert to render."""
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


_SYSTEM_PATH_PROBE = (
    "import json, jupyter_core.paths as jp; print(json.dumps(jp.SYSTEM_JUPYTER_PATH))"
)


def _system_jupyter_paths(env: dict[str, str]) -> list[Path]:
    """Return ``SYSTEM_JUPYTER_PATH`` as computed under ``env``."""
    completed = subprocess.run(
        [sys.executable, "-c", _SYSTEM_PATH_PROBE],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return [Path(entry) for entry in json.loads(completed.stdout)]


def _plant_unreadable_conf(*, env: dict[str, str], under: Path) -> list[Path]:
    """Plant an unreadable ``conf.json`` where ``env`` says Jupyter will look.

    The directory is discovered by asking ``jupyter_core`` rather than being
    predicted, because the answer is neither stable across platforms nor
    something this test should encode: the same override yields
    ``.../Jupyter`` on macOS and ``.../jupyter`` on Linux. Guessing it wrong
    plants the file where nothing reads it and the simulation passes
    vacuously — which is how a macOS-only version of this fix survived a
    green suite.
    """
    targets = [path for path in _system_jupyter_paths(env) if path.is_relative_to(under)]
    assert targets, (
        "the hostile environment did not redirect SYSTEM_JUPYTER_PATH under "
        f"{under}, so nothing would read the planted file: "
        f"{_system_jupyter_paths(env)}"
    )
    for target in targets:
        target.mkdir(parents=True, exist_ok=True)
        conf = target / "conf.json"
        conf.write_text("{}", encoding="utf-8")
        conf.chmod(0o000)
        if os.access(conf, os.R_OK):  # pragma: no cover - running as root
            pytest.skip("cannot make a file unreadable as this user")
    return targets


def _render_command(*, notebook: Path, html_path: Path, jupyter_path: Path) -> str:
    """Return the render command ginkgo builds for one notebook task."""
    runner = NotebookRunner.__new__(NotebookRunner)
    return runner._build_notebook_render_command(
        notebook_path=notebook,
        notebook_kind="ipynb",
        executed_path=notebook,
        html_path=html_path,
        jupyter_path=jupyter_path,
    )


def _run(command: str | list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        shell=isinstance(command, str),
        env=env,
        capture_output=True,
        text=True,
    )


class TestExportSurvivesAnUnreadableHostConfig:
    """The property: a hostile host config cannot fail the export.

    Two hostile shapes are covered, because the platforms differ in where the
    system Jupyter data directories come from.

    ``hostile_platform_dirs``
        The host already has ``JUPYTER_PLATFORM_DIRS=1``, and its
        ``XDG_DATA_DIRS`` names an unreadable Jupyter data directory. This is
        the Linux shape: there, the platform-appropriate system data
        directories *are* ``/usr/local/share`` and ``/usr/share``, so the flag
        ginkgo sets moves nothing on its own and only the ``XDG_DATA_DIRS``
        override escapes.

    ``hostile_hardcoded_roots``
        Plain default configuration, where ``SYSTEM_JUPYTER_PATH`` is the
        hardcoded ``/usr/local/share/jupyter``. A ``sitecustomize`` module
        redirects that constant to an unreadable temporary directory — the
        reported host's exact shape, without touching the real one. It applies
        only while platform dirs are off, which is what the real host looks
        like, so ginkgo's escape has to come from both of its assignments.
    """

    @staticmethod
    def _hostile_platform_dirs(tmp_path: Path) -> dict[str, str]:
        data_dir = tmp_path / "hostile-xdg"
        data_dir.mkdir()
        env = _clean_env(JUPYTER_PLATFORM_DIRS="1", XDG_DATA_DIRS=str(data_dir))
        _plant_unreadable_conf(env=env, under=data_dir)
        return env

    @staticmethod
    def _hostile_hardcoded_roots(tmp_path: Path) -> dict[str, str]:
        system_dir = tmp_path / "hostile-system" / "jupyter"

        sitedir = tmp_path / "sitecustomize-dir"
        sitedir.mkdir()
        (sitedir / "sitecustomize.py").write_text(
            "import os\n"
            "import jupyter_core.paths as _jp\n"
            "if not _jp.use_platform_dirs():\n"
            "    _jp.SYSTEM_JUPYTER_PATH = [os.environ['GINKGO_TEST_SYSTEM_JUPYTER']]\n",
            encoding="utf-8",
        )
        env = _clean_env(
            PYTHONPATH=os.pathsep.join(
                [str(sitedir), *([os.environ["PYTHONPATH"]] if "PYTHONPATH" in os.environ else [])]
            )
        )
        # Set after _clean_env, which strips GINKGO_-prefixed variables.
        env["GINKGO_TEST_SYSTEM_JUPYTER"] = str(system_dir)
        _plant_unreadable_conf(env=env, under=tmp_path)
        return env

    @pytest.fixture(params=["hostile_platform_dirs", "hostile_hardcoded_roots"])
    def hostile_env(self, request: pytest.FixtureRequest, tmp_path: Path) -> dict[str, str]:
        return getattr(self, f"_{request.param}")(tmp_path)

    def test_the_simulated_condition_really_breaks_a_bare_export(
        self, tmp_path: Path, hostile_env: dict[str, str]
    ) -> None:
        """Without ginkgo's prefix the export must fail, or the fix proves nothing."""
        pytest.importorskip("nbconvert")
        notebook = _notebook(tmp_path)

        completed = _run(
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
            hostile_env,
        )

        assert completed.returncode != 0
        assert "PermissionError" in completed.stderr
        assert not (tmp_path / "bare.html").exists()

    def test_ginkgos_render_command_exports_html_anyway(
        self, tmp_path: Path, hostile_env: dict[str, str]
    ) -> None:
        pytest.importorskip("nbconvert")
        notebook = _notebook(tmp_path)
        html_path = tmp_path / "prefixed.html"

        completed = _run(
            _render_command(
                notebook=notebook,
                html_path=html_path,
                jupyter_path=tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter",
            ),
            hostile_env,
        )

        assert completed.returncode == 0, completed.stderr
        assert html_path.is_file()
        assert "PermissionError" not in completed.stderr


class TestJupyterEnvPrefix:
    """What the prefix must contain for the property above to hold."""

    def test_the_render_command_carries_the_prefix(self, tmp_path: Path) -> None:
        """The render used to run bare while only Papermill got the prefix."""
        jupyter_path = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter"

        command = _render_command(
            notebook=tmp_path / "overview.ipynb",
            html_path=tmp_path / "task_0000.html",
            jupyter_path=jupyter_path,
        )

        assert command.startswith(build_jupyter_env_prefix(jupyter_path=jupyter_path))
        assert "nbconvert" in command

    def test_the_system_data_directories_become_ginkgos_own(self, tmp_path: Path) -> None:
        """Narrow mechanism check, stated as ownership rather than as paths.

        Asserting that particular host paths are absent would hold on macOS
        while ``SYSTEM_JUPYTER_PATH`` was still ``/usr/share/jupyter`` on Linux.
        Asserting that every entry lies under the prefix ginkgo owns holds on
        both.
        """
        jupyter_path = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter"
        jupyter_path.mkdir(parents=True)

        completed = _run(
            f"{build_jupyter_env_prefix(jupyter_path=jupyter_path)} "
            f"{sys.executable} -c "
            "'import json, jupyter_core.paths as jp; print(json.dumps(jp.SYSTEM_JUPYTER_PATH))'",
            _clean_env(),
        )

        assert completed.returncode == 0, completed.stderr
        system_paths = json.loads(completed.stdout)
        assert system_paths
        for entry in system_paths:
            assert Path(entry).is_relative_to(jupyter_path)
