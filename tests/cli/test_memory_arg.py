"""#193 — ``--memory`` accepts the Kubernetes notation ``memory=`` requires."""

from __future__ import annotations

import argparse

import pytest

from ginkgo.cli.app import _build_parser, _memory_arg


@pytest.mark.parametrize(("value", "gib"), [("64", 64), ("80Gi", 80), ("512Mi", 1), ("8G", 8)])
def test_memory_accepts_bare_gib_and_kubernetes_notation(value: str, gib: int) -> None:
    assert _memory_arg(value) == gib
    parser, _ = _build_parser()
    assert parser.parse_args(["run", "wf.py", "--memory", value]).memory == gib


def test_nonsense_keeps_the_notation_hint(capsys: pytest.CaptureFixture) -> None:
    """argparse reports ArgumentTypeError text verbatim; ValueError would collapse it."""
    with pytest.raises(argparse.ArgumentTypeError, match="Kubernetes resource notation"):
        _memory_arg("eighty")
    parser, _ = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "wf.py", "--memory", "eighty"])
    assert "Kubernetes resource notation" in capsys.readouterr().err
