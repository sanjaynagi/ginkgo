"""#193 — ``--memory`` accepts the Kubernetes notation ``memory=`` requires."""

from __future__ import annotations

import pytest

from ginkgo.cli.app import _build_parser, _memory_arg


class TestMemoryArg:
    def test_bare_integer_is_gib(self) -> None:
        assert _memory_arg("64") == 64

    @pytest.mark.parametrize(
        ("value", "gib"),
        [("80Gi", 80), ("512Mi", 1), ("8G", 8)],
    )
    def test_kubernetes_notation(self, value: str, gib: int) -> None:
        assert _memory_arg(value) == gib

    def test_invalid_value_keeps_the_notation_hint(self) -> None:
        """argparse reports ArgumentTypeError text verbatim; ValueError would
        collapse to a bare "invalid value"."""
        import argparse

        with pytest.raises(argparse.ArgumentTypeError, match="Kubernetes resource notation"):
            _memory_arg("eighty")

    def test_run_parser_accepts_both_forms(self) -> None:
        parser, _ = _build_parser()
        assert parser.parse_args(["run", "wf.py", "--memory", "64"]).memory == 64
        assert parser.parse_args(["run", "wf.py", "--memory", "80Gi"]).memory == 80

    def test_run_parser_rejects_nonsense_with_the_hint(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        parser, _ = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["run", "wf.py", "--memory", "eighty"])
        assert "Kubernetes resource notation" in capsys.readouterr().err
