"""Tests for config loading and what a missed config key reports."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

import ginkgo
from ginkgo.config import ConfigKeyError, ConfigMapping, config_session, merge_config_layers

_CONFIG = """
windows = 3

[qc]
min_length = 50
min_quality = 20
threads = 4

[qc.thresholds]
depth = 10
"""


@pytest.fixture
def cfg() -> dict:
    Path("ginkgo.toml").write_text(_CONFIG.lstrip(), encoding="utf-8")
    return ginkgo.config("ginkgo.toml")


def _miss(mapping, *keys: str) -> str:
    for key in keys[:-1]:
        mapping = mapping[key]
    with pytest.raises(ConfigKeyError) as excinfo:
        mapping[keys[-1]]
    return str(excinfo.value)


@pytest.mark.parametrize(
    ("keys", "expected", "absent"),
    [
        (
            ("window",),
            ["'window' is not a key in ginkgo.toml", "Available: windows, qc"],
            [],
        ),
        (
            ("qc", "min_lenght"),
            [
                "'min_lenght' is not a key in [qc] of ginkgo.toml",
                "Available: min_length, min_quality, threads, thresholds",
                "Did you mean 'min_length'?",
            ],
            [],
        ),
        (("qc", "thresholds", "dpeth"), ["in [qc.thresholds] of ginkgo.toml"], []),
        # A did-you-mean on a key that resembles nothing would be noise.
        (("completely_different",), [], ["Did you mean"]),
    ],
)
def test_a_missed_key_locates_the_mistake(cfg, keys, expected, absent):
    message = _miss(cfg, *keys)

    for phrase in expected:
        assert phrase in message
    for phrase in absent:
        assert phrase not in message
    # KeyError repr-quotes its argument, which would quote the whole sentence.
    assert not message.startswith('"')


def test_empty_table_says_so_rather_than_listing_nothing():
    Path("ginkgo.toml").write_text("[qc]\n", encoding="utf-8")

    assert "It has no keys." in _miss(ginkgo.config("ginkgo.toml"), "qc", "min_length")


def test_loaded_config_still_behaves_as_a_plain_dict(cfg):
    """Workflows guard lookups with ``except KeyError``, and everything
    downstream (merging, deepcopy, the JSON ledger) treats the value as a dict."""
    with pytest.raises(KeyError):
        cfg["nope"]
    assert cfg.get("nope", "fallback") == "fallback"
    assert "nope" not in cfg
    assert isinstance(cfg, dict)
    assert cfg == {
        "windows": 3,
        "qc": {"min_length": 50, "min_quality": 20, "threads": 4, "thresholds": {"depth": 10}},
    }
    assert json.loads(json.dumps(cfg)) == cfg
    merged = merge_config_layers([cfg, {"windows": 9}])
    assert (merged["windows"], merged["qc"]["min_length"]) == (9, 50)
    # The config session deepcopies loaded values; the copy must still explain.
    assert "in [qc] of ginkgo.toml" in _miss(copy.deepcopy(cfg), "qc", "min_lenght")


def test_session_records_loaded_values_that_merge_plainly(cfg):
    """``--config`` overrides layer through the same value a miss is raised from."""
    Path("override.toml").write_text("windows = 9\n", encoding="utf-8")

    with config_session(override_paths=["override.toml"]) as session:
        overridden = ginkgo.config("ginkgo.toml")

    assert overridden["windows"] == 9
    assert session.merged_loaded_values()["windows"] == 9
    assert "in [qc] of" in _miss(overridden, "qc", "min_lenght")


@pytest.mark.parametrize(
    ("name", "text", "keys", "expected"),
    [
        # An array of tables has no name of its own, so it borrows the list's.
        (
            "ginkgo.toml",
            "[[samples]]\nname = 'a'\n\n[[samples]]\nname = 'b'\n",
            ("samples", 0, "nmae"),
            "in [samples] of ginkgo.toml",
        ),
        ("ginkgo.yaml", "qc:\n  min_length: 50\n", ("qc", "min_lenght"), "in [qc] of ginkgo.yaml"),
    ],
)
def test_lists_and_yaml_report_the_same_way(name, text, keys, expected):
    Path(name).write_text(text, encoding="utf-8")
    loaded = ginkgo.config(name)
    if keys[0] == "samples":
        assert isinstance(loaded["samples"][0], ConfigMapping)

    assert expected in _miss(loaded, *keys)
