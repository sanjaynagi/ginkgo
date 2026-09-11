"""Tests for config loading and what a missed config key reports."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

import ginkgo
from ginkgo.config import (
    ConfigKeyError,
    ConfigMapping,
    config_session,
    merge_config_layers,
)

_CONFIG = """
windows = 3

[qc]
min_length = 50
min_quality = 20
threads = 4

[qc.thresholds]
depth = 10
"""


def _write_config(name: str = "ginkgo.toml") -> Path:
    path = Path(name)
    path.write_text(_CONFIG.lstrip(), encoding="utf-8")
    return path


def test_missing_top_level_key_names_the_file_and_the_keys():
    """The bare KeyError said nothing; the message must locate the mistake."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["window"]

    message = str(excinfo.value)
    assert "'window' is not a key in ginkgo.toml" in message
    assert "Available: windows, qc" in message


def test_missing_nested_key_names_its_section():
    """A nested miss reported only the key, with no hint of which table it was in."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["qc"]["min_lenght"]

    message = str(excinfo.value)
    assert "'min_lenght' is not a key in [qc] of ginkgo.toml" in message
    assert "Available: min_length, min_quality, threads, thresholds" in message


def test_deeply_nested_section_reports_its_full_trail():
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["qc"]["thresholds"]["dpeth"]

    assert "in [qc.thresholds] of ginkgo.toml" in str(excinfo.value)


def test_close_match_is_suggested():
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["qc"]["min_lenght"]

    assert "Did you mean 'min_length'?" in str(excinfo.value)


def test_unrelated_key_gets_no_suggestion():
    """A did-you-mean on a key that resembles nothing would be noise."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["completely_different"]

    assert "Did you mean" not in str(excinfo.value)


def test_empty_table_says_so_rather_than_listing_nothing():
    Path("ginkgo.toml").write_text("[qc]\n", encoding="utf-8")
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["qc"]["min_length"]

    assert "It has no keys." in str(excinfo.value)


def test_missed_key_is_still_a_key_error():
    """Workflows that guard a lookup with ``except KeyError`` must keep working."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(KeyError):
        cfg["nope"]
    assert cfg.get("nope", "fallback") == "fallback"
    assert "nope" not in cfg


def test_message_is_not_repr_quoted_whole():
    """KeyError repr-quotes its argument, which would quote the whole sentence."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["nope"]

    assert not str(excinfo.value).startswith('"')


def test_loaded_config_is_a_dict_and_equals_a_plain_dict():
    """Everything downstream treats the value as a dict, so it must stay one."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    assert isinstance(cfg, dict)
    assert cfg == {
        "windows": 3,
        "qc": {
            "min_length": 50,
            "min_quality": 20,
            "threads": 4,
            "thresholds": {"depth": 10},
        },
    }


def test_deepcopy_keeps_the_message():
    """The config session deepcopies loaded values; the copy must still explain."""
    _write_config()
    cfg = copy.deepcopy(ginkgo.config("ginkgo.toml"))

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["qc"]["min_lenght"]

    assert "in [qc] of ginkgo.toml" in str(excinfo.value)


def test_loaded_config_serialises_to_json():
    """Run params reach the ledger as JSON, so the mapping must serialise."""
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    assert json.loads(json.dumps(cfg)) == cfg


def test_merging_layers_accepts_the_mapping():
    _write_config()
    cfg = ginkgo.config("ginkgo.toml")

    merged = merge_config_layers([cfg, {"windows": 9}])

    assert merged["windows"] == 9
    assert merged["qc"]["min_length"] == 50


def test_session_records_loaded_values_that_merge_plainly():
    """``--config`` overrides layer through the same value a miss is raised from."""
    _write_config()
    Path("override.toml").write_text("windows = 9\n", encoding="utf-8")

    with config_session(override_paths=["override.toml"]) as session:
        cfg = ginkgo.config("ginkgo.toml")

    assert cfg["windows"] == 9
    assert session.merged_loaded_values()["windows"] == 9

    with pytest.raises(ConfigKeyError):
        cfg["qc"]["min_lenght"]


def test_table_inside_a_list_reports_the_list_it_came_from():
    """An array of tables has no name of its own, so it borrows the list's."""
    Path("ginkgo.toml").write_text(
        "[[samples]]\nname = 'a'\n\n[[samples]]\nname = 'b'\n", encoding="utf-8"
    )
    cfg = ginkgo.config("ginkgo.toml")

    assert isinstance(cfg["samples"][0], ConfigMapping)
    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["samples"][0]["nmae"]

    assert "in [samples] of ginkgo.toml" in str(excinfo.value)


def test_yaml_config_reports_the_same_way():
    Path("ginkgo.yaml").write_text("qc:\n  min_length: 50\n", encoding="utf-8")
    cfg = ginkgo.config("ginkgo.yaml")

    with pytest.raises(ConfigKeyError) as excinfo:
        cfg["qc"]["min_lenght"]

    assert "in [qc] of ginkgo.yaml" in str(excinfo.value)
