"""Network-filesystem detection for the store's one-line warning."""

from __future__ import annotations

from pathlib import Path

from ginkgo.store import fs


def test_a_local_path_is_not_a_network_filesystem_even_before_it_exists(tmp_path):
    assert fs.is_network_filesystem(tmp_path / "ginkgo.db") is None
    assert fs.is_network_filesystem(tmp_path / "missing" / "ginkgo.db") is None


def test_the_longest_matching_mount_point_classifies_the_path(tmp_path, monkeypatch):
    nested = tmp_path / "scratch"
    nested.mkdir()
    monkeypatch.setattr(
        fs, "_mount_table", lambda: [("/", "apfs"), (str(tmp_path), "nfs4"), (str(nested), "ext4")]
    )

    assert fs.is_network_filesystem(tmp_path / "ginkgo.db") == "nfs4"
    assert fs.is_network_filesystem(nested / "ginkgo.db") is None


def test_both_mount_table_formats_are_parsed():
    proc = "server:/vol /mnt/data nfs4 rw,relatime 0 0\n/dev/sda1 / ext4 rw 0 0\n"
    bsd = (
        "/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)\n"
        "//user@host/share on /Volumes/share (smbfs, nodev, nosuid, mounted by user)\n"
    )

    assert fs._parse_proc_mounts(proc) == [("/mnt/data", "nfs4"), ("/", "ext4")]
    assert fs._parse_mount_output(bsd) == [("/", "apfs"), ("/Volumes/share", "smbfs")]


def test_a_network_path_warns_once_naming_the_database(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(fs, "_warned", False)
    fs.warn_if_network_filesystem(tmp_path / "ginkgo.db")
    assert capsys.readouterr().err == ""

    monkeypatch.setattr(fs, "_warned", False)
    monkeypatch.setattr(fs, "is_network_filesystem", lambda path: "lustre")
    fs.warn_if_network_filesystem(Path("/mnt/shared/ledger.db"))
    fs.warn_if_network_filesystem(Path("/mnt/shared/ledger.db"))

    err = " ".join(capsys.readouterr().err.split())
    assert err.count("SQLite locking may be unreliable") == 1
    assert "/mnt/shared/ledger.db is on lustre" in err
    assert "Set GINKGO_DB to a local path" in err
