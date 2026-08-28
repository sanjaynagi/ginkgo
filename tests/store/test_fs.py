"""Network-filesystem detection for the store's one-line warning."""

from __future__ import annotations

from pathlib import Path

from ginkgo.store import fs


class TestClassification:
    """Which mounts SQLite is warned about."""

    def test_a_temporary_directory_is_not_a_network_filesystem(self, tmp_path):
        assert fs.is_network_filesystem(tmp_path / "ginkgo.db") is None

    def test_a_path_that_does_not_exist_yet_is_still_classified(self, tmp_path):
        """The database is checked before it is created."""
        assert fs.is_network_filesystem(tmp_path / "missing" / "ginkgo.db") is None

    def test_an_nfs_mount_is_reported_with_its_type(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fs, "_mount_table", lambda: [("/", "apfs"), (str(tmp_path), "nfs4")])

        assert fs.is_network_filesystem(tmp_path / "ginkgo.db") == "nfs4"

    def test_the_longest_matching_mount_point_wins(self, tmp_path, monkeypatch):
        nested = tmp_path / "scratch"
        nested.mkdir()
        monkeypatch.setattr(
            fs, "_mount_table", lambda: [(str(tmp_path), "nfs4"), (str(nested), "ext4")]
        )

        assert fs.is_network_filesystem(nested / "ginkgo.db") is None


class TestMountTableParsing:
    """Both mount-table formats ginkgo reads."""

    def test_proc_mounts(self):
        text = "server:/vol /mnt/data nfs4 rw,relatime 0 0\n/dev/sda1 / ext4 rw 0 0\n"

        assert fs._parse_proc_mounts(text) == [("/mnt/data", "nfs4"), ("/", "ext4")]

    def test_bsd_mount_output(self):
        text = (
            "/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)\n"
            "//user@host/share on /Volumes/share (smbfs, nodev, nosuid, mounted by user)\n"
        )

        assert fs._parse_mount_output(text) == [
            ("/", "apfs"),
            ("/Volumes/share", "smbfs"),
        ]


class TestWarning:
    """The warning is one line, and only ever printed once."""

    def test_nothing_is_printed_for_a_local_path(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setattr(fs, "_warned", False)

        fs.warn_if_network_filesystem(tmp_path / "ginkgo.db")

        assert capsys.readouterr().err == ""

    def test_a_network_path_warns_once_per_process(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setattr(fs, "_warned", False)
        monkeypatch.setattr(fs, "is_network_filesystem", lambda path: "lustre")

        fs.warn_if_network_filesystem(Path("/scratch/.ginkgo/ginkgo.db"))
        fs.warn_if_network_filesystem(Path("/scratch/.ginkgo/ginkgo.db"))

        assert capsys.readouterr().err == (
            "⚠ .ginkgo is on lustre; SQLite locking may be unreliable. "
            "Set GINKGO_DB to a local path.\n"
        )
