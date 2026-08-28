"""``ginkgo db``: the maintenance surface for the ledger."""

from __future__ import annotations

from ginkgo.cli.app import main


class TestDbCommands:
    """Each subcommand, run against a workspace that starts out empty."""

    def test_migrate_creates_the_database(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "migrate"]) == 0
        assert (tmp_path / ".ginkgo" / "ginkgo.db").exists()
        assert "schema version 1" in capsys.readouterr().out

    def test_check_reports_the_version_and_the_integrity_result(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "check"]) == 0

        output = capsys.readouterr().out
        assert "Schema version: 1" in output
        assert "integrity check passed" in output

    def test_path_prints_where_the_database_lives(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "path"]) == 0

        assert capsys.readouterr().out.strip() == ".ginkgo/ginkgo.db"
        assert not (tmp_path / ".ginkgo").exists()

    def test_the_database_environment_override_is_honoured(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        elsewhere = tmp_path / "local" / "ledger.db"
        monkeypatch.setenv("GINKGO_DB", str(elsewhere))

        assert main(["db", "migrate"]) == 0

        assert elsewhere.exists()
        assert not (tmp_path / ".ginkgo").exists()
