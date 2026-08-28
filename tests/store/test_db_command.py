"""``ginkgo db``: the maintenance surface for the ledger."""

from __future__ import annotations

from ginkgo.cli.app import main
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.schema import SCHEMA_VERSION
from ginkgo.store.sqlite import open_store


def _entry(tmp_path, cache_key: str, *, with_bytes: bool = True) -> None:
    """Record one cache entry, optionally without the bytes it names."""
    if with_bytes:
        entry_dir = tmp_path / ".ginkgo" / "cache" / cache_key
        entry_dir.mkdir(parents=True)
        (entry_dir / "output.json").write_text("{}", encoding="utf-8")
    with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
        index.record_entry(
            cache_key=cache_key,
            meta={"function": "pkg.produce", "created_at": "2026-08-28T10:00:00+00:00"},
            artifact_ids={},
            size_bytes=2,
            run_id=None,
        )


class TestDbCheckCache:
    """``db check`` reports every way the index and the bytes disagree."""

    def test_a_row_without_its_output_is_reported(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1", with_bytes=False)

        assert main(["db", "check"]) == 1
        assert "cache entry key-1 has a row but no output.json" in capsys.readouterr().out

    def test_a_directory_without_a_row_is_reported_as_an_orphan(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1")
        orphan = tmp_path / ".ginkgo" / "cache" / "orphan-1"
        orphan.mkdir(parents=True)
        (orphan / "output.json").write_text("{}", encoding="utf-8")

        assert main(["db", "check"]) == 1
        assert "cache directory orphan-1 has no row (orphan)" in capsys.readouterr().out

    def test_a_cache_artifact_missing_from_the_store_is_reported(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1")
        with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
            index.record_artifact(
                ArtifactRecord(
                    artifact_id="artifact-1",
                    kind="blob",
                    digest_algorithm="blake3",
                    digest_hex="artifact-1",
                    extension=".txt",
                    size=4,
                    created_at="2026-08-28T10:00:00+00:00",
                    storage_backend="local",
                )
            )
        with open_store(tmp_path / ".ginkgo" / "ginkgo.db") as store:
            with store.transaction():
                store.apply(
                    [
                        ProjectionOp(
                            sql="INSERT INTO cache_artifacts (cache_key, path, artifact_id) "
                            "VALUES ('key-1', '/out/a.txt', 'artifact-1')",
                        )
                    ]
                )

        assert main(["db", "check"]) == 1
        assert "cache artifact artifact-1 is missing" in capsys.readouterr().out

    def test_a_consistent_cache_passes(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1")

        assert main(["db", "check"]) == 0
        assert "integrity check passed" in capsys.readouterr().out

    def test_a_save_in_flight_is_not_an_orphan(self, tmp_path, monkeypatch, capsys):
        """A concurrent save's temporary directory is about to be renamed."""
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1")
        in_flight = tmp_path / ".ginkgo" / "cache" / "key-2.tmp-abc123"
        in_flight.mkdir(parents=True)
        (in_flight / "output.json").write_text("{}", encoding="utf-8")

        assert main(["db", "check"]) == 0
        assert "orphan" not in capsys.readouterr().out


class TestDbCommands:
    """Each subcommand, run against a workspace that starts out empty."""

    def test_migrate_creates_the_database(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "migrate"]) == 0
        assert (tmp_path / ".ginkgo" / "ginkgo.db").exists()
        assert f"schema version {SCHEMA_VERSION}" in capsys.readouterr().out

    def test_check_reports_the_version_and_the_integrity_result(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "check"]) == 0

        output = capsys.readouterr().out
        assert f"Schema version: {SCHEMA_VERSION}" in output
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
