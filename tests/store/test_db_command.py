"""``ginkgo db``: the maintenance surface for the ledger."""

from __future__ import annotations

import json

from ginkgo.cli.app import main
from ginkgo.core.asset import AssetKey, AssetRef, AssetVersion
from ginkgo.formatting import now_iso
from ginkgo.remote.staging import StagingCache, StagingEntry
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.value_codec import encode_value
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.schema import SCHEMA_VERSION
from ginkgo.store import sqlite as sqlite_module
from ginkgo.store.sqlite import open_store


def _run_row(tmp_path, run_id: str, *, finished_at: str | None = None, events: int = 0) -> None:
    """Record one run row, and optionally some events belonging to it."""
    with open_store(tmp_path / ".ginkgo" / "ginkgo.db") as store, store.transaction():
        store.apply(
            [
                ProjectionOp(
                    sql="INSERT INTO runs (run_id, workflow, status, started_at, finished_at) "
                    "VALUES (?, 'flow.py', 'succeeded', '2020-01-01T00:00:00+00:00', ?)",
                    params=(run_id, finished_at),
                ),
                *(
                    ProjectionOp(
                        sql="INSERT INTO events (run_id, ts, type, v, payload) "
                        "VALUES (?, '2020-01-01T00:00:00+00:00', 'task_started', 1, '{}')",
                        params=(run_id,),
                    )
                    for _ in range(events)
                ),
            ]
        )


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


_REPLAYED_REF = AssetRef(
    key=AssetKey(namespace="table", name="a"),
    version_id="7f" * 32,
    kind="table",
    artifact_id="artifact-a",
    content_hash="hash-a",
    artifact_path="/artifacts/artifact-a",
)
"""The asset reference a cache entry hands back to whoever consumes it."""


def _asset_output(tmp_path, cache_key: str, ref: AssetRef) -> None:
    """Give an entry the stored output of a task that produced one asset."""
    entry_dir = tmp_path / ".ginkgo" / "cache" / cache_key
    payload = encode_value(ref, base_dir=entry_dir)
    (entry_dir / "output.json").write_text(json.dumps(payload), encoding="utf-8")


def _catalogue(tmp_path, ref: AssetRef) -> None:
    """Register the catalog row the entry's ref names."""
    with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
        AssetStore.attached_to(index).register_version(
            version=AssetVersion(
                key=ref.key,
                version_id=ref.version_id,
                kind=ref.kind,
                artifact_id=ref.artifact_id,
                content_hash=ref.content_hash,
                run_id="run-1",
                producer_task="pkg.produce",
                created_at=now_iso(),
            )
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

    def test_an_entry_replaying_an_uncatalogued_version_is_reported(
        self, tmp_path, monkeypatch, capsys
    ):
        """A catalog lost behind an intact cache is diagnosable (issue #263)."""
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1")
        _asset_output(tmp_path, "key-1", _REPLAYED_REF)

        assert main(["db", "check"]) == 1

        output = capsys.readouterr().out
        assert "1 asset version(s) a cache entry replays have no catalog row" in output
        assert "table:a" in output

    def test_an_entry_whose_versions_are_catalogued_passes(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _entry(tmp_path, "key-1")
        _asset_output(tmp_path, "key-1", _REPLAYED_REF)
        _catalogue(tmp_path, _REPLAYED_REF)

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
        main(["db", "migrate"])
        capsys.readouterr()

        assert main(["db", "check"]) == 0

        output = capsys.readouterr().out
        assert f"Schema version: {SCHEMA_VERSION}" in output
        assert "integrity check passed" in output

    def test_check_on_an_empty_workspace_creates_nothing(self, tmp_path, monkeypatch, capsys):
        """Creating the database is migrate's job, and a run's. Not a check's."""
        monkeypatch.chdir(tmp_path)

        assert main(["db", "check"]) == 0

        assert "nothing has run in this workspace" in capsys.readouterr().out
        assert not (tmp_path / ".ginkgo").exists()

    def test_check_never_opens_a_write_connection(self, tmp_path, monkeypatch):
        """A read path must not be able to migrate a database out from under a run."""
        monkeypatch.chdir(tmp_path)
        main(["db", "migrate"])
        opened: list[bool] = []
        real_open = sqlite_module.SqliteStore.open

        def _record(cls, path, *, readonly=False, thread_shared=False):
            opened.append(readonly)
            return real_open(path, readonly=readonly, thread_shared=thread_shared)

        monkeypatch.setattr(sqlite_module.SqliteStore, "open", classmethod(_record))

        assert main(["db", "check"]) == 0
        assert opened and all(opened)

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


class TestDbCheckWorkspace:
    """``db check`` looks at every index and the bytes beside it."""

    def test_a_run_without_its_directory_is_reported(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1")

        assert main(["db", "check"]) == 1
        assert "run run-1 has a row but no run directory" in capsys.readouterr().out

    def test_a_run_directory_without_a_row_is_reported(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        main(["db", "migrate"])
        (tmp_path / ".ginkgo" / "runs" / "run-2").mkdir(parents=True)

        assert main(["db", "check"]) == 1
        assert "run directory run-2 has no row (orphan)" in capsys.readouterr().out

    def test_a_run_with_its_directory_passes(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1")
        (tmp_path / ".ginkgo" / "runs" / "run-1").mkdir(parents=True)

        assert main(["db", "check"]) == 0
        assert "integrity check passed" in capsys.readouterr().out

    def test_a_staged_uri_without_its_bytes_is_reported(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        cache = StagingCache(root=tmp_path / ".ginkgo" / "staging")
        cache.index.record(
            StagingEntry(
                uri="s3://bucket/key",
                digest="deadbeef",
                etag=None,
                version_id=None,
                size=4,
                staged_at="2026-08-28T10:00:00+00:00",
                blob_path="blobs/deadbeef",
            )
        )
        cache.close()

        assert main(["db", "check"]) == 1
        assert "staged s3://bucket/key has a row but no bytes" in capsys.readouterr().out

    def test_an_orphan_blob_is_reported(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        main(["db", "migrate"])
        blobs = tmp_path / ".ginkgo" / "artifacts" / "blobs"
        blobs.mkdir(parents=True)
        (blobs / "abc123").write_bytes(b"orphaned")

        assert main(["db", "check"]) == 1
        assert "artifact file blobs/abc123 has no row (orphan)" in capsys.readouterr().out

    def test_an_environment_that_materialized_two_ways_is_reported(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.chdir(tmp_path)
        with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
            index.record_env_materialization(
                env_hash="env-1", host="alpha", materialized_digest="digest-a"
            )
            index.record_env_materialization(
                env_hash="env-1", host="beta", materialized_digest="digest-b"
            )

        assert main(["db", "check"]) == 1
        assert "environment env-1 materialized 2 different ways" in capsys.readouterr().out

    def test_one_host_reinstalling_is_not_drift_between_hosts(self, tmp_path, monkeypatch, capsys):
        """One row per host: the latest observation replaces the previous one."""
        monkeypatch.chdir(tmp_path)
        with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
            for digest in ("digest-a", "digest-b"):
                index.record_env_materialization(
                    env_hash="env-1", host="alpha", materialized_digest=digest
                )
            assert [row["materialized_digest"] for row in index.env_materializations()] == [
                "digest-b"
            ]

        assert main(["db", "check"]) == 0
        assert "integrity check passed" in capsys.readouterr().out


class TestDbPrune:
    """``db prune`` deletes history the projections do not need."""

    def test_events_of_a_finished_run_are_deleted(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1", finished_at="2020-01-01T00:00:00+00:00", events=3)

        assert main(["db", "prune", "--events-older-than", "1d"]) == 0
        assert "deleted 3 event rows" in capsys.readouterr().out

        with open_store(tmp_path / ".ginkgo" / "ginkgo.db") as store:
            assert store.query("SELECT count(*) AS n FROM events")[0]["n"] == 0
            # The projection the events fed is untouched.
            assert store.query("SELECT count(*) AS n FROM runs")[0]["n"] == 1

    def test_an_unfinished_run_keeps_its_events(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1", finished_at=None, events=2)

        assert main(["db", "prune", "--events-older-than", "1d"]) == 0
        assert "deleted 0 event rows" in capsys.readouterr().out

    def test_a_recent_run_keeps_its_events(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1", finished_at=now_iso(), events=2)

        assert main(["db", "prune", "--events-older-than", "30d"]) == 0
        assert "deleted 0 event rows" in capsys.readouterr().out

    def test_a_dry_run_counts_without_deleting(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1", finished_at="2020-01-01T00:00:00+00:00", events=3)

        assert main(["db", "prune", "--events-older-than", "1d", "--dry-run"]) == 0
        assert "would delete 3 event rows" in capsys.readouterr().out
        with open_store(tmp_path / ".ginkgo" / "ginkgo.db") as store:
            assert store.query("SELECT count(*) AS n FROM events")[0]["n"] == 3

    def test_the_digest_memo_is_pruned_on_last_seen(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        with open_store(tmp_path / ".ginkgo" / "ginkgo.db") as store, store.transaction():
            store.apply(
                [
                    ProjectionOp(
                        sql="INSERT INTO digest_memo (kind, fingerprint, digest, last_seen) "
                        "VALUES (?, ?, ?, ?)",
                        params=("file", f"fp-{n}", "d", stamp),
                    )
                    for n, stamp in enumerate(("2020-01-01T00:00:00+00:00", now_iso()))
                ]
            )

        assert main(["db", "prune", "--digest-memo-older-than", "1d"]) == 0
        assert "deleted 1 digest memo row" in capsys.readouterr().out

    def test_naming_no_cutoff_is_an_error(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "prune"]) == 1
        assert "provide --events-older-than" in capsys.readouterr().out

    def test_an_unparseable_duration_is_an_error(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        assert main(["db", "prune", "--events-older-than", "soon"]) == 1
        assert "Invalid duration for --events-older-than" in capsys.readouterr().out


class TestDbVacuum:
    """``db vacuum`` rebuilds the file."""

    def test_vacuum_reports_the_size_either_side(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _run_row(tmp_path, "run-1", finished_at="2020-01-01T00:00:00+00:00", events=2000)
        main(["db", "prune", "--events-older-than", "1d"])
        capsys.readouterr()

        assert main(["db", "vacuum"]) == 0
        assert "→" in capsys.readouterr().out

    def test_vacuum_says_so_when_nothing_was_reclaimed(self, tmp_path, monkeypatch, capsys):
        """An unchanged size is the only signal SQLite gives that it did nothing."""
        monkeypatch.chdir(tmp_path)
        main(["db", "migrate"])
        main(["db", "vacuum"])
        capsys.readouterr()

        assert main(["db", "vacuum"]) == 0
        assert "no space reclaimed" in capsys.readouterr().out

    def test_a_relocated_staging_root_is_not_reported_as_missing_bytes(
        self, tmp_path, monkeypatch, capsys
    ):
        """`db check` must find the staged bytes wherever they were configured."""
        elsewhere = tmp_path / "scratch" / "staging"
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GINKGO_STAGING_ROOT", str(elsewhere))
        cache = StagingCache(db_path=tmp_path / ".ginkgo" / "ginkgo.db")
        (elsewhere / "blobs").mkdir(parents=True, exist_ok=True)
        (elsewhere / "blobs" / "deadbeef").write_bytes(b"bytes")
        cache.index.record(
            StagingEntry(
                uri="s3://bucket/key",
                digest="deadbeef",
                etag=None,
                version_id=None,
                size=5,
                staged_at="2026-08-28T10:00:00+00:00",
                blob_path="blobs/deadbeef",
            )
        )
        cache.close()

        assert main(["db", "check"]) == 0
        assert "integrity check passed" in capsys.readouterr().out
