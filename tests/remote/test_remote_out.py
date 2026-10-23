"""Functional tests for remote round trips: payload transport and ``Out[...]`` parameters.

Exercises the full round trip through a fake ``RemoteExecutor`` that runs
``ginkgo.remote.worker.run_worker_payload`` in-process against a distinct
"pod" scratch directory and a local-directory-backed fake ``ObjectStore`` —
close enough to a real backend to prove worker-local path rewriting, staging
the written output back, and restoring it at the driver's declared path all
actually work, not just that they typecheck.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ginkgo import Out, file, folder, task
from ginkgo.core.remote import remote_file
from ginkgo.remote.staging import RemoteIdentityError
from ginkgo.core.hashing import hash_bytes
from ginkgo.runtime.events import TaskAnnotated, TaskStaging
from ginkgo.runtime.remote_executor import (
    RemoteJobHandle,
)
from tests.conftest import EventCollector
from tests.remote_fakes import RemoteRoundTrip, RoundTripExecutor, evaluate_remotely


pytestmark = pytest.mark.usefixtures("remote_round_trip")


# ---------------------------------------------------------------------------
# Module-level tasks
# ---------------------------------------------------------------------------


@task(remote=True)
def remote_write_output(payload: str, out_path: Out[file]) -> file:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return file(out_path)


@task(remote=True)
def remote_write_output_untracked_return(payload: str, out_path: Out[file]) -> int:
    """Writes its ``Out[file]`` but returns something else entirely.

    Isolates the ``Out[...]`` path from the return-value artifact-restoration
    machinery — a ``-> file`` return of the same path would let a cache hit
    heal the deleted file from CAS, masking the ``Out[...]`` presence check
    this is meant to exercise. Mirrors ``tests/runtime/test_out_annotation.py``.
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return len(payload)


@task(remote=True)
def remote_write_output_dir(payload: str, out_dir: Out[folder]) -> folder:
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    (path / "a.txt").write_text(payload, encoding="utf-8")
    return folder(out_dir)


@task(remote=True)
def remote_write_output_list(payloads: list[str], out_paths: Out[list[file]]) -> list[file]:
    for text, out_path in zip(payloads, out_paths):
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return [file(p) for p in out_paths]


@task(remote=True)
def remote_maybe_write(payload: str | None, out_path: Out[file | None]) -> file | None:
    if out_path is None:
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload or "", encoding="utf-8")
    return file(out_path)


@task(remote=True)
def remote_write_output_inferred(payload: str, out_path: Out[file]):
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


@task(remote=True)
def remote_forgetful(marker: file, out_path: Out[file]) -> file:
    """Declares an output but never writes it."""
    return marker


@task()
def read_file_content(*, path: file) -> str:
    return Path(str(path)).read_text(encoding="utf-8").strip()


@task(remote=True)
def remote_chatty(value: int) -> int:
    print("hello from the pod")
    print("a warning", file=sys.stderr)
    return value + 1


@task(remote=True)
def remote_double(frame: pd.DataFrame) -> pd.DataFrame:
    return frame * 2


@task(remote=True)
def remote_read(path: file) -> str:
    return Path(str(path)).read_text(encoding="utf-8")


@task(remote=True)
def remote_read_both(direct: file, staged: file) -> str:
    return Path(str(direct)).read_text(encoding="utf-8") + Path(str(staged)).read_text(
        encoding="utf-8"
    )


@task(remote=True, retries=2)
def remote_read_with_retries(path: file) -> str:
    return Path(str(path)).read_text(encoding="utf-8")


@task(remote=True)
def remote_fails() -> int:
    raise ValueError("boom in the pod")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRemotePayloadTransport:
    def test_task_output_goes_to_the_pods_own_streams(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert evaluate_remotely(remote_chatty(value=1)) == 2

        # Not to log files at the driver's paths, which only the pod would see.
        captured = capsys.readouterr()
        assert "hello from the pod" in captured.out
        assert "a warning" in captured.err

    def test_failing_task_traceback_goes_to_the_pods_stderr(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(ValueError, match="boom in the pod"):
            evaluate_remotely(remote_fails())

        # Writing the traceback must leave the process's stderr open.
        assert "Traceback" in capsys.readouterr().err

    def test_large_dataframe_crosses_both_ways(self) -> None:
        # Random floats keep the encoded frame well over the inline limit.
        frame = pd.DataFrame({"x": np.random.default_rng(0).random(100_000)})

        result = evaluate_remotely(remote_double(frame=frame))

        pd.testing.assert_frame_equal(result, frame * 2)


class TestRemoteOutFile:
    def test_out_file_lands_at_driver_path(self, tmp_path: Path) -> None:
        out_path = tmp_path / "results" / "x.txt"
        result = evaluate_remotely(
            remote_write_output(payload="hello", out_path=str(out_path)),
        )
        assert Path(str(result)) == out_path
        assert out_path.read_text(encoding="utf-8") == "hello"

    def test_downstream_local_consumer_reads_it(self, tmp_path: Path) -> None:
        out_path = tmp_path / "produced.txt"
        upstream = remote_write_output(payload="downstream content", out_path=str(out_path))
        result = evaluate_remotely(read_file_content(path=upstream))
        assert result == "downstream content"

    def test_second_run_is_a_cache_hit(self, tmp_path: Path) -> None:
        out_path = tmp_path / "cached.txt"
        evaluate_remotely(
            remote_write_output(payload="v1", out_path=str(out_path)),
        )

        collector = EventCollector()
        evaluate_remotely(
            remote_write_output(payload="v1", out_path=str(out_path)),
            event_bus=collector.bus,
        )
        assert collector.cached()

    def test_a_deleted_output_is_restored_on_a_hit(self, tmp_path: Path) -> None:
        out_path = tmp_path / "deleted.txt"
        evaluate_remotely(
            remote_write_output_untracked_return(payload="v1", out_path=str(out_path)),
        )
        out_path.unlink()

        collector = EventCollector()
        evaluate_remotely(
            remote_write_output_untracked_return(payload="v1", out_path=str(out_path)),
            event_bus=collector.bus,
        )
        assert collector.cached()
        assert out_path.read_text(encoding="utf-8") == "v1"

    def test_task_that_fails_to_write_its_output_fails_clearly(self, tmp_path: Path) -> None:
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")
        missing = tmp_path / "never_written.txt"

        with pytest.raises(FileNotFoundError, match="was not written"):
            evaluate_remotely(
                remote_forgetful(marker=file(str(marker)), out_path=str(missing)),
            )

    def test_an_earlier_jobs_output_on_the_worker_does_not_count(self, tmp_path: Path) -> None:
        """Each dispatch writes Out paths into its own worker scratch directory.

        Both outputs share a basename, so with one shared scratch directory the
        first job's file would sit exactly where the second job's unwritten
        output is looked for, and the second job would wrongly succeed.
        """
        first = tmp_path / "a" / "out.txt"
        evaluate_remotely(remote_write_output(payload="first", out_path=str(first)))
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="was not written"):
            evaluate_remotely(
                remote_forgetful(
                    marker=file(str(marker)), out_path=str(tmp_path / "b" / "out.txt")
                ),
            )

    def test_the_worker_removes_its_scratch_outputs_after_the_dispatch(
        self, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.txt"
        evaluate_remotely(remote_write_output(payload="x", out_path=str(out)))

        assert out.read_text(encoding="utf-8") == "x"
        assert list((tmp_path / "pod-scratch" / "ginkgo-outputs").iterdir()) == []


class TestRemoteOutFolder:
    def test_out_folder_lands_at_driver_path(self, tmp_path: Path) -> None:
        out_dir = tmp_path / "qc"
        result = evaluate_remotely(
            remote_write_output_dir(payload="folder content", out_dir=str(out_dir)),
        )
        assert Path(str(result)) == out_dir
        assert (out_dir / "a.txt").read_text(encoding="utf-8") == "folder content"


class TestRemoteOutList:
    def test_out_list_of_files_all_land_at_their_driver_paths(self, tmp_path: Path) -> None:
        paths = [str(tmp_path / f"item-{i}.txt") for i in range(3)]
        result = evaluate_remotely(
            remote_write_output_list(payloads=["a", "b", "c"], out_paths=paths),
        )
        assert [str(f) for f in result] == paths
        for path, expected in zip(paths, ["a", "b", "c"]):
            assert Path(path).read_text(encoding="utf-8") == expected


class TestRemoteOutOptional:
    def test_absent_optional_output_produces_nothing(self, tmp_path: Path) -> None:
        result = evaluate_remotely(
            remote_maybe_write(payload=None, out_path=None),
        )
        assert result is None

    def test_present_optional_output_lands_at_driver_path(self, tmp_path: Path) -> None:
        out_path = tmp_path / "present.txt"
        result = evaluate_remotely(
            remote_maybe_write(payload="hi", out_path=str(out_path)),
        )
        assert Path(str(result)) == out_path
        assert out_path.read_text(encoding="utf-8") == "hi"


class TestRemoteOutInferredReturn:
    def test_inferred_return_from_out_param(self, tmp_path: Path) -> None:
        out_path = tmp_path / "inferred.txt"
        result = evaluate_remotely(
            remote_write_output_inferred(payload="inferred", out_path=str(out_path)),
        )
        assert Path(str(result)) == out_path
        assert out_path.read_text(encoding="utf-8") == "inferred"

    def test_named_output_access(self, tmp_path: Path) -> None:
        out_path = tmp_path / "named.txt"
        call = remote_write_output(payload="named", out_path=str(out_path))
        result = evaluate_remotely(call.output["out_path"])
        assert Path(str(result)) == out_path


class TestDirectRemoteInputs:
    """A remote task reads its remote inputs itself, straight from the source."""

    def _put_source(self, round_trip: RemoteRoundTrip, content: bytes) -> str:
        return round_trip.store.put(bucket="data-bucket", key="in.txt", data=content)

    def _artifact_blobs(self, round_trip: RemoteRoundTrip) -> list[bytes]:
        artifacts = round_trip.store.root / "test-bucket"
        return [path.read_bytes() for path in artifacts.rglob("*") if path.is_file()]

    def test_the_driver_neither_downloads_nor_uploads_a_direct_input(
        self, remote_round_trip: RemoteRoundTrip
    ) -> None:
        uri = self._put_source(remote_round_trip, b"from the source")

        assert evaluate_remotely(remote_read(path=remote_file(uri))) == "from the source"

        assert not (Path.cwd() / ".ginkgo" / "staging").exists()
        assert b"from the source" not in self._artifact_blobs(remote_round_trip)

    def test_staging_reports_direct_access(self, remote_round_trip: RemoteRoundTrip) -> None:
        uri = self._put_source(remote_round_trip, b"from the source")
        collector = EventCollector()

        evaluate_remotely(remote_read(path=remote_file(uri)), event_bus=collector.bus)

        staging = [event for event in collector.events if isinstance(event, TaskStaging)]
        assert [event.access_method for event in staging] == ["direct"]

    def test_an_input_marked_stage_goes_through_the_driver(
        self, remote_round_trip: RemoteRoundTrip
    ) -> None:
        uri = self._put_source(remote_round_trip, b"staged by the driver")

        result = evaluate_remotely(remote_read(path=remote_file(uri, access="stage")))

        assert result == "staged by the driver"
        assert (Path.cwd() / ".ginkgo" / "staging").exists()
        assert b"staged by the driver" in self._artifact_blobs(remote_round_trip)

    def test_a_change_before_the_worker_reads_fails_without_retries(
        self, remote_round_trip: RemoteRoundTrip, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uri = self._put_source(remote_round_trip, b"before")
        submits = 0
        real_submit = RoundTripExecutor.submit

        def overwrite_then_submit(self, *, attempt: dict) -> RemoteJobHandle:
            nonlocal submits
            submits += 1
            remote_round_trip.store.put(bucket="data-bucket", key="in.txt", data=b"after")
            return real_submit(self, attempt=attempt)

        monkeypatch.setattr(RoundTripExecutor, "submit", overwrite_then_submit)

        with pytest.raises(RemoteIdentityError, match="changed after its cache key"):
            evaluate_remotely(remote_read_with_retries(path=remote_file(uri)))
        assert submits == 1

    def test_the_worker_records_the_digest_of_a_direct_input(
        self, remote_round_trip: RemoteRoundTrip
    ) -> None:
        uri = self._put_source(remote_round_trip, b"from the source")
        collector = EventCollector()

        evaluate_remotely(remote_read(path=remote_file(uri)), event_bus=collector.bus)

        assert _recorded_input_digests(collector) == {uri: hash_bytes(b"from the source")}

    def test_driver_and_worker_digests_are_recorded_together(
        self, remote_round_trip: RemoteRoundTrip
    ) -> None:
        direct_uri = self._put_source(remote_round_trip, b"direct")
        staged_uri = remote_round_trip.store.put(
            bucket="data-bucket", key="staged.txt", data=b"staged"
        )
        collector = EventCollector()

        result = evaluate_remotely(
            remote_read_both(
                direct=remote_file(direct_uri), staged=remote_file(staged_uri, access="stage")
            ),
            event_bus=collector.bus,
        )

        assert result == "directstaged"
        assert _recorded_input_digests(collector) == {
            direct_uri: hash_bytes(b"direct"),
            staged_uri: hash_bytes(b"staged"),
        }


def _recorded_input_digests(collector: EventCollector) -> dict[str, str]:
    """Return the last ``remote_input_digests`` annotation the run recorded."""
    recorded = [
        event.fields["remote_input_digests"]
        for event in collector.events
        if isinstance(event, TaskAnnotated) and "remote_input_digests" in event.fields
    ]
    return recorded[-1]
