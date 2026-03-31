"""Source manifest loading for benchmark inputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True, kw_only=True)
class BenchmarkSourceManifest:
    """Pinned source metadata for a benchmark dataset.

    Parameters
    ----------
    name : str
        Human-readable benchmark source name.
    repo : str
        Source GitHub repository in ``owner/name`` form.
    commit : str
        Immutable commit SHA pinned for the benchmark input set.
    metadata_url : str
        Raw URL for the upstream metadata table.
    reads_base_url : str
        Base raw URL used to construct paired read URLs.
    metadata_format : str
        Metadata file format identifier.
    sample_id_column : str
        Column name containing sample identifiers in the metadata table.
    read1_column : str
        Output column name for forward read paths.
    read2_column : str
        Output column name for reverse read paths.
    read1_pattern : str
        Filename template for forward reads.
    read2_pattern : str
        Filename template for reverse reads.
    sample_ids : tuple[str, ...]
        Selected sample identifiers for the benchmark dataset.
    """

    name: str
    repo: str
    commit: str
    metadata_url: str
    reads_base_url: str
    metadata_format: str
    sample_id_column: str
    read1_column: str
    read2_column: str
    read1_pattern: str
    read2_pattern: str
    sample_ids: tuple[str, ...]

    @classmethod
    def from_toml(cls, *, path: Path) -> BenchmarkSourceManifest:
        """Load a benchmark source manifest from TOML.

        Parameters
        ----------
        path : Path
            TOML file describing the pinned benchmark data source.

        Returns
        -------
        BenchmarkSourceManifest
            Parsed manifest.
        """
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        source = data["source"]
        sample_ids = tuple(data["samples"]["ids"])
        return cls(
            name=str(source["name"]),
            repo=str(source["repo"]),
            commit=str(source["commit"]),
            metadata_url=str(source["metadata_url"]),
            reads_base_url=str(source["reads_base_url"]).rstrip("/"),
            metadata_format=str(source["metadata_format"]),
            sample_id_column=str(source["sample_id_column"]),
            read1_column=str(source["read1_column"]),
            read2_column=str(source["read2_column"]),
            read1_pattern=str(source["read1_pattern"]),
            read2_pattern=str(source["read2_pattern"]),
            sample_ids=sample_ids,
        )
