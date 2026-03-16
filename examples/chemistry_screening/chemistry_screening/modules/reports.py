"""Reporting tasks for the chemistry screening example."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ginkgo import ExprList, file, task


def _safe_slug(value: str) -> str:
    """Return a file-safe slug for series names."""
    return "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")


@task()
def write_series_packet(
    series: str,
    compounds: list[dict[str, object]],
) -> file:
    """Render a chemistry review packet for one chemotype series.

    Parameters
    ----------
    series : str
        Chemistry series name.
    compounds : list[dict[str, object]]
        Compound records for the selected series.

    Returns
    -------
    file
        Markdown packet summarizing the series.
    """
    ordered = sorted(
        compounds,
        key=lambda item: (float(item["developability_score"]), str(item["compound_id"])),
        reverse=True,
    )
    lines = [f"# {series.title()} Series Packet", "", "## Ranked Compounds"]
    for item in ordered:
        lines.append(
            (
                f"- {item['compound_id']}: score={item['developability_score']} "
                f"recommendation={item['advance_recommendation']} "
                f"pIC50={item['pIC50']} exposure_margin={item['exposure_margin']}"
            )
        )

    output = Path(f"results/series/{_safe_slug(series)}_packet.md")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))


@task()
def plan_series_packets(compounds: pd.DataFrame) -> list[file]:
    """Expand to one chemistry packet per discovered series.

    Parameters
    ----------
    compounds : pandas.DataFrame
        Annotated screening table.

    Returns
    -------
    list[file]
        Markdown packets for each discovered chemistry series.
    """
    exprs = []

    # Series membership is data-driven, so packet expansion is determined at runtime.
    for series, group in sorted(compounds.groupby("series"), key=lambda item: item[0]):
        records = (
            group[
                [
                    "compound_id",
                    "pIC50",
                    "exposure_margin",
                    "developability_score",
                    "advance_recommendation",
                ]
            ]
            .sort_values(["developability_score", "compound_id"], ascending=[False, True])
            .to_dict(orient="records")
        )
        exprs.append(write_series_packet(series=series, compounds=records))

    return ExprList(exprs=exprs)


@task()
def write_candidate_register(compounds: pd.DataFrame, series_packets: list[file]) -> file:
    """Write the compound advancement register.

    Parameters
    ----------
    compounds : pandas.DataFrame
        Annotated screening table.
    series_packets : list[file]
        Series-level review packets.

    Returns
    -------
    file
        CSV with compounds selected for advancement or monitoring.
    """
    register = compounds.loc[
        compounds["advance_recommendation"].isin(["advance", "watch"])
    ][
        [
            "compound_id",
            "series",
            "developability_score",
            "advance_recommendation",
            "cyp3a4_risk",
        ]
    ].copy()
    register["series_packet_count"] = len(series_packets)
    register = register.sort_values(
        ["advance_recommendation", "developability_score", "compound_id"],
        ascending=[True, False, True],
    )

    output = Path("results/candidate_register.csv")
    register.to_csv(output, index=False)
    return file(str(output))


@task()
def build_portfolio_summary(
    developability_matrix: file,
    candidate_register: file,
    series_packets: list[file],
) -> file:
    """Assemble a portfolio-level chemistry summary.

    Parameters
    ----------
    developability_matrix : file
        CSV export of compound-level developability signals.
    candidate_register : file
        CSV export of advancement recommendations.
    series_packets : list[file]
        Series-specific chemistry review packets.

    Returns
    -------
    file
        Markdown summary for portfolio review.
    """
    matrix = pd.read_csv(developability_matrix)
    register = pd.read_csv(candidate_register)

    series_summary = (
        matrix.groupby("series", as_index=False)
        .agg(
            compounds=("compound_id", "count"),
            best_score=("developability_score", "max"),
            advancing=("advance_recommendation", lambda values: int((values == "advance").sum())),
        )
        .sort_values(["best_score", "series"], ascending=[False, True])
    )

    lines = [
        "# Chemistry Screening Portfolio Summary",
        "",
        f"Compounds reviewed: {len(matrix)}",
        f"Advancing compounds: {int((register['advance_recommendation'] == 'advance').sum())}",
        "",
        "## Series Overview",
        series_summary.to_string(index=False),
        "",
        "## Series Packets",
    ]
    for packet_path in series_packets:
        lines.append(f"- {packet_path}")

    output = Path("results/portfolio_summary.md")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))
