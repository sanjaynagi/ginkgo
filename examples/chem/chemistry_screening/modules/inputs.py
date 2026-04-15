"""Input and scoring tasks for the chemistry screening example."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from ginkgo import file, table, task
from ginkgo.core.wrappers import TableResult


@task()
def load_compound_panel(panel_path: file) -> pd.DataFrame:
    """Load the compound screening panel.

    Parameters
    ----------
    panel_path : file
        CSV file containing compound assay measurements.

    Returns
    -------
    pandas.DataFrame
        Screening panel with one row per compound.
    """
    return pd.read_csv(panel_path)


@task()
def annotate_compounds(
    compounds: pd.DataFrame,
    potency_floor_nM: float,
    solubility_floor_uM: float,
    permeability_floor: float,
    max_clearance: float,
) -> TableResult:
    """Compute screening annotations used by downstream portfolio reviews.

    Parameters
    ----------
    compounds : pandas.DataFrame
        Screening panel records.
    potency_floor_nM : float
        Maximum IC50 threshold for advancement.
    solubility_floor_uM : float
        Minimum acceptable solubility threshold.
    permeability_floor : float
        Minimum acceptable permeability score.
    max_clearance : float
        Maximum acceptable hepatocyte clearance.

    Returns
    -------
    TableResult
        Wrapped annotated table registered as
        ``annotate_compounds.annotated_compounds`` in the asset catalog;
        downstream tasks receive the rehydrated DataFrame transparently.
    """
    annotated = compounds.copy()

    # Normalize core potency and developability signals once for downstream reuse.
    annotated["pIC50"] = annotated["assay_ic50_nM"].map(
        lambda value: round(9.0 - math.log10(float(value)), 3)
    )
    annotated["exposure_margin"] = (annotated["solubility_uM"] / annotated["assay_ic50_nM"]).round(
        3
    )
    annotated["meets_potency"] = annotated["assay_ic50_nM"] <= potency_floor_nM
    annotated["meets_solubility"] = annotated["solubility_uM"] >= solubility_floor_uM
    annotated["meets_permeability"] = annotated["permeability_score"] >= permeability_floor
    annotated["meets_clearance"] = annotated["hepatocyte_clearance"] <= max_clearance
    annotated["cyp3a4_risk"] = annotated["cyp3a4_risk"].str.lower()
    annotated["risk_penalty"] = annotated["cyp3a4_risk"].map(
        {"low": 0.0, "medium": 0.4, "high": 0.9}
    )
    annotated["developability_score"] = (
        annotated["meets_potency"].astype(int) * 3.0
        + annotated["meets_solubility"].astype(int) * 2.0
        + annotated["meets_permeability"].astype(int) * 2.0
        + annotated["meets_clearance"].astype(int) * 2.0
        + annotated["exposure_margin"]
        - annotated["risk_penalty"]
        - annotated["synthesis_steps"] * 0.1
    ).round(3)
    annotated["advance_recommendation"] = annotated.apply(
        lambda row: (
            "advance"
            if row["meets_potency"]
            and row["meets_solubility"]
            and row["meets_permeability"]
            and row["meets_clearance"]
            and row["cyp3a4_risk"] != "high"
            else "watch"
        ),
        axis=1,
    )
    sorted_annotated = annotated.sort_values(
        ["series", "developability_score", "compound_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    return table(sorted_annotated, name="annotated_compounds")


@task()
def write_developability_matrix(compounds: pd.DataFrame) -> file:
    """Write the compound-level developability matrix.

    Parameters
    ----------
    compounds : pandas.DataFrame
        Annotated screening table.

    Returns
    -------
    file
        CSV export of compound-level screening decisions.
    """
    matrix = compounds[
        [
            "compound_id",
            "series",
            "assay_ic50_nM",
            "solubility_uM",
            "permeability_score",
            "hepatocyte_clearance",
            "pIC50",
            "exposure_margin",
            "developability_score",
            "advance_recommendation",
        ]
    ].copy()
    output = Path("results/developability_matrix.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    matrix.to_csv(output, index=False)
    return file(str(output))
