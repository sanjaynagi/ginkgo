"""Canonical workflow entrypoint for the chemistry screening example."""

from __future__ import annotations

import ginkgo
from ginkgo import file, flow

from chemistry_screening.modules.delivery import write_delivery_manifest
from chemistry_screening.modules.inputs import (
    annotate_compounds,
    load_compound_panel,
    write_developability_matrix,
)
from chemistry_screening.modules.reports import (
    build_portfolio_summary,
    plan_series_packets,
    write_candidate_register,
)


cfg = ginkgo.config("ginkgo.toml")


@flow
def main() -> file:
    """Build a chemistry screening review bundle.

    Parameters
    ----------
    None

    Returns
    -------
    file
        Artifact manifest for chemistry portfolio review.
    """
    compounds = load_compound_panel(panel_path=cfg["paths"]["compound_panel_csv"])
    annotated = annotate_compounds(
        compounds=compounds,
        potency_floor_nM=float(cfg["screening"]["potency_floor_nM"]),
        solubility_floor_uM=float(cfg["screening"]["solubility_floor_uM"]),
        permeability_floor=float(cfg["screening"]["permeability_floor"]),
        max_clearance=float(cfg["screening"]["max_clearance"]),
    )
    developability_matrix = write_developability_matrix(compounds=annotated)
    series_packets = plan_series_packets(compounds=annotated)
    candidate_register = write_candidate_register(
        compounds=annotated,
        series_packets=series_packets,
    )
    portfolio_summary = build_portfolio_summary(
        developability_matrix=developability_matrix,
        candidate_register=candidate_register,
        series_packets=series_packets,
    )
    return write_delivery_manifest(
        portfolio_summary=portfolio_summary,
        developability_matrix=developability_matrix,
        candidate_register=candidate_register,
        series_packets=series_packets,
    )
