"""Pipeline wiring for the starter workflow."""

from __future__ import annotations

import ginkgo
from ginkgo import expand, flow

from ginkgo_init_template.modules.analysis import build_brief, package_brief, write_summary
from ginkgo_init_template.modules.prep import normalize_seed_card, write_seed_card
from ginkgo_init_template.modules.reporting import render_overview_notebook, write_delivery_manifest


cfg = ginkgo.config("ginkgo.toml")


def _job_axes() -> tuple[list[str], list[str]]:
    """Return the item and variant axes in expand-compatible order."""
    items = list(cfg["items"])
    variants = list(cfg["variants"])
    job_items = [item for item in items for _variant in variants]
    job_variants = [variant for _item in items for variant in variants]
    return job_items, job_variants


@flow
def main():
    """Run the canonical starter workflow across a small item matrix."""
    items = list(cfg["items"])
    variants = list(cfg["variants"])
    job_items, job_variants = _job_axes()

    seed_paths = expand("results/seed/{item}_{variant}.txt", item=items, variant=variants)
    normalized_paths = expand(
        "results/normalized/{item}_{variant}.txt",
        item=items,
        variant=variants,
    )
    brief_paths = expand("results/briefs/{item}_{variant}.md", item=items, variant=variants)
    package_paths = expand("results/packages/{item}_{variant}.txt", item=items, variant=variants)

    seed_cards = write_seed_card().map(
        item=job_items,
        variant=job_variants,
        output_path=seed_paths,
    )
    normalized_cards = normalize_seed_card().map(
        seed_card=seed_cards,
        output_path=normalized_paths,
    )
    briefs = build_brief().map(
        item=job_items,
        variant=job_variants,
        normalized_card=normalized_cards,
        output_path=brief_paths,
    )
    packages = package_brief().map(
        brief=briefs,
        output_path=package_paths,
    )

    summary = write_summary(
        items=job_items,
        variants=job_variants,
        seed_cards=seed_cards,
        normalized_cards=normalized_cards,
        briefs=briefs,
        packages=packages,
    )
    notebook_html = render_overview_notebook(
        summary_path=summary,
        run_label=str(cfg["run_label"]),
    )
    return write_delivery_manifest(
        summary_path=summary,
        notebook_html=notebook_html,
        package_reports=packages,
    )
