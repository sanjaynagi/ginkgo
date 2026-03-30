"""Workflow definition for the starter project."""

import ginkgo
from ginkgo import expand, flow

from ginkgo_init_template.modules.analysis import build_brief, package_brief, write_summary
from ginkgo_init_template.modules.prep import normalize_seed_card, write_seed_card
from ginkgo_init_template.modules.reporting import render_overview_notebook, write_delivery_manifest


cfg = ginkgo.config("ginkgo.toml")


@flow
def main():
    """Run the canonical starter workflow across one item axis."""
    items = list(cfg["items"])

    seed_paths = expand("results/seed/{item}.txt", item=items)
    normalized_paths = expand("results/normalized/{item}.txt", item=items)
    check_paths = expand("results/checks/{item}.sha", item=items)
    brief_paths = expand("results/briefs/{item}.md", item=items)
    package_paths = expand("results/packages/{item}.txt", item=items)

    seed_cards = write_seed_card().map(
        item=items,
        output_path=seed_paths,
    )

    # normalize_seed_card returns tuple[file, file]: (normalized_card, checksum).
    # Use .output[i] to select individual elements from the tuple result.
    norm_results = normalize_seed_card().map(
        seed_card=seed_cards,
        output_path=normalized_paths,
        check_path=check_paths,
    )
    normalized_cards = norm_results.output[0]
    checksums = norm_results.output[1]

    briefs = build_brief().map(
        item=items,
        normalized_card=normalized_cards,
        output_path=brief_paths,
    )
    packages = package_brief().map(
        brief=briefs,
        output_path=package_paths,
    )

    summary = write_summary(
        items=items,
        seed_paths=seed_paths,
        normalized_cards=normalized_cards,
        checksums=checksums,
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


__all__ = ["main"]
