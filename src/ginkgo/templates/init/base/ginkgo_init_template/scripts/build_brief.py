"""Build one Markdown brief for the starter workflow."""

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    """Render one brief from a normalized seed card."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--item", required=True)
    parser.add_argument("--normalized-card", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    normalized_text = Path(args.normalized_card).read_text(encoding="utf-8").strip()
    output = Path(args.output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() or output.is_symlink():
        output.unlink()
    output.write_text(
        "\n".join(
            [
                f"# Brief: {args.item}",
                "",
                "## Normalized Seed",
                "",
                "```text",
                normalized_text,
                "```",
                "",
                "## Notes",
                "",
                "- Built by the Pixi-backed script task.",
                "- Used later by the Docker packaging task.",
                "",
            ]
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
