# Documentation Stack

End-user documentation lives in a dedicated Sphinx + MyST site under
`docs/site/`.

- Sphinx provides navigation, API reference generation, and local static-site builds.
- MyST keeps the authored pages in Markdown rather than splitting the docs
  stack between Markdown and reStructuredText.
- The local docs build is wired through Pixi with `pixi run docs-build`, which
  writes the site to `docs/_build/dirhtml/`.

This published docs site is intentionally separate from the repository's
internal implementation plans and historical notes, which remain under `docs/`
as development artifacts rather than end-user pages.
