# Documentation Stack

End-user documentation is an [Astro Starlight](https://starlight.astro.build/)
site under `docs/site/`.

- Pages are Markdown under `docs/site/src/content/docs/`. The sidebar is set in
  `docs/site/astro.config.mjs`, and the theme in `docs/site/src/styles/ginkgo.css`.
- The site root is the forest home page, `docs/site/public/index.html`, a static
  page served as is. Images and the walkthrough video are under
  `docs/site/public/_static/`.
- Internal links are absolute and include the `/ginkgo` base, so the
  `starlight-links-validator` plugin can check every page and anchor at build
  time. A broken link fails the build.
- `pixi run docs-build` installs the Node packages and writes the site to
  `docs/site/dist/`. `pixi run docs-serve` serves it with live reload. Node
  comes from the Pixi environment.

This published docs site is intentionally separate from the repository's
internal implementation plans and historical notes, which remain under `docs/`
as development artifacts rather than end-user pages.
