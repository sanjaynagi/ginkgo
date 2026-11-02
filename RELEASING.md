# Releasing

Ginkgo and its eight first-party plugins release together at one version. Each
release's notes live on its GitHub release.

1. Set the version everywhere and relock:

   ```bash
   pixi run set-version 0.5.0
   ```

   This rewrites every package's version, every `ginkgo-*==` pin between them
   and the docs pin, then runs `pixi lock`.
2. Merge those changes to `main`.
3. Tag the merge commit and push the tag:

   ```bash
   git tag v0.5.0 && git push origin v0.5.0
   ```

   The release workflow checks the tag against every version and pin, builds
   all nine packages and publishes them to PyPI.
4. Create the GitHub release from the tag and write its notes. Start from
   `gh release create v0.5.0 --generate-notes`, which lists the merged PRs,
   then group them into breaking changes and new features as earlier releases do.
