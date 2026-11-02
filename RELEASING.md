# Releasing

Ginkgo and its eight first-party plugins release together at one version.

1. Move the `Unreleased` entries in `CHANGELOG.md` under a heading for the new
   version and date.
2. Set the version everywhere and relock:

   ```bash
   pixi run set-version 0.5.0
   ```

   This rewrites every package's version, every `ginkgo-*==` pin between them
   and the docs pin, then runs `pixi lock`.
3. Merge those changes to `main`.
4. Tag the merge commit and push the tag:

   ```bash
   git tag v0.5.0 && git push origin v0.5.0
   ```

   The release workflow checks the tag against every version and pin, builds
   all nine packages and publishes them to PyPI.
5. Create the GitHub release from the tag, with the changelog entry as its notes.
