# Phase 13 — Secrets and Credentials Management

## Problem Definition

Ginkgo currently treats task inputs as ordinary resolved Python values. That is
the wrong model for credentials. If users pass API keys, webhook URLs, or
passwords through normal task arguments, those values can flow into cache
metadata, run manifests, params files, and task logs.

Phase 13 adds a first-class secrets mechanism so workflows can declare secret
dependencies explicitly while keeping secret values out of provenance and cache
identity. The design also needs to leave a clean extension point for later
integration with external secret managers.

---

## Proposed Solution

Implement a small secrets subsystem with three core pieces:

1. **Secret references in the DSL**
   Workflow authors declare a secret dependency by passing a secret reference
   rather than a raw value, for example `secret("SLACK_WEBHOOK")`. The
   expression tree stores only the secret identifier, not the resolved value.

2. **Runtime-only resolution**
   Secret values are resolved immediately before task execution through a
   pluggable resolver layer. Initial backends:
   - environment variable lookup
   - optional `.env` loading with explicit opt-in
   - a stable interface for future vault-backed resolvers

3. **Redaction and identity rules**
   Secret values must never be written to:
   - run manifests or params files
   - cache metadata or cache keys
   - persisted stdout/stderr logs

   Cache identity should use the secret reference identity, not the secret
   value, so credential rotation does not invalidate otherwise reusable work.

### Integration Points

- [`ginkgo/core/task.py`](/Users/sanjay.nagi/Software/ginkgo/ginkgo/core/task.py):
  accept secret references as a distinct input form in task calls.
- [`ginkgo/runtime/evaluator.py`](/Users/sanjay.nagi/Software/ginkgo/ginkgo/runtime/evaluator.py):
  resolve secret references after DAG construction and before task execution.
- [`ginkgo/runtime/cache.py`](/Users/sanjay.nagi/Software/ginkgo/ginkgo/runtime/cache.py):
  hash secret identities rather than secret values.
- [`ginkgo/runtime/provenance.py`](/Users/sanjay.nagi/Software/ginkgo/ginkgo/runtime/provenance.py):
  redact secret-bearing inputs before writing manifests.
- CLI:
  add `ginkgo secrets list` and `ginkgo secrets validate`, and extend
  `ginkgo doctor` with secret-resolution checks.

---

## Risks and Tradeoffs

- **Log redaction is the highest-risk area.** Preventing leaks in stored logs is
  stricter than simply omitting secrets from manifests.
- **Secret safety is framework-level, not absolute.** If user task code writes a
  secret into an output artifact, Phase 13 should not try to infer or undo that.
- **Static secret discovery will be intentionally conservative.** CLI reporting
  should work well for directly declared secret references, but not promise full
  recovery of dynamically constructed secret names.
- **Cache semantics need to be explicit.** Using secret identity instead of
  value avoids unnecessary invalidation, but it also means secret rotation alone
  will not force recomputation.

---

## Success Criteria

- Tasks can declare secret dependencies without embedding secret values in code
  or config.
- Secret values are resolved at execution time and do not appear in manifests,
  params files, cache metadata, or cache keys.
- Persisted task logs redact resolved secret values.
- `ginkgo secrets list` reports declared secrets for a workflow.
- `ginkgo secrets validate` reports missing or unresolvable secrets clearly.
- `ginkgo doctor` flags workflows with declared but unresolvable secrets before
  execution begins.
- Rotating a secret value without changing its identifier does not invalidate an
  otherwise valid cache entry.

---

## Suggested Implementation Order

1. Add secret reference types and a resolver interface.
2. Integrate runtime resolution into the evaluator.
3. Enforce redaction in provenance, cache metadata, and task logs.
4. Add CLI secret inspection and validation commands.
5. Extend diagnostics and add focused tests for leak prevention and cache
   stability.
