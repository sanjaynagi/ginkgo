# Configuration and Secrets

Workflows can declare runtime-only secret dependencies via `secret(...)`
references, which are resolved at execution time through a pluggable resolver
layer with environment-variable lookup and optional `.env` support. Secret
references remain identifiers during graph construction and cache-keying, so
rotating a credential value does not invalidate cache entries that are
otherwise still valid.

Secret-bearing inputs are redacted before they reach persisted provenance or
cache metadata, and task log capture redacts resolved secret values before they
are written to per-task stdout/stderr logs.
