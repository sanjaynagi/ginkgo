# Current Constraints

The current runtime still has important boundaries and tradeoffs:

- worker-executed Python tasks must be importable by module path
- the scheduler's authoritative live execution state is still in-memory, with
  persisted run state exported incrementally to `manifest.yaml` and
  `events.jsonl`

Those constraints drive several of the future roadmap items in the implementation plan.
