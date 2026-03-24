# Test Updates

- Several evaluator and container-backend tests still monkeypatch
  `_ConcurrentEvaluator._run_subprocess(...)` with the legacy signature
  `(*, argv, use_shell)` and older fake `Popen` objects that only implement
  `communicate()`.
- The runtime now supports streamed log callbacks via `on_stdout` and
  `on_stderr`, but the evaluator currently preserves backward compatibility so
  these older tests continue to pass unchanged.
- Follow-up cleanup: update those tests to use streaming-aware mocks so the
  suite exercises the newer subprocess interface directly instead of relying on
  the compatibility path.
