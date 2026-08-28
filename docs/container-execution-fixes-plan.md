# Plan — container execution: host data, output ownership, image shell

## Problem

External review (`ginkgo-container-findings.md`, 2026-08-17, plus a fourth gap
reported alongside it) found four gaps in container-backed shell execution, all
hit the first time a workflow's inputs live outside the project directory:

1. **Blocking.** `ContainerBackend.exec_argv` bind-mounts exactly one path, the
   project root. Any declared `file`/`folder` input outside that tree does not
   exist inside the container, so the task fails. Symlinks do not help — the
   link target resolves to an unmounted path. There is also no config surface:
   `runtime` and `pull_policy` are unreachable from `ginkgo.toml`.
2. **High.** No `--user` is passed, so containers run as the image's default
   user (usually root) and leave root-owned outputs. Container envs are
   shell-only, so a downstream Python task rewriting that path fails with
   `EPERM`, and reruns cannot overwrite their own outputs.
3. **Low but sharp.** `bash` is assumed present in the image. Alpine and
   distroless images fail with an opaque exec error that does not name the
   cause.
4. **Quiet.** `build_shell_subprocess_env` computes `GINKGO_THREADS`, and the
   BLAS/OpenMP variables under `export_thread_env`, but for a container task the
   subprocess is the runtime *client*, and `exec_argv` emitted no `-e` flags. A
   command written as `--threads ${GINKGO_THREADS:-8}` silently took the
   fallback, and `export_thread_env=True` was entirely inert — so a container
   task could not be given a CPU budget through Ginkgo's own mechanism.

## Proposed solution

### Automatic mounts for declared path inputs and outputs

`exec_argv` gains `mounts: Sequence[Mount] = ()`. A `Mount` carries a host
path, the container path it appears at, and a mode (`ro`/`rw`).

`ShellRunner.run_logged_command` already holds the `NodeRun`, whose
`task_def.type_hints` and `resolved_args` between them identify the declared
path-shaped inputs (`is_path_shaped_annotation`). It derives read-only mounts
from those, and each driver runner adds read-write mounts for its directive's
declared outputs. Mount resolution:

- Resolve symlinks and mount the real path *at the path as given*, so a command
  written against the symlink still resolves.
- Drop any mount already covered by the project-root mount (same real path, and
  the declared path under the project root).
- Collapse a descendant into an enclosing mount of the same mode.
- Mount a `file` input's *directory*, read-only. Tools routinely read a sibling
  of the file they are handed (`ref.fa.fai`, `.bai`), and mounting the file alone
  makes an index that exists on the host invisible. A tool that wants to *write*
  an index then fails and says so, which is the right outcome: the container's
  own layer is discarded on exit, so an undeclared sibling write is silently
  lost. Declaring the index as an output is what makes it writable and keeps it.
- Mount a declared output's parent, since the output does not exist yet and the
  runtime would create a directory at that path to satisfy the mount. Do not
  mount a directive's `log`: it is captured host-side from the client's pipes,
  so the container never opens it.
- Carry each mount's origin. A *declared* mount may be widened from read-only to
  read-write by another declared mount; a *configured* one is the user's
  decision, so widening it raises instead.
- Refuse `/`, system directories, and the home directory, under both their
  literal and resolved names (macOS reaches system directories through
  `/private`). An output written straight into `$HOME` would otherwise hand the
  image `~/.ssh` and `~/.aws`.

Pixi ignores the new keyword. A default of `()` keeps existing callers working.

Because only *declared* inputs are mounted, this rewards annotating paths as
`file`/`folder` — already required for cache correctness. A path interpolated
into a command string from config stays invisible, which is the right pressure.

### Forward Ginkgo's computed environment

`exec_argv` gains `env_vars`, a list of *names* emitted as bare `-e NAME`. Both
runtimes resolve a bare name from the client process's own environment, so the
values are not spelled out in the argument vector. `computed_env_var_names`
names exactly what `build_shell_subprocess_env` computes; forwarding the rest of
`os.environ` would undermine the controlled environment that is a container's
whole point.

### `[container]` config table

```toml
[container]
runtime = "docker"          # or "podman"
pull_policy = "if-not-present"
user = "auto"               # "auto" (host uid:gid) | "root" | "1000:1000"
shell = "bash"
auto_mount = true
extra_mounts = ["/scratch:rw", "/opt/refs:ro"]
```

`extra_mounts` is the escape hatch for paths that cannot be declared inputs (a
tool's cache directory, a licence file).

`container_backend_from_config` rejects unknown keys and mistyped values the way
`[resources.overrides]` does, rather than falling back to defaults a reader of
the file would not expect.

### Run as the invoking user

`user = "auto"` picks whatever leaves outputs owned by the invoking user, which
is runtime-specific. Docker needs `-u <uid>:<gid>`, paired with `-e HOME=/tmp`
because that uid has no passwd entry and images that resolve `$HOME` fail on
that. Rootless Podman already maps the container's root to the invoking user,
and passing `-u` there maps into the *subordinate* uid range instead — leaving
files the user cannot chmod or delete — so nothing is passed. `user = "root"`
restores the old behaviour for images that install at runtime.

### Name the missing shell

A prepare-time probe was considered and rejected: it costs a container start per
image per run, and it cannot run under `pull_policy = "never"` without the image
already present. Instead `exec_failure_hint` on the `ExecutionEnvironment`
protocol diagnoses the failure that already happened — no extra work — and every
driver runner asks through `ShellRunner.failure_hint`, since each raises its own
error type.

The test is narrow on purpose: only markers the runtime itself emits, and only
when the shell is named as the binary that could not be executed. A shell that
started and then failed on a CRLF script or a missing input says
"no such file or directory" too, and diagnosing that as a missing shell would
cost its author more time than saying nothing.

## Out of scope

- **Project root discovery.** The finding notes `project_root` is `Path.cwd()`
  rather than a discovered root. That is true of the whole CLI — the Pixi
  registry, secret resolver, and config loader are all cwd-rooted — so it is a
  cross-cutting change, not a container one, and auto-mounts remove the pressure
  that made it blocking.
- **Warning on out-of-root local paths under a remote executor.** Remote
  executors do not use `ContainerBackend`; the concern belongs with remote input
  staging policy.

## Risks and tradeoffs

- Mounting real paths at the paths as given can shadow part of the project mount
  when a path inside the project symlinks outside it. That is the intended
  behaviour and matches what the command dereferences.
- `-u` will break images that need root. Mitigated by `user = "root"`.
- Mounting a `file` input's directory exposes more than the file. It is
  read-only, and the alternative hides sibling indexes the tool needs.
- The missing-shell hint is a heuristic over runtime output. It is tuned to stay
  silent when unsure, so the failure mode is an absent hint, not a wrong one.

## Success criteria

- A container task with a declared input outside the project root runs.
- A container task's outputs are owned by the invoking user.
- A container shell task honours `GINKGO_THREADS` and, under
  `export_thread_env`, the BLAS/OpenMP variables.
- An image without `bash` fails with a message naming the image and the shell,
  and an ordinary command failure does not produce that message.
- `[container]` settings reach the backend from `ginkgo.toml`, for `run` and
  `doctor` alike.
- Shell, script, and notebook container tasks all get declared mounts and the
  failure hint.
- Existing container tests pass; new unit tests cover mount resolution, mount
  safety, mode conflicts, user selection per runtime, environment forwarding,
  the config table, and the failure hint's silence on false positives.
