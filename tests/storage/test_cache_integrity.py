"""Source hashing, cache invalidation, and restoring outputs from the cache."""

import importlib
import json
import linecache
import shutil
import stat
import sys
import textwrap
from contextlib import contextmanager
from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, folder, shell, task, untracked
from ginkgo.cli.commands.cache import _remove_tree
from ginkgo.core import source_hash
from ginkgo.core.task import TaskDef
from ginkgo.runtime.artifacts.artifact_store import make_writable_recursive
from ginkgo.runtime.caching.cache import MISSING, CacheStore, DynamicTemplate
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from tests.conftest import EventCollector, run_cli


def _write_module(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(body), encoding="utf-8")


@contextmanager
def _imported_from(root: Path, name: str):
    """Import ``name`` from ``root``, then unload every module loaded from there."""
    sys.path.insert(0, str(root))
    try:
        yield importlib.import_module(name)
    finally:
        sys.path.remove(str(root))
        for loaded, module in list(sys.modules.items()):
            path = getattr(module, "__file__", None)
            if path is not None and Path(path).is_relative_to(root):
                sys.modules.pop(loaded, None)


def _closure(module) -> set[str]:
    return set(source_hash._local_import_closure(module))


# ---------------------------------------------------------------------------
# Source hashing
# ---------------------------------------------------------------------------


def _source(decorator: str, body: str = "return x + 1", prelude: str = "") -> str:
    return f"from ginkgo import task\n{prelude}\n{decorator}\ndef compute(x: int) -> int:\n    {body}\n"


_TAG = "def tag(**kwargs):\n    return lambda fn: fn\n"


@pytest.mark.parametrize(
    ("first", "second", "same"),
    [
        pytest.param(
            _source("@task(threads=1)"),
            _source('@task(threads=8, memory="16Gi", retries=3)'),
            True,
            id="resources-are-not-hashed",
        ),
        pytest.param(
            _source("@task(threads=1)"),
            _source('@task(\n    threads=1,\n    memory="4Gi",\n)'),
            True,
            id="multi-line-decorator",
        ),
        pytest.param(_source("@task()"), _source("@task(threads=4)"), True, id="bare-decorator"),
        pytest.param(
            _source("@task(threads=2)"),
            _source('@task(threads=2, on_failure="ignore")'),
            True,
            id="failure-policy-is-not-hashed",
        ),
        pytest.param(
            _source("@task(threads=4)"), _source('@task(threads=4, env="flye")'), False, id="env"
        ),
        pytest.param(_source("@task()"), _source("@task()", "return x + 2"), False, id="body"),
        pytest.param(
            _source("@task()"),
            _source("@task()", "# Bump by one.\n    return x + 1"),
            False,
            id="comment",
        ),
        pytest.param(
            _source('@task(kind="python", threads=2)'),
            _source('@task(kind="shell", threads=2)'),
            False,
            id="kind",
        ),
        pytest.param(
            _source("@task(version=1, threads=2)"),
            _source("@task(version=2, threads=2)"),
            False,
            id="version",
        ),
        pytest.param(
            _source("@task(threads=2)\n@tag(threads=2)", prelude=_TAG),
            _source("@task(threads=2)\n@tag(threads=8)", prelude=_TAG),
            False,
            id="user-decorators-are-opaque",
        ),
    ],
)
def test_which_edits_change_a_task_s_source_hash(tmp_path, monkeypatch, first, second, same):
    monkeypatch.syspath_prepend(str(tmp_path))

    def hash_of(source: str) -> str:
        (tmp_path / "e2e_tasks.py").write_text(source, encoding="utf-8")
        linecache.checkcache()
        sys.modules.pop("e2e_tasks", None)
        importlib.invalidate_caches()
        return source_hash.compute_source_hash(importlib.import_module("e2e_tasks").compute.fn)

    try:
        assert (hash_of(first) == hash_of(second)) is same
    finally:
        sys.modules.pop("e2e_tasks", None)


def test_a_nested_definition_is_dedented_before_its_resources_are_filtered():
    def declare(threads: int):
        if threads == 1:

            @task(threads=1)
            def compute(x: int) -> int:
                return x + 1

        else:

            @task(threads=8, memory="16Gi")
            def compute(x: int) -> int:
                return x + 1

        return compute

    assert declare(1).source_hash == declare(8).source_hash


def test_a_nested_task_with_a_column_zero_string_falls_back_to_raw_source():
    """``textwrap.dedent`` cannot flatten this: over-invalidate rather than crash."""

    def declare(threads: int):
        if threads == 1:

            @task(threads=1)
            def compute() -> str:
                return """
banner
"""
        else:

            @task(threads=8)
            def compute() -> str:
                return """
banner
"""

        return compute

    assert declare(1).source_hash != declare(8).source_hash


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        pytest.param(
            '@task("shell", threads=8, env="flye")\ndef c():\n    pass\n',
            '@task("shell", env="flye")\ndef c():\n    pass\n',
            id="positional-kind-and-env-survive",
        ),
        pytest.param(
            '@task(\n    threads=8,\n    memory="4Gi",\n)\ndef c():\n    pass\n',
            "@task()\ndef c():\n    pass\n",
            id="multi-line-call-with-trailing-comma",
        ),
        pytest.param(
            "@task(retry_on=(ValueError, KeyError), version=2)\ndef c():\n    pass\n",
            "@task(version=2)\ndef c():\n    pass\n",
            id="complex-value-expression",
        ),
        pytest.param(
            "@task(threads=8, **extra)\ndef c():\n    pass\n",
            "@task(**extra)\ndef c():\n    pass\n",
            id="double-star-argument-survives",
        ),
        pytest.param(
            "@ginkgo.task(threads=8)\n@other(threads=8)\ndef c():\n    pass\n",
            "@ginkgo.task()\n@other(threads=8)\ndef c():\n    pass\n",
            id="attribute-spelling-only",
        ),
        pytest.param(
            '@task(threads=8)\ndef c():\n    return "é"  # threads=8\n',
            '@task()\ndef c():\n    return "é"  # threads=8\n',
            id="non-ascii-offsets-and-comment-text",
        ),
        pytest.param(
            "@task(threads=8)\nasync def c(x: int) -> int:\n    return x + 1\n",
            "@task()\nasync def c(x: int) -> int:\n    return x + 1\n",
            id="async",
        ),
        pytest.param("@task\ndef c():\n    pass\n", "@task\ndef c():\n    pass\n", id="bare"),
        pytest.param(
            "@task(threads=8)\ndef compute(x: int) ->\n",
            "@task(threads=8)\ndef compute(x: int) ->\n",
            id="unparseable-falls-back-unchanged",
        ),
    ],
)
def test_resource_kwargs_are_deleted_from_task_calls(source, expected):
    assert source_hash._without_resource_kwargs(source) == expected


def test_an_unsourceable_function_is_refused_at_registration():
    namespace: dict = {}
    exec("def dynamic_fn(x): return x", namespace)
    with pytest.raises(ValueError, match="Cannot extract source"):
        TaskDef(fn=namespace["dynamic_fn"])


def test_an_unparseable_helper_in_the_import_closure_is_refused_at_registration(tmp_path):
    """A helper that fails to parse must fail loudly, not truncate the closure."""
    _write_module(tmp_path / "pkg" / "__init__.py", "")
    _write_module(
        tmp_path / "pkg" / "tasks.py",
        "from . import helpers\n\ndef compute(x):\n    return helpers.compute(x)\n",
    )
    _write_module(tmp_path / "pkg" / "helpers.py", "def compute(x):\n    return x + 1\n")

    with _imported_from(tmp_path, "pkg.tasks") as tasks:
        # Break the already-imported helper on disk, as during a mid-edit.
        (tmp_path / "pkg" / "helpers.py").write_text("def compute(x) ->\n", encoding="utf-8")
        with pytest.raises(ValueError, match="Cannot parse imports of module 'pkg.helpers'"):
            TaskDef(fn=tasks.compute)


def test_a_function_local_import_not_yet_run_joins_the_closure(tmp_path, monkeypatch):
    """The closure follows import statements, not what ``sys.modules`` holds."""
    _write_module(
        tmp_path / "lazy_tasks.py",
        "def compute(x):\n    from lazy_helpers import scale\n    return scale(x)\n",
    )
    _write_module(tmp_path / "lazy_helpers.py", "def scale(x): return x * 2\n")
    monkeypatch.delitem(sys.modules, "lazy_helpers", raising=False)

    with _imported_from(tmp_path, "lazy_tasks") as tasks:
        assert "lazy_helpers" not in sys.modules
        assert _closure(tasks) == {"lazy_tasks", "lazy_helpers"}


def test_an_environment_inside_the_project_stays_out_of_the_closure(tmp_path, monkeypatch):
    """An env nested in the project root must not pull installed modules in."""
    _write_module(tmp_path / "pkg" / "__init__.py", "")
    _write_module(
        tmp_path / "pkg" / "tasks.py",
        "import vendorlib\n\nfrom . import helpers\n\n"
        "def compute(x):\n    return vendorlib.scale(helpers.compute(x))\n",
    )
    _write_module(tmp_path / "pkg" / "helpers.py", "def compute(x):\n    return x + 1\n")
    env_dir = tmp_path / ".venv"
    _write_module(env_dir / "lib" / "vendorlib.py", "def scale(x):\n    return x * 2\n")
    monkeypatch.syspath_prepend(str(env_dir / "lib"))

    with _imported_from(tmp_path, "pkg.tasks") as tasks:
        monkeypatch.setattr(source_hash, "_installed_roots", lambda: ())
        assert "vendorlib" in _closure(tasks)

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (env_dir,))
        assert _closure(tasks) == {"pkg", "pkg.tasks", "pkg.helpers"}


class TestInstalledTaskModuleClosure:
    """A task module living in site-packages is bounded by its own package."""

    _USES_VENDOR = "import vendorlib\n\ndef compute(x):\n    return vendorlib.scale(x)\n"

    @pytest.fixture
    def site(self, tmp_path, monkeypatch) -> Path:
        root = tmp_path / "site-packages"
        _write_module(root / "vendorlib" / "__init__.py", "def scale(x): return x * 2\n")
        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (root,))
        return root

    def test_own_helpers_stay_in_and_other_distributions_stay_out(self, site):
        _write_module(site / "mypkg" / "__init__.py", "")
        _write_module(
            site / "mypkg" / "tasks.py",
            "import vendorlib\n\nfrom . import helpers\n\n"
            "def compute(x):\n    return vendorlib.scale(helpers.compute(x))\n",
        )
        _write_module(site / "mypkg" / "helpers.py", "def compute(x):\n    return x + 1\n")

        with _imported_from(site, "mypkg.tasks") as tasks:
            assert _closure(tasks) == {"mypkg", "mypkg.tasks", "mypkg.helpers"}

    def test_a_namespace_portion_is_bounded_by_its_own_regular_package(self, site):
        _write_module(site / "ns" / "inner" / "__init__.py", "")
        _write_module(
            site / "ns" / "inner" / "tasks.py",
            "from ns import other\n\nfrom . import helpers\n\n"
            "def compute(x):\n    return other.scale(helpers.compute(x))\n",
        )
        _write_module(site / "ns" / "inner" / "helpers.py", "def compute(x):\n    return x + 1\n")
        _write_module(site / "ns" / "other" / "__init__.py", "def scale(x): return x * 2\n")

        with _imported_from(site, "ns.inner.tasks") as tasks:
            assert _closure(tasks) == {"ns.inner", "ns.inner.tasks", "ns.inner.helpers"}

    @pytest.mark.parametrize(
        ("package", "distributions"),
        [("mypkg", None), ("nspkg", {"nspkg": ["nsdist"]})],
        ids=["regular-package", "init-less-package"],
    )
    def test_a_changed_installed_helper_changes_the_hash(
        self, site, monkeypatch, package, distributions
    ):
        """The point of the closure: an installed helper still invalidates."""
        if distributions is None:
            _write_module(site / package / "__init__.py", "")
        else:
            monkeypatch.setattr(source_hash, "_packages_distributions", lambda: distributions)
        _write_module(
            site / package / "tasks.py",
            f"from {package} import helpers\n\ndef compute(x):\n    return helpers.compute(x)\n",
        )
        helpers = site / package / "helpers.py"
        _write_module(helpers, "def compute(x):\n    return x + 1\n")

        with _imported_from(site, f"{package}.tasks") as tasks:
            before = source_hash.compute_source_hash(tasks.compute)
            helpers.write_text("def compute(x):\n    return x + 100\n", encoding="utf-8")
            assert source_hash.compute_source_hash(tasks.compute) != before

    @pytest.mark.parametrize(
        ("distributions", "expected"),
        [({}, {"solo_tasks", "vendorlib"}), ({"solo_tasks": ["solodist"]}, {"solo_tasks"})],
        ids=["unresolvable-over-hashes", "distribution-confirmed-holds-only-itself"],
    )
    def test_a_bare_installed_module(self, site, monkeypatch, distributions, expected):
        _write_module(site / "solo_tasks.py", self._USES_VENDOR)
        monkeypatch.setattr(source_hash, "_packages_distributions", lambda: distributions)

        with _imported_from(site, "solo_tasks") as tasks:
            assert _closure(tasks) == expected


def test_editing_a_task_or_a_helper_it_imports_is_a_cache_miss(tmp_path):
    _write_module(tmp_path / "pkg" / "__init__.py", "")
    tasks_path = tmp_path / "pkg" / "tasks.py"
    _write_module(
        tasks_path,
        "from ginkgo import task\nfrom . import helpers\n\n"
        "@task()\ndef compute(x: int) -> int:\n    return helpers.compute(x)\n",
    )
    _write_module(
        tmp_path / "pkg" / "helpers.py",
        "from .transform import adjust\n\ndef compute(x):\n    return adjust(x)\n",
    )
    transform_path = tmp_path / "pkg" / "transform.py"
    _write_module(transform_path, "def adjust(x):\n    return x + 1\n")

    def run(tasks) -> tuple[int, EventCollector]:
        collector = EventCollector()
        return evaluate(tasks.compute(x=5), event_bus=collector.bus), collector

    with _imported_from(tmp_path, "pkg.tasks") as tasks:
        assert run(tasks)[0] == 6
        result, collector = run(tasks)
        assert result == 6 and collector.cached()

        # A transitive helper changes.
        transform_path.write_text("def adjust(x):\n    return x + 10\n", encoding="utf-8")
        importlib.reload(sys.modules["pkg.transform"])
        importlib.reload(sys.modules["pkg.helpers"])
        tasks = importlib.reload(tasks)
        result, collector = run(tasks)
        assert result == 15 and collector.started() and not collector.cached()

        # The task's own body changes.
        tasks_path.write_text(
            tasks_path.read_text(encoding="utf-8").replace(
                "helpers.compute(x)\n", "helpers.compute(x) * 2\n"
            ),
            encoding="utf-8",
        )
        tasks = importlib.reload(tasks)
        result, collector = run(tasks)
        assert result == 30 and collector.started() and not collector.cached()


_SIBLINGS = """\
from ginkgo import flow, task

SCALE = 2


def helper(x):
    return x + 1


@task()
def a(x: int) -> int:
    return helper(x) * SCALE


@task()
def b(x: int) -> int:
    return x - 1


@flow
def main():
    return [a(x=1), b(x=2)]
"""


@pytest.mark.parametrize(
    ("old", "new", "a_same", "b_same"),
    [
        pytest.param("a(x=1), b(x=2)", "a(x=3), b(x=4)", True, True, id="flow-body"),
        pytest.param("return x - 1", "return x - 2", True, False, id="sibling-body"),
        pytest.param("return x + 1", "return x + 3", False, False, id="helper"),
        pytest.param("SCALE = 2", "SCALE = 3", False, False, id="constant"),
        pytest.param("def b(", "def b2(", False, None, id="sibling-renamed"),
        pytest.param(
            "@task()\ndef b", "@task(version=2)\ndef b", False, False, id="sibling-decorator"
        ),
    ],
)
def test_a_task_s_hash_leaves_out_other_task_and_flow_bodies(
    tmp_path, monkeypatch, old, new, a_same, b_same
):
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        a1, b1 = _task_hashes(tmp_path, _SIBLINGS)
        a2, b2 = _task_hashes(tmp_path, _SIBLINGS.replace(old, new))
        assert (a1 == a2) is a_same
        assert b_same is None or (b1 == b2) is b_same
    finally:
        sys.modules.pop("sibling_tasks", None)


def _task_hashes(root: Path, source: str) -> tuple[str, str | None]:
    """Load ``source`` as ``sibling_tasks`` from ``root``; hash tasks ``a`` and ``b``."""
    (root / "sibling_tasks.py").write_text(source, encoding="utf-8")
    linecache.checkcache()
    sys.modules.pop("sibling_tasks", None)
    importlib.invalidate_caches()
    module = importlib.import_module("sibling_tasks")
    b = getattr(module, "b", None)
    return (
        source_hash.compute_source_hash(module.a.fn),
        b and source_hash.compute_source_hash(b.fn),
    )


_FOREIGN_AND_ALIASED = """\
from celery import Celery
from ginkgo import task as t

app = Celery()


@app.task
def helper():
    return 1


@t()
def a(x: int) -> int:
    return x + helper()


@t()
def b(x: int) -> int:
    return x - 1
"""


@pytest.mark.parametrize(
    ("old", "new", "a_same"),
    [
        pytest.param("return 1", "return 2", False, id="another-library-s-task-keeps-its-body"),
        pytest.param(
            "return x - 1", "return x - 2", True, id="an-aliased-ginkgo-task-is-left-out"
        ),
    ],
)
def test_only_ginkgo_s_own_decorators_leave_a_body_out(tmp_path, monkeypatch, old, new, a_same):
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "celery.py").write_text(
        "class Celery:\n    def task(self, fn):\n        return fn\n", encoding="utf-8"
    )
    try:
        a1, _ = _task_hashes(tmp_path, _FOREIGN_AND_ALIASED)
        a2, _ = _task_hashes(tmp_path, _FOREIGN_AND_ALIASED.replace(old, new))
        assert (a1 == a2) is a_same
    finally:
        sys.modules.pop("sibling_tasks", None)
        sys.modules.pop("celery", None)


def test_editing_one_task_in_a_workflow_file_leaves_the_other_cached(tmp_path):
    flow_path = tmp_path / "flow.py"
    flow_path.write_text(_SIBLINGS, encoding="utf-8")
    assert run_cli("run", "flow.py", cwd=tmp_path).returncode == 0

    flow_path.write_text(_SIBLINGS.replace("return x - 1", "return x - 2"), encoding="utf-8")
    assert run_cli("run", "flow.py", cwd=tmp_path).returncode == 0

    explained = json.loads(run_cli("cache", "explain", "--json", cwd=tmp_path).stdout)
    reasons = {task["task_name"]: task["reason"] for task in explained["tasks"]}
    assert reasons == {"a": "all_inputs_match", "b": "source_hash_changed"}


# ---------------------------------------------------------------------------
# Outputs restored from the cache
# ---------------------------------------------------------------------------


@task(kind="shell")
def write_file_task(output_path: Out[file]) -> file:
    return shell(cmd=f"echo 'hello' > {output_path}", output=output_path)


@task(kind="shell")
def write_folder_task(output_dir: Out[folder]) -> folder:
    return shell(
        cmd=f"mkdir -p {output_dir} && echo a > {output_dir}/a.txt && echo b > {output_dir}/b.txt",
        output=output_dir,
    )


@task()
def write_python_file_task(output_path: Out[file], payload: str) -> file:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload, encoding="utf-8")
    return str(output)


def _cached_run(expr) -> object:
    collector = EventCollector()
    result = evaluate(expr, event_bus=collector.bus)
    assert collector.cached()
    return result


def test_a_file_output_is_a_writable_copy_that_a_hit_restores(tmp_path):
    output = tmp_path / "result.txt"
    result = Path(str(evaluate(write_file_task(output_path=str(output)))))
    assert result.is_file() and not result.is_symlink()
    assert result.read_text().strip() == "hello"

    # The user can edit it; the next run restores it without missing the cache.
    output.write_text("tampered", encoding="utf-8")
    assert (
        Path(str(_cached_run(write_file_task(output_path=str(output))))).read_text() == "hello\n"
    )

    output.unlink()
    restored = Path(str(_cached_run(write_file_task(output_path=str(output)))))
    assert restored.is_file() and not restored.is_symlink()
    assert restored.read_text().strip() == "hello"


def test_a_folder_output_holds_writable_copies_that_a_hit_restores(tmp_path):
    output = tmp_path / "outdir"
    result = Path(str(evaluate(write_folder_task(output_dir=str(output)))))
    assert result.is_dir()
    assert not (result / "a.txt").is_symlink() and not (result / "b.txt").is_symlink()
    assert (result / "b.txt").read_text().strip() == "b"

    (output / "a.txt").write_text("tampered", encoding="utf-8")
    restored = Path(str(_cached_run(write_folder_task(output_dir=str(output)))))
    assert (restored / "a.txt").read_text().strip() == "a"

    shutil.rmtree(output)
    restored = Path(str(_cached_run(write_folder_task(output_dir=str(output)))))
    assert (restored / "a.txt").read_text().strip() == "a"


def test_corrupt_stored_bytes_are_a_cache_miss(tmp_path):
    output = tmp_path / "corrupt.txt"
    evaluate(write_python_file_task(output_path=str(output), payload="GOOD"))
    output.unlink()
    (blob,) = (tmp_path / ".ginkgo" / "artifacts" / "blobs").iterdir()
    blob.chmod(0o644)
    blob.write_text("BAD!", encoding="utf-8")

    collector = EventCollector()
    evaluate(
        write_python_file_task(output_path=str(output), payload="GOOD"), event_bus=collector.bus
    )
    assert not collector.cached()
    assert output.read_text(encoding="utf-8") == "GOOD"
    assert blob.read_text(encoding="utf-8") == "GOOD"


@pytest.mark.parametrize("kind", ["file", "folder"])
def test_a_hit_whose_stored_bytes_were_deleted_stores_them_again(tmp_path, kind):
    """ED-7: the outputs are intact, so the hit stands and re-stores them."""
    output = tmp_path / "intact"
    expr = (
        (lambda: write_python_file_task(output_path=str(output), payload="GOOD"))
        if kind == "file"
        else (lambda: write_folder_task(output_dir=str(output)))
    )
    evaluate(expr())
    artifacts = tmp_path / ".ginkgo" / "artifacts"
    make_writable_recursive(artifacts)
    shutil.rmtree(artifacts)

    _cached_run(expr())

    if kind == "file":
        (blob,) = (artifacts / "blobs").iterdir()
        assert blob.read_text(encoding="utf-8") == "GOOD"
    else:
        # The tree's manifest and member blobs are back: the folder restores from them.
        shutil.rmtree(output)
        assert (Path(str(evaluate(expr()))) / "b.txt").read_text().strip() == "b"


def test_a_python_file_output_rerun_overwrites_the_previous_materialization(tmp_path):
    output = tmp_path / "python.txt"
    first = evaluate(write_python_file_task(output_path=str(output), payload="first"))
    assert Path(str(first)).read_text(encoding="utf-8") == "first"

    second = evaluate(write_python_file_task(output_path=str(output), payload="second"))
    assert Path(str(second)).read_text(encoding="utf-8") == "second"


def test_pruning_removes_a_read_only_entry_tree(tmp_path):
    evaluate(write_file_task(output_path=str(tmp_path / "prunable.txt")))
    entries = [entry for entry in (tmp_path / ".ginkgo" / "cache").iterdir() if entry.is_dir()]
    assert entries

    # A naive shutil.rmtree would fail with PermissionError on this tree.
    for entry in entries:
        for child in entry.rglob("*"):
            child.chmod(stat.S_IRUSR | stat.S_IXUSR if child.is_dir() else stat.S_IRUSR)
        entry.chmod(stat.S_IRUSR | stat.S_IXUSR)
        _remove_tree(entry)

    assert not any(child.is_dir() for child in (tmp_path / ".ginkgo" / "cache").iterdir())


# ---------------------------------------------------------------------------
# Unreadable entries
# ---------------------------------------------------------------------------


@task()
def counted_square_task(log_path: untracked, value: int) -> int:
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write("run\n")
    return value * value


@pytest.mark.parametrize(
    "content",
    ['{"__ginkgo_type__": "retired", "value": {}}', '{"__ginkgo_type__": "li'],
    ids=["unknown-codec-tag", "truncated"],
)
def test_an_unreadable_entry_reruns_once_then_caches(tmp_path, content):
    log_path = str(tmp_path / "runs.log")
    evaluate(counted_square_task(log_path=log_path, value=3))
    (entry,) = (tmp_path / ".ginkgo" / "cache").glob("*/output.json")
    entry.write_text(content, encoding="utf-8")

    # Loading it, as a dry run does, leaves it for the next real run to replace.
    store = ConcurrentEvaluator().cache_store
    assert store.load(cache_key=entry.parent.name, task_def=counted_square_task) is MISSING
    assert entry.is_file()

    assert evaluate(counted_square_task(log_path=log_path, value=3)) == 9
    assert _cached_run(counted_square_task(log_path=log_path, value=3)) == 9
    assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["run", "run"]


def test_a_dynamic_template_entry_needs_its_task_and_its_index_row(tmp_path, monkeypatch):
    """A dynamic task's entry holds its returned expressions (G1)."""
    _write_module(
        tmp_path / "template_tasks.py",
        """\
        from ginkgo import task

        @task()
        def child(x: int) -> int:
            return x

        @task()
        def parent() -> int:
            return child(x=1)
        """,
    )
    with _imported_from(tmp_path, "template_tasks") as tasks:
        store = CacheStore(index=CacheIndex.in_memory(), root=tmp_path / "cache")
        store.save(
            cache_key="k",
            result=DynamicTemplate(tasks.child(x=1)),
            task_def=tasks.parent,
            resolved_args={},
            input_hashes={},
        )
        assert isinstance(store.load(cache_key="k", task_def=tasks.parent), DynamicTemplate)

        # A lost ginkgo.db: the entry's bytes remain, its row does not.
        rowless = CacheStore(index=CacheIndex.in_memory(), root=tmp_path / "cache")
        assert rowless.load(cache_key="k", task_def=tasks.parent) is MISSING
        assert not rowless.has_entry(cache_key="k", task_def=tasks.parent)

        # The child is renamed away: the stored template can no longer load.
        monkeypatch.delattr(tasks, "child")
        assert store.load(cache_key="k", task_def=tasks.parent) is MISSING
