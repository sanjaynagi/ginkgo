"""Integration tests for cache integrity and working-tree materialization."""

import importlib
import linecache
import stat
import sys
import textwrap
from contextlib import contextmanager
from pathlib import Path

import pytest

from ginkgo import evaluate, file, folder, shell, task
from ginkgo.cli.commands.cache import _safe_rmtree
from ginkgo.core import source_hash
from ginkgo.core.task import TaskDef
from tests.conftest import EventCollector


# ---------------------------------------------------------------------------
# Source hashing
# ---------------------------------------------------------------------------


class TestSourceHash:
    def test_source_hash_is_stable(self):
        @task()
        def stable_fn(x: int) -> int:
            return x + 1

        assert stable_fn.source_hash == stable_fn.source_hash

    def test_different_bodies_produce_different_hashes(self):
        @task()
        def fn_a(x: int) -> int:
            return x + 1

        @task()
        def fn_b(x: int) -> int:
            return x + 2

        assert fn_a.source_hash != fn_b.source_hash

    def test_resource_declarations_do_not_change_the_hash(self):
        """Resources are not cache-relevant, so decorator text must not be hashed."""

        def declare_lean():
            @task(threads=1)
            def compute(x: int) -> int:
                return x + 1

            return compute

        def declare_generous():
            @task(threads=8, memory="16Gi", retries=3)
            def compute(x: int) -> int:
                return x + 1

            return compute

        assert declare_lean().source_hash == declare_generous().source_hash

    def test_multi_line_decorator_is_filtered_the_same_way(self):
        """A resource kwarg spanning several lines leaves no trace in the hash."""

        def declare_inline():
            @task(threads=1)
            def compute(x: int) -> int:
                return x + 1

            return compute

        def declare_wrapped():
            @task(
                threads=1,
                memory="4Gi",
            )
            def compute(x: int) -> int:
                return x + 1

            return compute

        assert declare_inline().source_hash == declare_wrapped().source_hash

    def test_resourceless_decorator_hashes_like_its_resourced_twin(self):
        """Deleting every resource kwarg leaves the bare ``@task()`` call behind."""

        def declare_bare():
            @task()
            def compute(x: int) -> int:
                return x + 1

            return compute

        def declare_resourced():
            @task(threads=4)
            def compute(x: int) -> int:
                return x + 1

            return compute

        assert declare_bare().source_hash == declare_resourced().source_hash

    def test_env_argument_still_changes_the_hash(self):
        """Only the resource-only arguments are filtered; ``env`` stays."""

        def declare_local():
            @task(threads=4)
            def compute(x: int) -> int:
                return x + 1

            return compute

        def declare_foreign():
            @task(threads=4, env="flye")
            def compute(x: int) -> int:
                return x + 1

            return compute

        assert declare_local().source_hash != declare_foreign().source_hash

    def test_body_change_still_changes_the_hash(self):
        """Filtering resource kwargs must not blunt invalidation on a body edit."""

        def declare_original():
            @task(threads=1)
            def compute(x: int) -> int:
                return x + 1

            return compute

        def declare_edited():
            @task(threads=1)
            def compute(x: int) -> int:
                return x + 2

            return compute

        assert declare_original().source_hash != declare_edited().source_hash

    def test_comment_change_still_changes_the_hash(self):
        """The definition's original bytes are hashed, comments included."""

        def declare_uncommented():
            @task()
            def compute(x: int) -> int:
                return x + 1

            return compute

        def declare_commented():
            @task()
            def compute(x: int) -> int:
                # Bump by one.
                return x + 1

            return compute

        assert declare_uncommented().source_hash != declare_commented().source_hash

    def test_async_definition_is_filtered_like_any_other(self):
        source = textwrap.dedent("""\
            @task(threads=8)
            async def compute(x: int) -> int:
                return x + 1
        """)
        assert source_hash._without_resource_kwargs(source) == (
            "@task()\nasync def compute(x: int) -> int:\n    return x + 1\n"
        )

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
            pytest.param("@task\ndef c():\n    pass\n", "@task\ndef c():\n    pass\n", id="bare"),
        ],
    )
    def test_resource_kwargs_are_deleted_from_task_calls(self, source, expected):
        assert source_hash._without_resource_kwargs(source) == expected

    def test_unparseable_source_falls_back_unchanged(self):
        source = "@task(threads=8)\ndef compute(x: int) ->\n"
        assert source_hash._without_resource_kwargs(source) == source

    def test_nested_task_with_column_zero_string_falls_back_to_raw_source(self):
        """``textwrap.dedent`` cannot flatten this, so the raw text is hashed."""

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

        # Over-invalidation, not a crash: the definition text keeps its kwargs.
        assert declare(1).source_hash != declare(8).source_hash

    def test_unsourceable_function_raises_at_registration(self):
        # Functions created via exec() have no inspectable source.
        ns: dict = {}
        exec("def dynamic_fn(x): return x", ns)
        with pytest.raises(ValueError, match="Cannot extract source"):
            TaskDef(fn=ns["dynamic_fn"])

    def test_unparseable_helper_in_import_closure_raises_at_registration(self, tmp_path):
        """A helper that fails to parse must fail loudly, not truncate the closure."""
        module_dir = tmp_path / "pkg"
        module_dir.mkdir()
        (module_dir / "__init__.py").write_text("")
        (module_dir / "tasks.py").write_text(
            textwrap.dedent("""\
                from . import helpers

                def compute(x: int) -> int:
                    return helpers.compute(x)
            """),
            encoding="utf-8",
        )
        helpers_path = module_dir / "helpers.py"
        helpers_path.write_text(
            "def compute(x: int) -> int:\n    return x + 1\n", encoding="utf-8"
        )

        import importlib
        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            tasks = importlib.import_module("pkg.tasks")

            # Break the already-imported helper on disk, as during a mid-edit.
            helpers_path.write_text("def compute(x: int) ->\n", encoding="utf-8")

            with pytest.raises(ValueError, match="Cannot parse imports of module 'pkg.helpers'"):
                TaskDef(fn=tasks.compute)
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("pkg.helpers", None)
            sys.modules.pop("pkg.tasks", None)
            sys.modules.pop("pkg", None)

    def test_environment_inside_project_stays_out_of_the_closure(self, tmp_path, monkeypatch):
        """An env nested in the project root must not pull installed modules in."""
        module_dir = tmp_path / "pkg"
        module_dir.mkdir()
        (module_dir / "__init__.py").write_text("")
        (module_dir / "tasks.py").write_text(
            textwrap.dedent("""\
                import vendorlib

                from . import helpers

                def compute(x: int) -> int:
                    return vendorlib.scale(helpers.compute(x))
            """),
            encoding="utf-8",
        )
        (module_dir / "helpers.py").write_text(
            "def compute(x: int) -> int:\n    return x + 1\n", encoding="utf-8"
        )
        env_dir = tmp_path / ".venv"
        site_dir = env_dir / "lib"
        site_dir.mkdir(parents=True)
        (site_dir / "vendorlib.py").write_text(
            "def scale(x: int) -> int:\n    return x * 2\n", encoding="utf-8"
        )

        import importlib
        import sys

        sys.path.insert(0, str(site_dir))
        sys.path.insert(0, str(tmp_path))
        try:
            tasks = importlib.import_module("pkg.tasks")

            monkeypatch.setattr(source_hash, "_installed_roots", lambda: ())
            unfiltered = source_hash._local_import_closure(tasks)
            assert "vendorlib" in unfiltered

            monkeypatch.setattr(source_hash, "_installed_roots", lambda: (env_dir,))
            assert set(source_hash._local_import_closure(tasks)) == {
                "pkg",
                "pkg.tasks",
                "pkg.helpers",
            }
        finally:
            sys.path.remove(str(tmp_path))
            sys.path.remove(str(site_dir))
            sys.modules.pop("vendorlib", None)
            sys.modules.pop("pkg.helpers", None)
            sys.modules.pop("pkg.tasks", None)
            sys.modules.pop("pkg", None)


@contextmanager
def _imported_from(install_root: Path, name: str):
    """Import ``name`` from ``install_root``, then unload it and its siblings."""
    sys.path.insert(0, str(install_root))
    try:
        yield importlib.import_module(name)
    finally:
        sys.path.remove(str(install_root))
        for loaded in list(sys.modules):
            module = sys.modules[loaded]
            path = getattr(module, "__file__", None)
            if path is not None and Path(path).is_relative_to(install_root):
                sys.modules.pop(loaded, None)


def _write_module(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(body), encoding="utf-8")


class TestInstalledTaskModuleClosure:
    """A task module living in site-packages is bounded by its own package."""

    def test_own_helpers_stay_in_closure_and_other_distributions_stay_out(
        self, tmp_path, monkeypatch
    ):
        install_root = tmp_path / "site-packages"
        _write_module(install_root / "mypkg" / "__init__.py", "")
        _write_module(
            install_root / "mypkg" / "tasks.py",
            """\
            import vendorlib

            from . import helpers

            def compute(x: int) -> int:
                return vendorlib.scale(helpers.compute(x))
            """,
        )
        _write_module(
            install_root / "mypkg" / "helpers.py",
            "def compute(x: int) -> int:\n    return x + 1\n",
        )
        _write_module(install_root / "vendorlib" / "__init__.py", "def scale(x): return x * 2\n")

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (install_root,))
        with _imported_from(install_root, "mypkg.tasks") as tasks:
            assert set(source_hash._local_import_closure(tasks)) == {
                "mypkg",
                "mypkg.tasks",
                "mypkg.helpers",
            }

    def test_namespace_portion_bounds_at_the_own_regular_package(self, tmp_path, monkeypatch):
        """Sibling portions of a shared namespace belong to other distributions."""
        install_root = tmp_path / "site-packages"
        _write_module(install_root / "ns" / "inner" / "__init__.py", "")
        _write_module(
            install_root / "ns" / "inner" / "tasks.py",
            """\
            from ns import other

            from . import helpers

            def compute(x: int) -> int:
                return other.scale(helpers.compute(x))
            """,
        )
        _write_module(
            install_root / "ns" / "inner" / "helpers.py",
            "def compute(x: int) -> int:\n    return x + 1\n",
        )
        _write_module(
            install_root / "ns" / "other" / "__init__.py", "def scale(x): return x * 2\n"
        )

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (install_root,))
        with _imported_from(install_root, "ns.inner.tasks") as tasks:
            assert set(source_hash._local_import_closure(tasks)) == {
                "ns.inner",
                "ns.inner.tasks",
                "ns.inner.helpers",
            }

    def test_changed_helper_changes_the_hash(self, tmp_path, monkeypatch):
        """The point of the closure: an installed helper still invalidates."""
        install_root = tmp_path / "site-packages"
        _write_module(install_root / "mypkg" / "__init__.py", "")
        _write_module(
            install_root / "mypkg" / "tasks.py",
            """\
            from . import helpers

            def compute(x: int) -> int:
                return helpers.compute(x)
            """,
        )
        helpers_path = install_root / "mypkg" / "helpers.py"
        _write_module(helpers_path, "def compute(x: int) -> int:\n    return x + 1\n")

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (install_root,))
        with _imported_from(install_root, "mypkg.tasks") as tasks:
            before = source_hash.compute_source_hash(tasks.compute)
            helpers_path.write_text(
                "def compute(x: int) -> int:\n    return x + 100\n", encoding="utf-8"
            )
            assert source_hash.compute_source_hash(tasks.compute) != before

    def test_init_less_installed_package_still_hashes_its_helpers(self, tmp_path, monkeypatch):
        """A PEP 420 / stub-only package has no ``__init__.py`` to bound the walk."""
        install_root = tmp_path / "site-packages"
        _write_module(
            install_root / "nspkg" / "tasks.py",
            """\
            from nspkg import helpers

            def compute(x: int) -> int:
                return helpers.compute(x)
            """,
        )
        helpers_path = install_root / "nspkg" / "helpers.py"
        _write_module(helpers_path, "def compute(x: int) -> int:\n    return x + 1\n")

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (install_root,))
        monkeypatch.setattr(source_hash, "_packages_distributions", lambda: {"nspkg": ["nsdist"]})
        with _imported_from(install_root, "nspkg.tasks") as tasks:
            before = source_hash.compute_source_hash(tasks.compute)
            helpers_path.write_text(
                "def compute(x: int) -> int:\n    return x + 100\n", encoding="utf-8"
            )
            assert source_hash.compute_source_hash(tasks.compute) != before

    def test_unresolvable_installed_module_falls_back_to_the_install_root(
        self, tmp_path, monkeypatch
    ):
        """No ``__init__.py`` and no metadata: over-hash rather than narrow silently."""
        install_root = tmp_path / "site-packages"
        _write_module(
            install_root / "solo_tasks.py",
            """\
            import vendorlib

            def compute(x: int) -> int:
                return vendorlib.scale(x)
            """,
        )
        _write_module(install_root / "vendorlib" / "__init__.py", "def scale(x): return x * 2\n")

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (install_root,))
        monkeypatch.setattr(source_hash, "_packages_distributions", dict)
        with _imported_from(install_root, "solo_tasks") as tasks:
            assert set(source_hash._local_import_closure(tasks)) == {
                "solo_tasks",
                "vendorlib",
            }

    def test_distribution_confirmed_bare_module_holds_only_itself(self, tmp_path, monkeypatch):
        """Metadata naming the module as its own top level bounds it to itself."""
        install_root = tmp_path / "site-packages"
        _write_module(
            install_root / "solo_tasks.py",
            """\
            import vendorlib

            def compute(x: int) -> int:
                return vendorlib.scale(x)
            """,
        )
        _write_module(install_root / "vendorlib" / "__init__.py", "def scale(x): return x * 2\n")

        monkeypatch.setattr(source_hash, "_installed_roots", lambda: (install_root,))
        monkeypatch.setattr(
            source_hash, "_packages_distributions", lambda: {"solo_tasks": ["solodist"]}
        )
        with _imported_from(install_root, "solo_tasks") as tasks:
            assert set(source_hash._local_import_closure(tasks)) == {"solo_tasks"}


def _hash_module_task(path: Path, body: str, *, module_name: str) -> str:
    """Write ``body`` to ``path``, import it fresh, and hash its ``compute`` task."""
    _write_module(path, body)
    linecache.checkcache()
    for loaded in [n for n in sys.modules if n == module_name or n.startswith(f"{module_name}.")]:
        sys.modules.pop(loaded, None)
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    return source_hash.compute_source_hash(module.compute.fn)


class TestSourceHashEndToEnd:
    """Hash one real module on disk, rewrite it, hash again."""

    @pytest.fixture
    def hash_of(self, tmp_path, monkeypatch):
        monkeypatch.syspath_prepend(str(tmp_path))
        module_path = tmp_path / "e2e_tasks.py"

        def _hash(body: str) -> str:
            return _hash_module_task(module_path, body, module_name="e2e_tasks")

        yield _hash
        sys.modules.pop("e2e_tasks", None)

    def test_resource_edit_leaves_the_hash_untouched(self, hash_of):
        lean = hash_of("""\
            from ginkgo import task

            @task(threads=24)
            def compute(x: int) -> int:
                return x + 1
            """)
        generous = hash_of("""\
            from ginkgo import task

            @task(threads=24, memory="16Gi")
            def compute(x: int) -> int:
                return x + 1
            """)
        assert lean == generous

    def test_failure_policy_edit_leaves_the_hash_untouched(self, hash_of):
        """``on_failure`` decides what a failure stops, never what a task makes."""
        fail_fast = hash_of("""\
            from ginkgo import task

            @task(threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        ignoring = hash_of("""\
            from ginkgo import task

            @task(threads=2, on_failure="ignore")
            def compute(x: int) -> int:
                return x + 1
            """)
        assert fail_fast == ignoring

    def test_failure_policy_edit_in_the_import_closure_leaves_the_hash_untouched(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A helper module's decorator text is filtered like the task's own."""
        monkeypatch.syspath_prepend(str(tmp_path))
        helper_path = tmp_path / "policy_helpers.py"
        task_body = """\
            import policy_helpers
            from ginkgo import task

            @task(threads=2)
            def compute(x: int) -> int:
                return policy_helpers.widen(x)
            """

        def _helper(policy: str) -> str:
            return f"""\
                from ginkgo import task

                @task(threads=4, on_failure="{policy}")
                def widen(x: int) -> int:
                    return x * 2
                """

        try:
            _write_module(helper_path, _helper("fail_fast"))
            fail_fast = _hash_module_task(
                tmp_path / "policy_tasks.py", task_body, module_name="policy_tasks"
            )
            _write_module(helper_path, _helper("ignore"))
            ignoring = _hash_module_task(
                tmp_path / "policy_tasks.py", task_body, module_name="policy_tasks"
            )
        finally:
            sys.modules.pop("policy_tasks", None)
            sys.modules.pop("policy_helpers", None)

        assert fail_fast == ignoring

    def test_kind_edit_changes_the_hash(self, hash_of):
        python_kind = hash_of("""\
            from ginkgo import task

            @task(kind="python", threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        shell_kind = hash_of("""\
            from ginkgo import task

            @task(kind="shell", threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        assert python_kind != shell_kind

    def test_body_edit_changes_the_hash(self, hash_of):
        original = hash_of("""\
            from ginkgo import task

            @task(threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        edited = hash_of("""\
            from ginkgo import task

            @task(threads=2)
            def compute(x: int) -> int:
                return x + 2
            """)
        assert original != edited

    def test_non_task_decorator_text_still_changes_the_hash(self, hash_of):
        """Only ``@task`` resource kwargs are filtered; user decorators are opaque."""
        original = hash_of("""\
            from ginkgo import task

            def tag(**kwargs):
                def wrap(fn):
                    return fn
                return wrap

            @task(threads=2)
            @tag(threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        edited = hash_of("""\
            from ginkgo import task

            def tag(**kwargs):
                def wrap(fn):
                    return fn
                return wrap

            @task(threads=2)
            @tag(threads=8)
            def compute(x: int) -> int:
                return x + 1
            """)
        assert original != edited

    def test_version_edit_changes_the_hash(self, hash_of):
        first = hash_of("""\
            from ginkgo import task

            @task(version=1, threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        second = hash_of("""\
            from ginkgo import task

            @task(version=2, threads=2)
            def compute(x: int) -> int:
                return x + 1
            """)
        assert first != second


class TestSourceHashCacheInvalidation:
    """Verify that modifying a task function body causes a cache miss."""

    def test_modified_source_causes_cache_miss(self, tmp_path):
        """Write two versions of a task module and verify cache miss on change."""
        module_dir = tmp_path / "pkg"
        module_dir.mkdir()
        (module_dir / "__init__.py").write_text("")

        # Version 1 of the task.
        v1_source = textwrap.dedent("""\
            from ginkgo import task

            @task()
            def compute(x: int) -> int:
                return x + 1
        """)
        (module_dir / "tasks.py").write_text(v1_source)

        import importlib
        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            mod = importlib.import_module("pkg.tasks")
            result1 = evaluate(mod.compute(x=5))
            assert result1 == 6

            # Version 2: change the function body.
            v2_source = textwrap.dedent("""\
                from ginkgo import task

                @task()
                def compute(x: int) -> int:
                    return x + 10
            """)
            (module_dir / "tasks.py").write_text(v2_source)

            # Reload the module to pick up the new source.
            importlib.reload(mod)
            collector = EventCollector()
            result2 = evaluate(mod.compute(x=5), event_bus=collector.bus)
            assert result2 == 15

            # Should have re-executed, not served from cache.
            assert collector.started()
            assert not collector.cached()
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("pkg.tasks", None)
            sys.modules.pop("pkg", None)

    def test_modified_imported_helper_causes_cache_miss(self, tmp_path):
        """Changing a transitive local helper invalidates an unchanged task."""
        module_dir = tmp_path / "pkg"
        module_dir.mkdir()
        (module_dir / "__init__.py").write_text("")
        (module_dir / "tasks.py").write_text(
            textwrap.dedent("""\
                from ginkgo import task
                from . import helpers

                @task()
                def compute(x: int) -> int:
                    return helpers.compute(x)
            """),
            encoding="utf-8",
        )
        (module_dir / "helpers.py").write_text(
            textwrap.dedent("""\
                from .transform import adjust

                def compute(x: int) -> int:
                    return adjust(x)
            """),
            encoding="utf-8",
        )
        transform_path = module_dir / "transform.py"
        transform_path.write_text(
            "def adjust(x: int) -> int:\n    return x + 1\n", encoding="utf-8"
        )

        import importlib
        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            tasks = importlib.import_module("pkg.tasks")
            assert evaluate(tasks.compute(x=5)) == 6

            cached = EventCollector()
            assert evaluate(tasks.compute(x=5), event_bus=cached.bus) == 6
            assert cached.cached()

            transform_path.write_text(
                "def adjust(x: int) -> int:\n    return x + 10\n", encoding="utf-8"
            )
            importlib.reload(sys.modules["pkg.transform"])
            importlib.reload(sys.modules["pkg.helpers"])
            tasks = importlib.reload(tasks)

            collector = EventCollector()
            assert evaluate(tasks.compute(x=5), event_bus=collector.bus) == 15
            assert collector.started()
            assert not collector.cached()
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("pkg.transform", None)
            sys.modules.pop("pkg.helpers", None)
            sys.modules.pop("pkg.tasks", None)
            sys.modules.pop("pkg", None)


# ---------------------------------------------------------------------------
# File output symlinks
# ---------------------------------------------------------------------------


@task(kind="shell")
def write_file_task(output_path: str) -> file:
    return shell(cmd=f"echo 'hello' > {output_path}", output=output_path)


@task(kind="shell")
def write_folder_task(output_dir: str) -> folder:
    return shell(
        cmd=f"mkdir -p {output_dir} && echo 'a' > {output_dir}/a.txt && echo 'b' > {output_dir}/b.txt",
        output=output_dir,
    )


@task()
def write_python_file_task(output_path: str, payload: str) -> file:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload, encoding="utf-8")
    return str(output)


class TestWritableFileOutputs:
    def test_output_remains_writable_after_execution(self, tmp_path):
        output = tmp_path / "result.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        result_path = Path(str(result))
        assert result_path.is_file()
        assert not result_path.is_symlink()
        assert result_path.read_text().strip() == "hello"

    def test_output_can_be_overwritten_locally(self, tmp_path):
        output = tmp_path / "editable.txt"
        result = evaluate(write_file_task(output_path=str(output)))
        result_path = Path(str(result))
        result_path.write_text("modified", encoding="utf-8")
        assert result_path.read_text(encoding="utf-8") == "modified"

    def test_deleted_file_is_restored_on_cache_hit(self, tmp_path):
        output = tmp_path / "recreate.txt"
        evaluate(write_file_task(output_path=str(output)))

        output.unlink()
        assert not output.exists()

        collector = EventCollector()
        result = evaluate(write_file_task(output_path=str(output)), event_bus=collector.bus)
        assert collector.cached()
        result_path = Path(str(result))
        assert result_path.is_file()
        assert not result_path.is_symlink()
        assert result_path.read_text().strip() == "hello"

    def test_modified_file_is_restored_without_cache_miss(self, tmp_path):
        output = tmp_path / "modified.txt"
        evaluate(write_file_task(output_path=str(output)))

        output.write_text("tampered", encoding="utf-8")

        collector = EventCollector()
        result = evaluate(write_file_task(output_path=str(output)), event_bus=collector.bus)
        assert collector.cached()
        result_path = Path(str(result))
        assert result_path.is_file()
        assert result_path.read_text(encoding="utf-8").strip() == "hello"

    def test_python_file_output_rerun_overwrites_previous_materialization(self, tmp_path):
        output = tmp_path / "python.txt"
        first = evaluate(write_python_file_task(output_path=str(output), payload="first"))
        assert Path(str(first)).read_text(encoding="utf-8") == "first"

        second = evaluate(write_python_file_task(output_path=str(output), payload="second"))
        assert Path(str(second)).read_text(encoding="utf-8") == "second"


class TestWritableFolderOutputs:
    def test_output_is_directory_with_regular_files_after_execution(self, tmp_path):
        output = tmp_path / "outdir"
        result = evaluate(write_folder_task(output_dir=str(output)))
        result_path = Path(str(result))
        assert result_path.is_dir()
        assert (result_path / "a.txt").read_text().strip() == "a"
        assert (result_path / "b.txt").read_text().strip() == "b"
        assert not (result_path / "a.txt").is_symlink()
        assert not (result_path / "b.txt").is_symlink()

    def test_folder_contents_are_writable(self, tmp_path):
        output = tmp_path / "writable_dir"
        result = evaluate(write_folder_task(output_dir=str(output)))
        result_path = Path(str(result))
        (result_path / "a.txt").write_text("updated", encoding="utf-8")
        assert (result_path / "a.txt").read_text(encoding="utf-8") == "updated"

    def test_deleted_folder_is_restored(self, tmp_path):
        output = tmp_path / "dir_recreate"
        evaluate(write_folder_task(output_dir=str(output)))

        import shutil

        shutil.rmtree(output)
        assert not output.exists()

        collector = EventCollector()
        result = evaluate(write_folder_task(output_dir=str(output)), event_bus=collector.bus)
        assert collector.cached()
        result_path = Path(str(result))
        assert result_path.is_dir()
        assert (result_path / "a.txt").read_text().strip() == "a"

    def test_modified_folder_is_restored_without_cache_miss(self, tmp_path):
        output = tmp_path / "dir_modified"
        evaluate(write_folder_task(output_dir=str(output)))

        (output / "a.txt").write_text("tampered", encoding="utf-8")

        collector = EventCollector()
        result = evaluate(write_folder_task(output_dir=str(output)), event_bus=collector.bus)
        assert collector.cached()
        assert (Path(str(result)) / "a.txt").read_text(encoding="utf-8").strip() == "a"


# ---------------------------------------------------------------------------
# Cache prune with read-only artifacts
# ---------------------------------------------------------------------------


class TestCachePruneReadOnly:
    def test_prune_handles_read_only_artifacts(self, tmp_path: Path) -> None:
        """The prune helper must remove a read-only entry tree without raising."""
        output = tmp_path / "prunable.txt"
        evaluate(write_file_task(output_path=str(output)))

        cache_root = Path(".ginkgo") / "cache"
        entries = [entry for entry in cache_root.iterdir() if entry.is_dir()]
        assert entries, "expected at least one cache entry to prune"

        # Strip the write bit from every directory and file so a naive
        # shutil.rmtree would fail with PermissionError; the production helper
        # must restore permissions and complete the removal.
        for entry in entries:
            for child in entry.rglob("*"):
                child.chmod(stat.S_IRUSR | stat.S_IXUSR if child.is_dir() else stat.S_IRUSR)
            entry.chmod(stat.S_IRUSR | stat.S_IXUSR)

        for entry in entries:
            _safe_rmtree(entry)

        assert not any(child.is_dir() for child in cache_root.iterdir())
