# API Reference

```{raw} html
<div class="reference-page">
```

This page lists the functions and classes you use to write workflows. For a
walkthrough with examples, see [Tasks and Flows](../guide/tasks-and-flows.md)
and [Core Concepts](../guide/concepts.md).

## Top-Level Package

Import these names from `ginkgo`, for example `from ginkgo import file, task`.

```{eval-rst}
.. automodule:: ginkgo
   :members:
   :undoc-members:
```

```{eval-rst}
.. py:currentmodule:: ginkgo

.. py:data:: Out

   Marks a task parameter as a path the task *writes*, not one it reads.

   Write ``Out[file]`` or ``Out[folder]``. ``Out`` can wrap a container or an
   optional, such as ``Out[list[file]]`` or ``Out[file | None]``, but it must be
   the outermost wrapper. Ginkgo rejects ``list[Out[file]]``, and an inner type
   that is not a path, such as ``Out[str]``, when the task is defined. A type
   checker sees ``Out[file]`` as plain ``file``.
```

## Flow API

```{eval-rst}
.. automodule:: ginkgo.core.flow
   :members:
```

## Task API

```{eval-rst}
.. automodule:: ginkgo.core.task
   :members:
   :exclude-members: _load_taskdef
```

## Shell API

```{eval-rst}
.. automodule:: ginkgo.core.shell
   :members:
```

## Notebook API

`notebook` comes from the `ginkgo-notebook` plugin, so install the plugin first.
See [Plugins](../getting-started/installation.md#plugins).
Then import it with `from ginkgo_notebook import notebook`.

```{eval-rst}
.. automodule:: ginkgo_notebook
   :members: notebook
```

## Query API

`ginkgo.query` reads the provenance database from Python. Open it with
`ginkgo.query.open()`. See [Querying Provenance](../guide/querying-provenance.md).

```{eval-rst}
.. automodule:: ginkgo.query
   :members: open, Query
```

## Authoring Helpers

```{eval-rst}
.. automodule:: ginkgo.wildcards
   :members:
```

```{eval-rst}
.. automodule:: ginkgo.project
   :members: project_root, find_project_root
```

```{raw} html
</div>
```
