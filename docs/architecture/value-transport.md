# Value Transport

Python task inputs and outputs cross process boundaries through the codec layer in `ginkgo/runtime/artifacts/value_codec.py`.

The current implementation supports:

- direct transport for small values
- artifact-backed transport for large values
- pickle-based fallback for general Python objects
- optimized codecs for NumPy arrays
- parquet-first DataFrame transport with pickle fallback

The same codec layer is used for both task transport and cache persistence.

Only exact `list`, `tuple` and `dict` values, and exact scalars, are encoded
structurally; a subclass (`np.float64`, a `namedtuple`, a `defaultdict`) is
pickled whole so it decodes as itself. The cache key hashes a container
subclass item by item like its builtin, under its own type name, so a `file`
it holds stays content-tracked.

A class defined in a single-file workflow lives in a module whose synthetic
name (`USER_MODULE_PREFIX`) is unique to one process and one absolute path.
The codec's pickler records such a class as a call to
`module_loader.import_workflow_attribute` with the workflow file's path
relative to the project root, which resolves to the workflow already loaded
in the decoding process or loads it. For hashing, the reference is replaced by
`CACHE_IDENTITY_MODULE`, so a moved or copied workflow keeps its cache keys.
A worker loads its task's module before decoding the task's arguments.

NumPy and pandas are imported lazily: the codec recognises an array or a
DataFrame through `sys.modules`, since such a value can only exist once its
library is imported, and imports the library only to encode, decode or hash
one. A spawned worker therefore starts its first task without paying for
those imports.
