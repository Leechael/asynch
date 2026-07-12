# WP07 pure-Python clickhouse-driver control

This directory contains mechanical Python translations of the four Cython-only
modules from `mymarilyn/clickhouse-driver` revision `49afa09`:

- `clickhouse_driver/bufferedreader.pyx`
- `clickhouse_driver/bufferedwriter.pyx`
- `clickhouse_driver/columns/largeint.pyx`
- `clickhouse_driver/varint.pyx`

Each translation preserves upstream control flow, buffering, and batch methods.
The only changes remove Cython declarations and replace C API memory/slice work
with ordinary Python objects; the nearby comments identify those unavoidable
equivalences by upstream source line. No additional batching or optimization is
introduced. `setup-no-ext-modules.patch` removes the upstream extension build
list, so these modules cannot be shadowed by generated C extensions.

Build the isolated comparison interpreter with:

```bash
python benchmarks/pure_python_driver/build_env.py --output-dir /tmp/wp07-pure-python
```

The command clones and pins the upstream source, applies the committed patch,
copies these modules, installs the checkout editable with the `lz4` extra, and fails
unless all four imports resolve to the copied `.py` files and the checkout is
exactly at `49afa09`. It writes the interpreter path and provenance JSON to
stdout. The CI workflow is the canonical measurement environment.
