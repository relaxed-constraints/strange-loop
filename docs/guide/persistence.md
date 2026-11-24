# Saving and resuming runs

Pass a SQLite file path to enable automatic checkpoints. Give a long-running experiment a run ID so you can find it even if the process stops before the call returns.

```python
from examples.workload import benchmark
from strange_loop.replay import Loop, Version

loop = Loop(store="experiments.sqlite")
answer: Version = await loop.run(benchmark, dataset="orders fixture", run_id="workload-01")
```

In a fresh session, open the saved run to inspect its chronological trace. Use the same flow code to resume. A completed run returns its saved result immediately.

```python
store = Loop(store="experiments.sqlite")
print(store.runs())
run = store.load("workload-01")
print(run.status)
print(run.trace())
answer = await run.resume(benchmark)
```

For a mapped run, each successful `await item.submit(value)` commits that item's version before the stage continues. Resume replays unfinished flows using the committed versions. An external operation that finished just before its output was committed can run again.

To try a real process interruption, run these commands from the repository root with a fresh database path. The first process exits with code 75 after saving q01 and q03. The second dispatches q02 alone.

```sh
python -m examples.persistence interrupt demo.sqlite
python -m examples.persistence resume demo.sqlite
```

SQLite stores each version's payload with a codec name. New versions and trace events are committed incrementally. Loading a run reads its history; accessing a version's value decodes its payload. Use compatible flow and stage code when resuming.

## Choosing a value format

| Value | Default storage |
| --- | --- |
| JSON objects, arrays and scalars | JSON for ordinary string-keyed documents and lists |
| Bytes or tuples | Binary bytes or a type-preserving tuple representation |
| BSON | Pickle fallback for PyMongo values such as ObjectId and Decimal128, including values nested in documents |
| NumPy array, pandas DataFrame, or Matplotlib Figure | Python pickle, which restores the Python object in a compatible environment |
| Other classes | A registered codec, or pickle when no codec handles the value |

Register a file codec when a class already supplies a single-file persistence format. Strange Loop calls the class's `save(value, path)` and `load(path)`. This also supports a normal instance method `save(self, path)`. Supply `suffix=".json"` when the class selects its format by extension.

```python
from bondi.workload import Workload
from strange_loop.replay import FileCodec, Loop

loop = Loop(store="runs.sqlite", codecs=[FileCodec(Workload, name="bondi.workload")])
```

Use a custom codec for an in-memory byte format. This example preserves the exact Decimal representation.

```python
from decimal import Decimal
from strange_loop.replay import CustomCodec, Loop

decimal_codec = CustomCodec(
    type=Decimal,
    name="decimal.v1",
    save=lambda number: str(number).encode("ascii"),
    load=lambda data: Decimal(data.decode("ascii")),
)
loop = Loop(store="runs.sqlite", codecs=[decimal_codec])
```

Registered codecs take precedence in registration order. Keep each codec name stable and supply its decoder again when reading the corresponding values. A selected codec's save error propagates so a broken serializer is visible.

The writer tries pickle last. A reader must explicitly enable unpickling with `Loop(store="runs.sqlite", allow_pickle=True)`; only enable it for trusted data. Trace inspection is available without this option. DataFrames, figures, and custom classes require compatible libraries on restore. For PyTorch, a consumer can register a model-specific codec using its chosen weight format and model constructor.

This storage format starts a new run catalog. Earlier prototype snapshot rows are left in place and are not loaded.
