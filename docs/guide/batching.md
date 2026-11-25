# Batch stages

A batch stage collects compatible ready calls into one execution. Each waiting flow still asks for one result.

Run this example in a Python notebook.

```python
from strange_loop import Batch, Context, Loop, Version, batch_stage


@batch_stage
def uppercase(batch: Batch) -> dict[str, str]:
    return {item.key: item.inputs["text"].upper() for item in batch}


async def shout(ctx: Context, source: Version) -> Version:
    return await uppercase(text=source)


loop = Loop()
results = await loop.map(shout, source={"a": "one", "b": "two", "c": "three"})
print([r.value for r in results.values()])  # ['ONE', 'TWO', 'THREE']
print(loop.batches)
```

Iterate over the `Batch` to get its items. Return outputs keyed by `item.key`. When a provider returns responses out of order, use `await batch[response.id].submit(response.value)` to deliver each one. Sync and async implementations are supported. Declare `@batch_stage(revisable=True)` to accept revision requests. Each item has its own `item.request`, containing previous and feedback when revising and None on a fresh call. The runtime groups revision requests separately from fresh calls.
