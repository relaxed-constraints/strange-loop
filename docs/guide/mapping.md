# Mapping and joining

Loop.map starts independent flows from keyed inputs. Inside a flow, ctx.map starts children from a version containing a string-keyed dictionary. Awaiting the map joins their results in input order.

Run this example in a Python notebook.

```python
from strange_loop.replay import Batch, Context, Loop, Version, stage

@stage
def labels(prefix: str) -> dict[str, str]:
    return {"a": prefix, "b": prefix + "s"}

@stage
def length(text: str) -> int:
    return len(text)

async def measure(ctx: Context, source: Version) -> Version:
    return await length(text=source)

@stage
def total(lengths: dict[str, int]) -> int:
    return sum(lengths.values())

async def workload(ctx: Context, prefix: Version) -> Version:
    names = await labels(prefix=prefix)
    lengths = await ctx.map(measure, source=names)
    return await total(lengths=lengths)

print((await Loop().run(workload, prefix="cat")).value)  # 7
items = await Loop().map(measure, source={"a": "cat", "b": "cats"})
print(items["b"].value)  # 4
```

The total records an ordered sequence of keys and version IDs. On replay, replacing only b changes the total’s binding from (a@1, b@1) to (a@1, b@2), so total executes again. These abbreviated IDs illustrate the dependency.

A child flow can call a `@batch_stage`. Its implementation receives a `Batch` for the calls ready at that moment. The parent receives the joined version handles from `ctx.map` after all children finish.
