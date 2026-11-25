# Partial results and execution retries

An async batch can submit each result as it arrives. Once submit returns, the runtime has recorded that key’s version.

Run this example in a Python notebook.

```python
from strange_loop.replay import Batch, Context, Loop, Version, batch_stage

@batch_stage
async def echo(batch: Batch) -> None:
    for item in batch:
        if item.key == "b" and len(batch) > 1:
            continue
        await item.submit(item.inputs["text"])
    if len(batch) > 1:
        raise TimeoutError("simulated interruption")

async def copy(ctx: Context, source: Version) -> Version:
    return await echo(text=source)

loop = Loop(attempts=2)
await loop.map(copy, source={"a": "one", "b": "two", "c": "three"})
print(loop.trace("batch"))
```

The first attempt accepts a and c. Only b enters the second attempt. Its accepted output is version 1. The attempts limit includes the initial execution. A semantic revision starts from an already accepted version and has a separate max_revisions budget.

After submitting an item, return `None` or let the function end. Returning any dictionary in that attempt raises `BatchContractError`. Accepted versions remain recorded, and the runtime skips execution retries for this error.

## Handling a failed item

Return `ctx.fail(reason, target=version, evidence=verdict)` to finish the current invocation. The target and evidence are optional. A revision or replacement request beyond `max_revisions` produces a failure automatically. Stages retry timeout and connection errors up to the loop's attempt limit. A batch retries only its outstanding items. Unexpected exceptions and batch contract violations propagate.

Both map methods collect outcomes in input order. A parent can use successful versions as inputs to another stage, or stop its own invocation when a complete collection is required.

```python
from strange_loop.replay import Context, Failure, Version
from examples.failures import translate_query
from examples.workload import time_workload

async def workload(ctx: Context, queries: Version) -> Version | Failure:
    translations = await ctx.map(translate_query, source=queries)
    if translations.failed:
        return ctx.fail("Incomplete workload", causes=translations.failed)
    return await time_workload(queries=translations.succeeded)
```

Here `time_workload` is the timing stage from the workload example. Run `python -m examples.failures` to see one success, an exhausted revision budget, and an explicitly unsupported query in the same map. SQLite retains the terminal outcomes and their version references on resume.
