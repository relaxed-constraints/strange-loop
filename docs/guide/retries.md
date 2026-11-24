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
