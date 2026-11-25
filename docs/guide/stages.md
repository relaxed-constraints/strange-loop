# Stages and flows

Decorate a sync or async function with `@stage`. Its parameters receive decoded Python values. Call it with keyword arguments inside an async flow and await its version.

Run this example in a Python notebook.

```python
from strange_loop import Context, Loop, Version, stage


@stage
def normalize(text: str) -> str:
    return text.strip()


@stage
def fallback() -> str:
    return "untitled"


async def title(ctx: Context, text: Version) -> Version:
    result = await normalize(text=text)
    if result.value:
        return result
    return await fallback()


print((await Loop().run(title, text="   ")).value)  # untitled
```

Branching and helpers are ordinary Python. The flow body can run again after revision. Put calls whose results should be reused, such as model requests, inside stages.
